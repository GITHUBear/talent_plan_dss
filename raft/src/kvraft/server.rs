use crate::proto::kvraftpb::*;
use crate::raft;

use crate::raft::ApplyMsg;
use futures::future::ok;
use futures::prelude::*;
use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::sync::oneshot::{channel, Sender};
use futures_timer::Delay;
use labrpc::RpcFuture;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

#[derive(Debug)]
pub enum Reply {
    Get(GetReply),
    PutAppend(PutAppendReply),
}

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    // 简易数据库
    db: HashMap<String, String>,
    // 保存一个 client 名到该客户端提交的操作的最大序列号的映射
    // 避免一个序列号的操作被 client 反复提交
    client_seq_map: HashMap<String, u64>,
    // 保存一个 操作日志索引 到 (相应操作请求的结果发送端,term,client name,client seq,操作方式) 的映射
    // 用于防止 get 和 put_append 方法的阻塞
    log_index_channel_map: HashMap<u64, (Sender<Reply>, u64, String, u64, u64)>,
    // 传输 客户端调用事件 的发送端和接收端
    msg_rx: UnboundedReceiver<ActionEv>,
    msg_tx: UnboundedSender<ActionEv>,
    // 接收来自下层的 Raft 发送来的 已经成功提交的日志
    apply_ch: UnboundedReceiver<ApplyMsg>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.
        let (tx, apply_ch) = unbounded();
        let raw_snapshot = persister.snapshot();
        let rf = raft::Raft::new(servers, me, persister, tx);
        let node = raft::Node::new(rf);
        let (msg_tx, msg_rx) = unbounded();

        let mut kv_server = KvServer {
            rf: node,
            me,
            maxraftstate,

            db: HashMap::new(),
            client_seq_map: HashMap::new(),
            log_index_channel_map: HashMap::new(),

            msg_rx,
            msg_tx,

            apply_ch,
        };

        kv_server.read_snapshot_from_raw(&raw_snapshot);
        kv_server
    }
}

impl KvServer {
    /// 创建 snapshot，保存 db 和 client_seq_map 的数据
    fn create_snapshot(&self) -> Vec<u8> {
        let mut data = vec![];
        let snapshot = Snapshot {
            keys: self.db.keys().map(|k| k.clone()).collect(),
            values: self.db.values().map(|v| v.clone()).collect(),
            client_names: self.client_seq_map.keys().map(|k| k.clone()).collect(),
            seqs: self.client_seq_map.values().map(|v| v.clone()).collect(),
        };

        labcodec::encode(&snapshot, &mut data).unwrap_or_else(|_| ());
        data
    }

    /// 判断是否需要进行 snapshot
    ///
    /// applied_index 保存了本次 snapshot 的最后一个日志索引
    fn need_snapshot(&self, applied_index: u64) {
        match self.maxraftstate {
            Some(limit) => {
                if self.rf.persist_size() < (limit * 8 / 10) as u64 {
                    // 小于最大阈值的 80%，不进行 snapshot
                    return;
                }
                let snapshot = self.create_snapshot();
                self.rf.local_snapshot(applied_index as usize, snapshot);
            },
            None => return,
        }
    }

    // 在创建 KvServer 之初，通过 snapshot 恢复状态
    fn read_snapshot_from_raw(&mut self, raw: &[u8]) {
        if raw.is_empty() {
            return;
        }

        if let Ok(snapshot) = labcodec::decode(&raw) {
            let snapshot: Snapshot = snapshot;
            self.db = snapshot.keys.into_iter()
                .zip(snapshot.values.into_iter()).collect();
            self.client_seq_map = snapshot.client_names.into_iter()
                .zip(snapshot.seqs.into_iter()).collect();
        }
    }
}

enum ActionEv {
    GetRpc(GetRequest, Sender<Reply>),
    PutAppendRpc(PutAppendRequest, Sender<Reply>),
    Kill,
}

struct KvServerFuture {
    server: KvServer,
}

impl Stream for KvServerFuture {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<()>, ()> {
        // 客户端调用事件
        match self.server.msg_rx.poll() {
            Ok(Async::Ready(Some(e))) => {
                match e {
                    ActionEv::GetRpc(args, tx) => {
                        let cmd = Command {
                            op: 0,
                            key: args.key.clone(),
                            value: "".to_owned(),
                            seq: args.seq,
                            name: args.name.clone(),
                        };
                        match self.server.rf.start(&cmd) {
                            Ok((index, term)) => {
                                // 成功将 command 送入 raft
                                info!("[Server {}] cmd: {:?} start", self.server.me, &cmd);
                                // 保存 发送端
                                self.server
                                    .log_index_channel_map
                                    .insert(index, (tx, term, args.name.clone(), args.seq, 0));
                            }
                            Err(_) => {
                                // 不是 leader, 立即发送 Reply
                                let reply = GetReply {
                                    wrong_leader: true,
                                    err: "Wrong Leader".to_owned(),
                                    value: "".to_owned(),
                                };
                                // 防止超时的客户端请求会drop掉接收端，导致出错
                                if !tx.is_canceled() {
                                    info!(
                                        "[Server {}] Tell {} : I am not a leader ",
                                        self.server.me, &args.name
                                    );
                                    tx.send(Reply::Get(reply)).unwrap_or_else(|_| {
                                        error!("[Server {}] send get reply error", self.server.me);
                                    });
                                }
                            }
                        }
                    }
                    ActionEv::PutAppendRpc(args, tx) => {
                        assert_ne!(args.op, 0);
                        let cmd = Command {
                            op: args.op as u64,
                            key: args.key.clone(),
                            value: args.value.clone(),
                            seq: args.seq,
                            name: args.name.clone(),
                        };
                        match self.server.rf.start(&cmd) {
                            Ok((index, term)) => {
                                // 成功将 command 送入 raft
                                info!("[Server {}] cmd: {:?} start", self.server.me, &cmd);
                                // 保存 发送端
                                self.server
                                    .log_index_channel_map
                                    .insert(index, (tx, term, args.name.clone(), args.seq, 1));
                            }
                            Err(_) => {
                                // 不是 leader, 立即发送 Reply
                                let reply = PutAppendReply {
                                    wrong_leader: true,
                                    err: "Wrong Leader".to_owned(),
                                };
                                // 防止超时的客户端请求会drop掉接收端，导致出错
                                if !tx.is_canceled() {
                                    info!(
                                        "[Server {}] Tell {} : I am not a leader ",
                                        self.server.me, &args.name
                                    );
                                    tx.send(Reply::PutAppend(reply)).unwrap_or_else(|_| {
                                        error!(
                                            "[Server {}] send put append reply error",
                                            self.server.me
                                        );
                                    });
                                }
                            }
                        }
                    }
                    ActionEv::Kill => {
                        self.server.rf.kill();
                        return Ok(Async::Ready(None));
                    }
                }
                return Ok(Async::Ready(Some(())));
            }
            Ok(Async::Ready(None)) => {
                return Ok(Async::Ready(None));
            }
            Ok(Async::NotReady) => {}
            Err(_) => unreachable!(),
        }
        // apply_ch 到达事件
        match self.server.apply_ch.poll() {
            Ok(Async::Ready(Some(apply_msg))) => {
                if let Ok(cmd) = labcodec::decode(&apply_msg.command) {
                    let cmd: Command = cmd;
                    // 检查是否需要 snapshot
                    self.server.need_snapshot(apply_msg.command_index);
                    match cmd.op {
                        0 => {
                            // Get
                            let seq = self.server.client_seq_map.get(&cmd.name).unwrap_or(&0);
                            if cmd.seq > *seq {
                                // 更新seq
                                self.server.client_seq_map.insert(cmd.name.clone(), cmd.seq);
                            }
                            if let Some((sender, term, name, seq, op)) = self
                                .server
                                .log_index_channel_map
                                .remove(&apply_msg.command_index)
                            {
                                if apply_msg.command_term == term
                                    && cmd.name.eq(&name)
                                    && cmd.seq == seq
                                {
                                    // 说明日志已经提交
                                    if !sender.is_canceled() {
                                        // 发送包含结果的 reply
                                        let reply = GetReply {
                                            wrong_leader: false,
                                            err: "".to_owned(),
                                            value: self
                                                .server
                                                .db
                                                .get(&cmd.key)
                                                .unwrap_or(&("".to_owned()))
                                                .clone(),
                                        };
                                        sender.send(Reply::Get(reply)).unwrap_or_else(|_| {
                                            error!(
                                                "[Server {} apply_ch] send get reply error",
                                                self.server.me
                                            );
                                        });
                                    }
                                } else {
                                    // 说明失去领导地位
                                    if !sender.is_canceled() {
                                        // 发送丢失领导地位的 error reply
                                        if op == 0 {
                                            let reply = GetReply {
                                                wrong_leader: true,
                                                err: "lose leadership".to_owned(),
                                                value: "".to_owned(),
                                            };
                                            sender.send(Reply::Get(reply)).unwrap_or_else(|_| {
                                                error!(
                                                    "[Server {} apply_ch] send get reply error",
                                                    self.server.me
                                                );
                                            });
                                        } else {
                                            let reply = PutAppendReply {
                                                wrong_leader: true,
                                                err: "lose leadership".to_owned(),
                                            };
                                            sender.send(Reply::PutAppend(reply)).unwrap_or_else(
                                                |_| {
                                                    error!(
                                                        "[Server {} apply_ch] send put reply error",
                                                        self.server.me
                                                    );
                                                },
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        1 | 2 => {
                            // Put & Append
                            let seq = self.server.client_seq_map.get(&cmd.name).unwrap_or(&0);
                            if cmd.seq > *seq {
                                // 更新数据库
                                if cmd.op == 1 {
                                    self.server.db.insert(cmd.key.clone(), cmd.value.clone());
                                } else {
                                    let old_val = self
                                        .server
                                        .db
                                        .entry(cmd.key.clone())
                                        .or_insert_with(|| "".to_owned());
                                    old_val.push_str(&*(cmd.value.clone()));
                                }
                                // 更新seq
                                self.server.client_seq_map.insert(cmd.name.clone(), cmd.seq);
                            }
                            if let Some((sender, term, name, seq, op)) = self
                                .server
                                .log_index_channel_map
                                .remove(&apply_msg.command_index)
                            {
                                if apply_msg.command_term == term
                                    && cmd.name.eq(&name)
                                    && cmd.seq == seq
                                {
                                    // 说明日志已经提交
                                    if !sender.is_canceled() {
                                        // 发送包含结果的 reply
                                        let reply = PutAppendReply {
                                            wrong_leader: false,
                                            err: "".to_owned(),
                                        };
                                        sender.send(Reply::PutAppend(reply)).unwrap_or_else(|_| {
                                            error!(
                                                "[Server {} apply_ch] send put reply error",
                                                self.server.me
                                            );
                                        });
                                    }
                                } else {
                                    // 说明失去领导地位
                                    if !sender.is_canceled() {
                                        // 发送丢失领导地位的 error reply
                                        if op == 0 {
                                            let reply = GetReply {
                                                wrong_leader: true,
                                                err: "lose leadership".to_owned(),
                                                value: "".to_owned(),
                                            };
                                            sender.send(Reply::Get(reply)).unwrap_or_else(|_| {
                                                error!(
                                                    "[Server {} apply_ch] send get reply error",
                                                    self.server.me
                                                );
                                            });
                                        } else {
                                            let reply = PutAppendReply {
                                                wrong_leader: true,
                                                err: "lose leadership".to_owned(),
                                            };
                                            sender.send(Reply::PutAppend(reply)).unwrap_or_else(
                                                |_| {
                                                    error!(
                                                        "[Server {} apply_ch] send put reply error",
                                                        self.server.me
                                                    );
                                                },
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
            Ok(Async::Ready(None)) => {
                return Ok(Async::Ready(None));
            }
            Ok(Async::NotReady) => {
                return Ok(Async::NotReady);
            }
            Err(_) => unreachable!(),
        }

        Ok(Async::Ready(Some(())))
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    state_machine: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    current_term: Arc<AtomicU64>,
    is_leader: Arc<AtomicBool>,
    msg_tx: UnboundedSender<ActionEv>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        let current_term = Arc::clone(&(kv.rf.current_term));
        let is_leader = Arc::clone(&(kv.rf.is_leader));
        let msg_tx = kv.msg_tx.clone();
        let state_future = KvServerFuture { server: kv };

        let state_machine = thread::spawn(move || tokio::run(state_future.for_each(|_| ok(()))));

        Node {
            state_machine: Arc::new(Mutex::new(Some(state_machine))),
            current_term,
            is_leader,
            msg_tx,
        }
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        let machine = self.state_machine.lock().unwrap().take();
        if let Some(_handle) = machine {
            self.msg_tx.unbounded_send(ActionEv::Kill).unwrap();
//            handle.join().unwrap();
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        let current_term = self.current_term.load(Ordering::SeqCst);
        let is_leader = self.is_leader.load(Ordering::SeqCst);
        // Your code here.
        raft::State {
            term: current_term,
            is_leader,
        }
    }
}

impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        // Your code here.
        let (tx, rx) = channel();

        if !self.msg_tx.is_closed() {
            self.msg_tx
                .clone()
                .unbounded_send(ActionEv::GetRpc(arg, tx))
                .map_err(|_| ())
                .unwrap();
        }

        Box::new(
            Delay::new(Duration::from_millis(500))
                .map(|_| GetReply {
                    wrong_leader: true,
                    err: "timeout".to_owned(),
                    value: "".to_owned(),
                })
                .map_err(|_| labrpc::Error::Other("timeout error".to_owned()))
                .select(
                    rx.map(move |reply| match reply {
                        Reply::Get(get_reply) => get_reply,
                        Reply::PutAppend(_) => unreachable!(),
                    })
                    .map_err(|_| labrpc::Error::Other("GetReply receive error".to_owned())),
                )
                .map(|(reply, _)| reply)
                .map_err(|(e, _)| e),
        )
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        // Your code here.
        let (tx, rx) = channel();

        if !self.msg_tx.is_closed() {
            self.msg_tx
                .clone()
                .unbounded_send(ActionEv::PutAppendRpc(arg, tx))
                .map_err(|_| ())
                .unwrap();
        }

        Box::new(
            Delay::new(Duration::from_millis(500))
                .map(|_| PutAppendReply {
                    wrong_leader: true,
                    err: "timeout".to_owned(),
                })
                .map_err(|_| labrpc::Error::Other("timeout error".to_owned()))
                .select(
                    rx.map(move |reply| match reply {
                        Reply::Get(_) => unreachable!(),
                        Reply::PutAppend(put_append_reply) => put_append_reply,
                    })
                    .map_err(|_| labrpc::Error::Other("GetReply receive error".to_owned())),
                )
                .map(|(reply, _)| reply)
                .map_err(|(e, _)| e),
        )
    }
}
