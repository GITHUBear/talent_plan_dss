use crate::proto::kvraftpb::*;
use crate::raft;

use futures::sync::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::sync::oneshot::{channel, Sender};
use futures::prelude::*;
use labrpc::RpcFuture;
use crate::raft::ApplyMsg;
use std::collections::HashMap;

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
    // 保存一个 操作日志索引 到 相应操作请求的结果发送端 的映射
    // 用于防止 get 和 put_append 方法的阻塞
    log_index_channel_map: HashMap<u64, Sender<Reply>>,
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
        let rf = raft::Raft::new(servers, me, persister, tx);

        crate::your_code_here((rf, maxraftstate, apply_ch))
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }
}

enum ActionEv {
    GetRpc(GetRequest, Sender<Reply>),
    PutAppendRpc(PutAppendRequest, Sender<Reply>),
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
                        let seq = self.server.client_seq_map.get(&args.name).unwrap_or(&0);
                        let cmd = Command {
                            op: 0,
                            key: args.key.clone(),
                            value: "".to_owned(),
                        };
                        // 忽略客户端发来的更旧的seq
                        if args.seq > seq {
                            match self.server.rf.start(&cmd) {
                                Ok((index, _)) => {
                                    // 成功将 command 送入 raft
                                    info!("[Server] cmd: {:?} start", &cmd);
                                    // 更新 client_seq_map
                                    self.server.client_seq_map.insert(args.name, args.seq);
                                    // 保存 发送端
                                    self.server.log_index_channel_map.insert(index, tx);
                                },
                                Err(_) => {
                                    // 不是 leader, 立即发送 Reply
                                    let reply = GetReply {
                                        wrong_leader: true,
                                        err: "Wrong Leader".to_owned(),
                                        value: "".to_owned(),
                                    };
                                    tx.send(Reply::Get(reply)).unwrap_or_else(|_| {
                                        error!("[Server] Tell {} : I am not a leader ", &args.name);
                                    });
                                }
                            }
                        }
                    },
                    ActionEv::PutAppendRpc(args, tx) => {
                        let seq = self.server.client_seq_map.get(&args.name).unwrap_or(&0);
                        assert_ne!(args.op, 0);
                        let cmd = Command {
                            op: args.op,
                            key: args.key.clone(),
                            value: args.value.clone(),
                        };

                        if args.seq > seq {
                            match self.server.rf.start(&cmd) {
                                Ok((index, _)) => {
                                    // 成功将 command 送入 raft
                                    info!("[Server] cmd: {:?} start", &cmd);
                                    // 更新 client_seq_map
                                    self.server.client_seq_map.insert(args.name, args.seq);
                                    // 保存 发送端
                                    self.server.log_index_channel_map.insert(index, tx);
                                },
                                Err(_) => {
                                    // 不是 leader, 立即发送 Reply
                                    let reply = PutAppendReply {
                                        wrong_leader: true,
                                        err: "Wrong Leader".to_owned(),
                                    };
                                    tx.send(Reply::PutAppend(reply)).unwrap_or_else(|_| {
                                        error!("[Server] Tell {} : I am not a leader ", &args.name);
                                    });
                                }
                            }
                        }
                    },
                }
                return Ok(Async::Ready(Some(())));
            },
            Ok(Async::Ready(None)) => {
                return Ok(Async::Ready(None));
            },
            Ok(Async::NotReady) => {},
            Err(_) => unreachable!(),
        }
        // apply_ch 到达事件
        match self.server.apply_ch.poll() {
            Ok(Async::Ready(Some(apply_msg))) => {
                if let Ok(cmd) = labcodec::decode(&apply_msg.command) {
                    let cmd: Command = cmd;
                    match cmd.op {
                        0 => {
                            // Get
                            if self.server.rf.is_leader() {

                            }
                        },
                        1 => {
                            // Put
                        },
                        2 => {
                            // Append
                        },
                        _ => unreachable!(),
                    }
                }
            },
            Ok(Async::Ready(None)) => {
                return Ok(Async::Ready(None));
            },
            Ok(Async::NotReady) => {
                return Ok(Async::NotReady);
            },
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
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        crate::your_code_here(kv);
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
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
        // Your code here.
        raft::State {
            ..Default::default()
        }
    }
}

impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        // Your code here.
        crate::your_code_here(arg)
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        // Your code here.
        crate::your_code_here(arg)
    }
}
