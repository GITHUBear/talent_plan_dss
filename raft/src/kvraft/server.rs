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
    // simple database
    db: HashMap<String, String>,
    // Saves a mapping of client name to the maximum sequence number
    // of operations submitted by the client.
    // Avoid a serial number operation being repeatedly submitted by the client.
    client_seq_map: HashMap<String, u64>,
    // Save a mapping of the operation log index to
    // (the result sender of the corresponding operation request, term,
    // client name, client seq, operation mode)
    // Used to prevent blocking of get and put_append methods.
    log_index_channel_map: HashMap<u64, (Sender<Reply>, u64, String, u64, u64)>,
    // The sender and receiver of the client invocation event.
    msg_rx: UnboundedReceiver<ActionEv>,
    msg_tx: UnboundedSender<ActionEv>,
    // Committed logs or snapshot from Raft
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
    /// Create a snapshot and save the data for `db` and `client_seq_map`
    fn create_snapshot(&self) -> Vec<u8> {
        let mut data = vec![];
        let snapshot = Snapshot {
            keys: self.db.keys().cloned().collect(),
            values: self.db.values().cloned().collect(),
            client_names: self.client_seq_map.keys().cloned().collect(),
            seqs: self.client_seq_map.values().copied().collect(),
        };

        labcodec::encode(&snapshot, &mut data).unwrap_or_else(|_| ());
        data
    }

    /// Determine if snapshot is needed.
    ///
    /// `applied_index` holds the last log index of this snapshot
    fn need_snapshot(&self, applied_index: u64) {
        if let Some(limit) = self.maxraftstate {
            if self.rf.persist_size() < (limit * 6 / 10) as u64 {
                // Less than 80% of the maximum threshold, no snapshot.
                return;
            }
            let snapshot = self.create_snapshot();
            self.rf.local_snapshot(applied_index as usize, snapshot);
        }
    }

    // When creating KvServer, restore state with snapshot.
    fn read_snapshot_from_raw(&mut self, raw: &[u8]) {
        if raw.is_empty() {
            return;
        }

        if let Ok(snapshot) = labcodec::decode(&raw) {
            let snapshot: Snapshot = snapshot;
            self.db = snapshot
                .keys
                .into_iter()
                .zip(snapshot.values.into_iter())
                .collect();
            self.client_seq_map = snapshot
                .client_names
                .into_iter()
                .zip(snapshot.seqs.into_iter())
                .collect();
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
        // Client invocation event.
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
                                // Successfully sent command to raft.
                                info!("[Server {}] cmd: {:?} start", self.server.me, &cmd);
                                // Save sender.
                                self.server
                                    .log_index_channel_map
                                    .insert(index, (tx, term, args.name.clone(), args.seq, 0));
                            }
                            Err(_) => {
                                // Not a leader, send a reply immediately.
                                let reply = GetReply {
                                    wrong_leader: true,
                                    err: "Wrong Leader".to_owned(),
                                    value: "".to_owned(),
                                };
                                // Prevent timeout client requests from
                                // dropping the receiving end and causing errors.
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
                                // Successfully sent command to raft.
                                info!("[Server {}] cmd: {:?} start", self.server.me, &cmd);
                                // Save sender.
                                self.server
                                    .log_index_channel_map
                                    .insert(index, (tx, term, args.name.clone(), args.seq, 1));
                            }
                            Err(_) => {
                                // Not a leader, send a reply immediately.
                                let reply = PutAppendReply {
                                    wrong_leader: true,
                                    err: "Wrong Leader".to_owned(),
                                };
                                // Prevent timeout client requests from
                                // dropping the receiving end and causing errors.
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
        // apply_ch arrival event.
        match self.server.apply_ch.poll() {
            Ok(Async::Ready(Some(apply_msg))) => {
                // First check if apply_msg is a snapshot.
                if !apply_msg.command_valid {
                    self.server.read_snapshot_from_raw(&apply_msg.command);
                } else if let Ok(cmd) = labcodec::decode(&apply_msg.command) {
                    let cmd: Command = cmd;
                    match cmd.op {
                        0 => {
                            // Get
                            let seq = self.server.client_seq_map.get(&cmd.name).unwrap_or(&0);
                            if cmd.seq > *seq {
                                // Update seq.
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
                                    // The log has been committed.
                                    if !sender.is_canceled() {
                                        // Send a reply with the result.
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
                                    // the loss of leadership.
                                    if !sender.is_canceled() {
                                        // Send error reply with the reason of missing leadership.
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
                                // Update database.
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
                                // Update seq.
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
                                    // The log has been committed.
                                    if !sender.is_canceled() {
                                        // Send a reply with the result.
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
                                    // the loss of leadership.
                                    if !sender.is_canceled() {
                                        // Send error reply with the reason of missing leadership.
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
                    // Check if snapshot is needed.
                    self.server.need_snapshot(apply_msg.command_index);
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
        if let Some(handle) = machine {
            self.msg_tx
                .unbounded_send(ActionEv::Kill)
                .unwrap_or_else(|_| ());
            handle.join().unwrap();
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
