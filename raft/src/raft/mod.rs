use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{sync_channel, Receiver},
        Arc,
    },
    time::Duration,
};

use futures::prelude::*;
use futures::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot::Sender as TX,
};
use futures::{Future, Stream};

use rand::Rng;

use labrpc::RpcFuture;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use futures_timer::Delay;
use futures::future::{err, ok};

const ELECTION_TIMEOUT_START: u64 = 150;
const ELECTION_TIMEOUT_END: u64 = 300;
const HEARTBEAT_TIMEOUT: u64 = 100;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

pub enum Role {
    Follower,
    Candidate,
    Leader,
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    //    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role,             // the role of a node
    voted_for: Option<u64>, // candidateId that received vote in current term
    current_term: Arc<AtomicU64>,
    is_leader: Arc<AtomicBool>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            //            state: Arc::default(),
            role: Role::Follower,
            voted_for: None,

            current_term: Arc::new(AtomicU64::new(0)),
            is_leader: Arc::new(AtomicBool::new(false)),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
        //        crate::your_code_here((rf, apply_ch))
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             Ok(())
        //         }),
        // );
        // rx
        // ```
        let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        //        crate::your_code_here((server, args, tx, rx))
        let peer = &self.peers[server];
        peer.spawn(
            peer.request_vote(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    tx.send(res);
                    Ok(())
                }),
        );
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    fn be_follower(&mut self) {
        self.role = Role::Follower;
        self.is_leader.store(false, Ordering::SeqCst);
    }

    fn be_candidate(&mut self) {
        self.role = Role::Candidate;
        self.is_leader.store(false, Ordering::SeqCst);
        self.current_term.fetch_add(1, Ordering::SeqCst);
        self.voted_for = Some(self.me as u64);
    }

    /// 处理 RPC RequestVote 请求, 返回 Reply
    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        // RequestVoteArgs { term, candidate_id }
        let term = self.current_term.load(Ordering::SeqCst);
        let vote_granted = if args.term > term {
            // 只要发现请求者的 term 比自己大就都同意投票
            self.current_term.store(args.term, Ordering::SeqCst);
            true
        } else if args.term < term {
            // 只要发现请求者的 term 比自己小就都拒绝投票
            false
        } else {
            // 在本任期已经投给过了请求者, 依然同意投票
            match self.voted_for {
                Some(peer) => peer == args.candidate_id,
                None => true,
            }
        };

        if vote_granted {
            // 同意投票, 更新 voted_for
            self.voted_for = Some(args.candidate_id);
        }

        RequestVoteReply { term, vote_granted }
    }

    /// 处理 RPC AppendEntries 请求, 返回 Reply
    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        // AppendEntriesArgs { term, leader_id }
        let term = self.current_term.load(Ordering::SeqCst);
        let success = if args.term > term {
            // 只要发现请求者的 term 比自己大就返回true
            self.current_term.store(args.term, Ordering::SeqCst);
            true
        } else if args.term < term {
            // 只要发现请求者的 term 比自己小就返回false
            false
        } else {
            true
        };

        AppendEntriesReply { term, success }
    }

    /// 向所有 peers 发送投票请求
    fn send_request_vote_all(&self) {
        let term = self.current_term.load(Ordering::SeqCst);
        let args = RequestVoteArgs {
            term,
            candidate_id: self.me as u64,
        };

        let me = self.me;
        // RequestVote Reply 的接收端
        let result_rxs: Vec<Receiver<Result<RequestVoteReply>>> =
            self.peers
            .iter()
            .enumerate()
            .filter(|(&id, _)| {
                // 筛除自己
                id != me
            })
            .map(|(id, _)| {
                self.send_request_vote(id, &args)
            })
            .collect();

        // 初始化为1, 默认投自己一票
        let vote_cnt = 1 as usize;
        let group_num = self.peers.len();
        let recv_fut =
            futures::stream::iter_ok::<_, ()>(result_rxs)
            .for_each(|rx| {
                let res = rx.recv().unwrap_or_else(|e| {
                    error!("[send_request_vote_all{}] receive error: {}", me, e);
                });
                // 由于虚拟网络的干扰会出现 Err, 忽略掉 Err 的 Reply
                if let Some(reply) = res {
                    debug!("[send_request_vote_all{}] get vote reply: {:?}", me, reply);
                    if reply.vote_granted {
                        vote_cnt += 1;
                    }
                    if vote_cnt * 2 > group_num {
                        // 超过半数投给了自己
                        /// TODO: 发送 SuccessElection 给 StateFuture
                    }
                }
                ok(())
            });

        tokio::spawn(recv_fut);
    }
}

/// 定义状态机事件
enum Event {
    /// 超时事件
    Timeout(TimeoutEv),
    /// 需要 raft 进行交互处理的事件
    Action(ActionEv),
}

enum TimeoutEv {
    /// 选举超时
    Election,
    /// 心跳超时
    Heartbeat,
}

enum ActionEv {
    /// 节点接收到来自其他节点的 RequestVote RPC
    /// 使用 oneshot 一次性管道发送结果到异步等待的接收端
    RequestVote(RequestVoteArgs, TX<RequestVoteReply>),
    /// 节点接收到来自其他节点的 AppendEntries RPC
    AppendEntries(AppendEntriesArgs, TX<AppendEntriesReply>),
    /// 在一次选举当中获胜 包含获胜任期
    SuccessElection(u64),
    /// 关闭状态机
    Kill,
}

struct StateFuture {
    raft: Raft,
    timeout: Delay,
    timeout_ev: TimeoutEv,
    action_rx: UnboundedReceiver<ActionEv>,
}

impl StateFuture {
    fn rand_election_timeout() -> Duration {
        let rand_timeout =
            rand::thread_rng().gen_range(ELECTION_TIMEOUT_START, ELECTION_TIMEOUT_END);
        Duration::from_millis(rand_timeout)
    }
}

impl Stream for StateFuture {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<()>, ()> {
        match self.action_rx.poll() {
            Ok(Async::Ready(Some(ev))) => {
                match ev {
                    ActionEv::RequestVote(args, tx) => {
                        // 调用 Raft::handle_request_vote, 返回 reply
                        // 将 reply 通过 tx 发送
                        debug!("[StateFuture] get RequestVote event");
                        let reply = self.raft.handle_request_vote(args);
                        let vote_granted = reply.vote_granted;
                        tx.send(reply).unwrap_or_else(|_| {
                            error!("[StateFuture] send RequestVoteReply error")
                        });
                        if vote_granted {
                            // 投票标记为真, 改变 raft 状态为 follower
                            self.raft.be_follower();
                            // 重置超时时间, 设置下次超时执行选举
                            self.timeout.reset(StateFuture::rand_election_timeout());
                            self.timeout_ev = TimeoutEv::Election;
                        }
                    }
                    ActionEv::AppendEntries(args, tx) => {
                        // 调用 Raft::handle_append_entries, 返回 reply
                        debug!("[StateFuture] get AppendEntries event");
                        let reply = self.raft.handle_append_entries(args);
                        let success = reply.success;
                        tx.send(reply).unwrap_or_else(|_| {
                            error!("[StateFuture] send AppendEntriesReply error")
                        });
                        if success {
                            // 投票标记为真, 改变 raft 状态为 follower
                            self.raft.be_follower();
                            // 重置超时时间, 设置下次超时执行选举
                            self.timeout.reset(StateFuture::rand_election_timeout());
                            self.timeout_ev = TimeoutEv::Election;
                        }
                    }
                    ActionEv::Kill => {
                        // Stream 完成
                        debug!("[StateFuture] killed");
                        return Ok(Async::Ready(None));
                    }
                }
                return Ok(Async::Ready(Some(())));
            }
            Ok(Async::Ready(None)) => {
                // action发送端关闭，表示 Stream 完成
                return Ok(Async::Ready(None));
            }
            Ok(Async::NotReady) => {}
            Err(()) => unreachable!(),
        }

        match self.timeout.poll() {
            Ok(Async::Ready(())) => {
                match self.timeout_ev {
                    TimeoutEv::Election => {
                        debug!("[StateFuture] Election Timeout");
                        // 改变 raft 状态为 candidate
                        // 重置超时时间, 设置下次超时执行选举
                        self.timeout.reset(StateFuture::rand_election_timeout());
                        self.timeout_ev = TimeoutEv::Election;
                    }
                    TimeoutEv::Heartbeat => {
                        debug!("[StateFuture] Heartbeat Timeout");
                    }
                }
                return Ok(Async::Ready(Some(())));
            }
            Ok(Async::NotReady) => {}
            Err(e) => {
                error!("[StateFuture] timeout channel error: {}", e);
            }
        }

        Ok(Async::NotReady)
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    request_vote_call: UnboundedSender<()>,
    append_entries_call: UnboundedSender<()>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        crate::your_code_here(raft)
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        crate::your_code_here(())
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        crate::your_code_here(())
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn request_vote(&self, _args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        //        crate::your_code_here(args)
        let reply = RequestVoteReply {
            term: 0,
            vote_granted: false,
        };
        Box::new(futures::future::ok(reply))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        // Your code here (2A, 2B).
        crate::your_code_here(args)
    }
}
