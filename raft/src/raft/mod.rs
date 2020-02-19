use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use futures::future::ok;
use futures::prelude::*;
use futures::sync::{
    mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    oneshot::{channel, Receiver, Sender},
};
use futures::{Future, Stream};
use futures_timer::Delay;

use rand::Rng;

use labrpc::{self, RpcFuture};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

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
    #[allow(dead_code)]
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    //    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role, // the role of a node
    is_leader: Arc<AtomicBool>,

    // 以下三项在服务器上持久存在
    voted_for: Option<u64>, // candidateId that received vote in current term
    current_term: Arc<AtomicU64>,
    logs: Vec<Log>, // 每一个 log 包含指令和该指令关联的任期号

    // 以下两项在服务器上经常变化
    commit_index: usize,
    last_applied: usize,

    // 成为 Leader 后开始维护
    next_index: Vec<usize>,
    match_index: Vec<usize>, // 成为 leader 后初始化为 0

    msg_tx: UnboundedSender<ActionEv>,
    msg_rx: UnboundedReceiver<ActionEv>,
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
        let (tx, rx) = unbounded::<ActionEv>();
        // Your initialization code here (2A, 2B, 2C).
        let group_num = peers.len();
        let mut rf = Raft {
            peers,
            persister,
            me,
            //            state: Arc::default(),
            role: Role::Follower,
            voted_for: None,

            current_term: Arc::new(AtomicU64::new(1)),
            is_leader: Arc::new(AtomicBool::new(false)),

            // 添加一个 dummy log
            logs: vec![Log {
                term: 0,
                index: 0,
                command: vec![],
            }],

            commit_index: 0,
            last_applied: 0,

            next_index: vec![0; group_num],
            match_index: vec![0; group_num],

            msg_tx: tx,
            msg_rx: rx,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
        //        crate::your_code_here((rf, apply_ch))
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    #[allow(dead_code)]
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
        let (tx, rx) = channel::<Result<RequestVoteReply>>();
        let peer = &self.peers[server];
        peer.spawn(
            peer.request_vote(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    if !tx.is_canceled() {
                        tx.send(res).unwrap();
                    }
                    ok(())
                }),
        );
        rx
    }

    fn send_heartbeat(
        &self,
        server: usize,
        args: &AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let (tx, rx) = channel::<Result<AppendEntriesReply>>();
        let peer = &self.peers[server];
        peer.spawn(
            peer.append_entries(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    if !tx.is_canceled() {
                        tx.send(res).unwrap();
                    }
                    ok(())
                }),
        );
        rx
    }

    #[allow(dead_code)]
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
    /// 判断 RequestVote RPC Caller 的日志集是否比本节点的旧，旧返回 true，反之 false (包括一样新)
    /// args_last_term 是 Caller 最后一项日志的 term
    /// args_last_index 是 Caller 最后一项日志的 index
    fn is_newer(&self, args_last_term: u64, args_last_index: u64) -> bool {
        // 初始化时添加了一个 dummy log，可以直接 unwrap
        let log = self.logs.last().unwrap();
        let (self_last_term, self_last_index) = (log.term, log.index);

        if self_last_term == args_last_term {
            // term 相同，比较长度
            self_last_index > args_last_index
        } else {
            // 比较 term
            self_last_term > args_last_term
        }
    }

    /// 判断 AppendEntries RPC Caller 的 prev log 是否和当前节点匹配
    /// 匹配返回 true，反之返回 false
    /// args_prev_term 是 Caller 希望匹配的日志 term
    /// args_prev_index 是 Caller 希望匹配的日志 index
    fn is_match(&self, args_prev_term: u64, args_prev_index: u64) -> bool {
        match self.logs.get(args_prev_index) {
            Some(log) => {
                assert_eq!(log.index, args_prev_index);
                log.term == args_prev_term
            }
            None => {
                // 说明请求者的 log数量 比本节点的 log 多
                false
            }
        }
    }

    fn be_follower(&mut self) {
        self.is_leader.store(false, Ordering::SeqCst);
        self.role = Role::Follower;
    }

    fn be_candidate(&mut self) {
        self.is_leader.store(false, Ordering::SeqCst);
        self.current_term.fetch_add(1, Ordering::SeqCst);
        self.role = Role::Candidate;
        self.voted_for = Some(self.me as u64);

        self.send_request_vote_all();
    }

    fn be_leader(&mut self) {
        // 2B: 添加 next_index 和 match_index 的初始化
        let log_len = self.logs.len();
        for index in self.next_index.iter_mut() {
            *index = log_len;
        }
        for index in self.match_index.iter_mut() {
            *index = 0;
        }
        self.is_leader.store(true, Ordering::SeqCst);
        self.role = Role::Leader;
    }

    /// 处理 RPC RequestVote 请求, 返回 Reply 和一个bool值表示 args.term 是否大于 term
    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> (RequestVoteReply, bool) {
        // RequestVoteArgs { term, candidate_id }
        let term = self.current_term.load(Ordering::SeqCst);

        // 2A: 只要发现请求者的 term 比自己大就都同意投票 true is OK
        // 2B: Raft Paper 5.4.1 选举限制
        // 不管如何，args.term 只要比自己大就更新自己的 term
        // 一个 candidate 在网络受阻的情况下多次提升自己的 term
        // 这样做有利于在恢复后立即更新集群的 term
        if args.term > term {
            self.current_term.store(args.term, Ordering::SeqCst);
        }

        let vote_granted =
            if args.term < term || self.is_newer(args.last_log_term, args.last_log_index) {
                // 只要发现请求者的 term 比自己小就都拒绝投票
                // 只要发现请求者的日志比自己的旧就拒绝投票
                false
            } else if args.term > term {
                // 请求者的日志比自己的新且term大
                true
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

        (RequestVoteReply { term, vote_granted }, args.term > term)
    }

    /// 处理 RPC AppendEntries 请求, 返回 Reply
    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> (AppendEntriesReply, bool) {
        // AppendEntriesArgs { term, leader_id }
        let prev_index = args.prev_log_index;
        let prev_term = args.prev_log_term;
        let term = self.current_term.load(Ordering::SeqCst);

        if args.term > term {
            self.current_term.store(args.term, Ordering::SeqCst);
        }

        let log_match = self.is_match(prev_term, prev_index);
        let success = !(args.term < term || !log_match);
        if success && !args.entries.is_empty() {
            // 匹配 且 entries 不为空
            // 这里没有再搜索冲突位置，直接截取 prev_index + 1 长度的 log
            self.logs.truncate(prev_index + 1);
            self.logs.append(&mut args.entries);
        }

        if log_match {
            (
                AppendEntriesReply {
                    term,
                    success,
                    conflict_index: 0,
                    conflict_term: 0,
                },
                args.term > term,
            )
        } else {
            let (conflict_index, conflict_term) = match self.logs.get(prev_index) {
                Some(log) => {
                    // 越过所有那个任期冲突的所有日志条目,找到该任期最早的日志索引
                    let mut index = prev_index;
                    for i in (0..=prev_index).rev() {
                        if self.logs[i].term != log.term {
                            index = i + 1;
                        }
                    }
                    (index, log.term)
                }
                None => {
                    // 说明请求者的 log数量 比本节点的 log 多
                    (self.logs.len() as u64, 0)
                }
            };
            (
                AppendEntriesReply {
                    term,
                    success,
                    conflict_index: 0,
                    conflict_term: 0,
                },
                args.term > term,
            )
        }
    }

    /// 向所有 peers 发送投票请求
    fn send_request_vote_all(&self) {
        let term = self.current_term.load(Ordering::SeqCst);
        let log = self.logs.last().unwrap();
        let args = RequestVoteArgs {
            term,
            candidate_id: self.me as u64,
            last_log_index: log.index,
            last_log_term: log.term,
        };

        let me = self.me;
        // RequestVote Reply 的接收端
        let result_rxs: Vec<Receiver<Result<RequestVoteReply>>> = self
            .peers
            .iter()
            .enumerate()
            .filter(|(id, _)| {
                // 筛除自己
                *id != me
            })
            .map(|(id, _)| self.send_request_vote(id, &args))
            .collect();

        // 初始化为1, 默认投自己一票
        let mut vote_cnt = 1 as usize;
        let group_num = self.peers.len();
        let tx = self.msg_tx.clone();

        // 这里使用 take_while 在达到半数后就会终止 stream 的执行
        // 进而导致有些 rx 端提前被销毁
        // 所以在 send_request_vote 函数中会出现 unwrap on a `Err` 的异常
        // 需要在 send_request_vote 中忽略掉 error
        let stream = futures::stream::futures_unordered(result_rxs)
            .take_while(move |reply| {
                // 由于虚拟网络的干扰会出现 Err, 忽略掉 Err 的 Reply
                if let Ok(reply) = reply {
                    info!("[send_request_vote_all {}] get vote reply: {:?}", me, reply);
                    if reply.vote_granted {
                        // 同意投票
                        vote_cnt += 1;
                        if vote_cnt * 2 > group_num {
                            // 超过半数
                            // 向状态机发送 SuccessElection 消息
                            tx.unbounded_send(ActionEv::SuccessElection(term))
                                .map_err(|e| {
                                    error!(
                                        "[send_request_vote_all {}] send Success Election fail {}",
                                        me, e
                                    );
                                })
                                .unwrap();
                            // stream 完成
                            ok(false)
                        } else {
                            ok(true)
                        }
                    } else {
                        // 不同意投票，查看回应的 term
                        // 不同意原因1：reply.term > term
                        if reply.term > term {
                            // 向状态机发送 FailElection 消息
                            tx.unbounded_send(ActionEv::Fail(reply.term))
                                .map_err(|e| {
                                    error!(
                                        "[send_request_vote_all {}] send ActionEv::Fail fail {}",
                                        me, e
                                    );
                                })
                                .unwrap();
                            // stream 完成
                            ok(false)
                        } else {
                            // 不同意原因2：本节点的日志不够新
                            ok(true)
                        }
                    }
                } else {
                    ok(true)
                }
            })
            .for_each(|_| ok(()))
            // Send 端 Canceled Err 应该不会出现
            .map_err(|_| ());

        tokio::spawn(stream);
    }

    fn send_heartbeat_all(&self) {
        let term = self.current_term.load(Ordering::SeqCst);
        let args = AppendEntriesArgs {
            term,
            leader_id: self.me as u64,
        };

        let me = self.me;
        // AppendEntries Reply 的接收端
        let result_rxs: Vec<Receiver<Result<AppendEntriesReply>>> = self
            .peers
            .iter()
            .enumerate()
            .filter(|(id, _)| {
                // 筛除自己
                *id != me
            })
            .map(|(id, _)| self.send_heartbeat(id, &args))
            .collect();

        let tx = self.msg_tx.clone();
        let stream = futures::stream::futures_unordered(result_rxs)
            .for_each(move |reply| {
                if let Ok(reply) = reply {
                    info!(
                        "[send_heartbeat_all {}] get AppendEntries reply: {:?}",
                        me, reply
                    );
                    if reply.term > term {
                        tx.unbounded_send(ActionEv::Fail(reply.term))
                            .map_err(|e| {
                                error!(
                                    "[send_heartbeat_all {}] send ActionEv::Fail fail {}",
                                    me, e
                                );
                            })
                            .unwrap();
                    }
                }
                ok(())
            })
            .map_err(|_| ());

        tokio::spawn(stream);
    }
}

/// 定义状态机事件
enum TimeoutEv {
    /// 选举超时
    Election,
    /// 心跳超时
    Heartbeat,
}

enum ActionEv {
    /// 节点接收到来自其他节点的 RequestVote RPC
    /// 使用 oneshot 一次性管道发送结果到异步等待的接收端
    RequestVote(RequestVoteArgs, Sender<RequestVoteReply>),
    /// 节点接收到来自其他节点的 AppendEntries RPC
    AppendEntries(AppendEntriesArgs, Sender<AppendEntriesReply>),
    /// 在一次选举当中获胜 包含获胜任期
    SuccessElection(u64),
    /// 在一次选举当中失败 包含导致失败的发送者任期
    Fail(u64),
    /// 关闭状态机
    Kill,
}

/// `StateFuture` 实现 `Stream` Trait
/// 在执行者的推动下，将完成异步状态机的功能
/// `StateFuture` 将 timeout 和 其他节点的 RPC 等事件都视为 `Future`
/// `StateFuture` 异步地等待事件的到达，根据事件相应地改变 Raft 状态
///
/// 所以 `StateFuture` 包含了对 `Raft` 的所有权
/// RPC 事件异步接收者包含在 `Raft` 结构中
/// timeout 事件则由内部 `Delay` 实现
/// timeout_ev 表示超时事件
struct StateFuture {
    raft: Raft,
    timeout: Delay,
    timeout_ev: TimeoutEv,
}

impl StateFuture {
    fn new(raft: Raft) -> Self {
        StateFuture {
            raft,
            timeout: Delay::new(StateFuture::rand_election_timeout()),
            timeout_ev: TimeoutEv::Election,
        }
    }

    fn rand_election_timeout() -> Duration {
        let rand_timeout =
            rand::thread_rng().gen_range(ELECTION_TIMEOUT_START, ELECTION_TIMEOUT_END);
        Duration::from_millis(rand_timeout)
    }

    fn heartbeat_timeout() -> Duration {
        Duration::from_millis(HEARTBEAT_TIMEOUT)
    }
}

impl Stream for StateFuture {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<()>, ()> {
        match self.raft.msg_rx.poll() {
            Ok(Async::Ready(Some(ev))) => {
                match ev {
                    ActionEv::RequestVote(args, tx) => {
                        // 调用 Raft::handle_request_vote, 返回 reply
                        // 将 reply 通过 tx 发送
                        info!("[StateFuture {}] get RequestVote event", self.raft.me);
                        let (reply, args_term_gtr) = self.raft.handle_request_vote(args);
                        tx.send(reply).unwrap_or_else(|_| {
                            error!("[StateFuture {}] send RequestVoteReply error", self.raft.me);
                        });
                        // 2B: 现在 vote_granted 就不能完全表示 args.term > term 的情况了
                        // 故修改 handle_request_vote 的接口
                        if args_term_gtr {
                            // 投票标记为真, 改变 raft 状态为 follower
                            self.raft.be_follower();
                            // 重置超时时间, 设置下次超时执行选举
                            self.timeout.reset(StateFuture::rand_election_timeout());
                            self.timeout_ev = TimeoutEv::Election;
                        }
                    }
                    ActionEv::AppendEntries(args, tx) => {
                        // 调用 Raft::handle_append_entries, 返回 reply
                        info!("[StateFuture {}] get AppendEntries event", self.raft.me);
                        let (reply, args_term_gtr) = self.raft.handle_append_entries(args);
                        tx.send(reply).unwrap_or_else(|_| {
                            error!(
                                "[StateFuture {}] send AppendEntriesReply error",
                                self.raft.me
                            );
                        });
                        if args_term_gtr {
                            info!(
                                "[StateFuture {}] After Handle AppendEntries => follower",
                                self.raft.me
                            );
                            // 投票标记为真, 改变 raft 状态为 follower
                            self.raft.be_follower();
                            // 重置超时时间, 设置下次超时执行选举
                            self.timeout.reset(StateFuture::rand_election_timeout());
                            self.timeout_ev = TimeoutEv::Election;
                        }
                    }
                    ActionEv::SuccessElection(term) => {
                        info!("[StateFuture {}] get SuccessElection event", self.raft.me);
                        // 如果接收到 SuccessElection 的时候
                        // 节点已经转变状态为 follower，那么目前的 term 一定是本节点之前选举的之后任期
                        // 如果选举超时再一次开始了选举，那么目前的 term 也一定是本节点之前选举的之后任期
                        if self.raft.current_term.load(Ordering::SeqCst) == term {
                            info!(
                                "[StateFuture {}] After Get SuccessElection => leader",
                                self.raft.me
                            );
                            self.raft.be_leader();
                            self.timeout.reset(StateFuture::heartbeat_timeout());
                            self.timeout_ev = TimeoutEv::Heartbeat;
                        }
                    }
                    ActionEv::Fail(term) => {
                        info!("[StateFuture {}] get Fail event", self.raft.me);
                        if term > self.raft.current_term.load(Ordering::SeqCst) {
                            info!(
                                "[StateFuture {}] After Get Fail event => follower",
                                self.raft.me
                            );
                            self.raft.be_follower();
                            self.timeout.reset(StateFuture::rand_election_timeout());
                            self.timeout_ev = TimeoutEv::Election;
                        }
                    }
                    ActionEv::Kill => {
                        // Stream 完成
                        info!("[StateFuture {}] killed", self.raft.me);
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
                        info!(
                            "[StateFuture {}] Election Timeout at local term: {}",
                            self.raft.me,
                            self.raft.current_term.load(Ordering::SeqCst)
                        );
                        // 改变 raft 状态为 candidate
                        self.raft.be_candidate();
                        // 重置超时时间, 设置下次超时执行选举
                        self.timeout.reset(StateFuture::rand_election_timeout());
                        self.timeout_ev = TimeoutEv::Election;
                    }
                    TimeoutEv::Heartbeat => {
                        info!(
                            "[StateFuture {}] Heartbeat Timeout at local term: {}",
                            self.raft.me,
                            self.raft.current_term.load(Ordering::SeqCst)
                        );
                        // 发送心跳包
                        self.raft.send_heartbeat_all();
                        self.timeout.reset(StateFuture::heartbeat_timeout());
                        self.timeout_ev = TimeoutEv::Heartbeat;
                    }
                }
                return Ok(Async::Ready(Some(())));
            }
            Ok(Async::NotReady) => {}
            Err(e) => {
                error!(
                    "[StateFuture {}] timeout channel error: {}",
                    self.raft.me, e
                );
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
/// state_machine 将 `StateFuture` 异步状态机放到一个独立线程运行
/// current_term 和 is_leader 共享 Raft 中对应成员的所有权 (为了 term 和 is_leader 方法)
/// msg_tx 从 Raft 结构中对应成员 clone 得到，用于捕获 RPC 调用，发送消息到 `StateFuture`
#[derive(Clone)]
pub struct Node {
    // Your code here.
    state_machine: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    current_term: Arc<AtomicU64>,
    is_leader: Arc<AtomicBool>,
    msg_tx: UnboundedSender<ActionEv>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        info!("[Node::new] node {} init", raft.me);
        let current_term = Arc::clone(&raft.current_term);
        let is_leader = Arc::clone(&raft.is_leader);
        let msg_tx = raft.msg_tx.clone();
        let state_future = StateFuture::new(raft);

        let handle = thread::spawn(move || tokio::run(state_future.for_each(|_| ok(()))));

        Node {
            state_machine: Arc::new(Mutex::new(Some(handle))),
            current_term,
            is_leader,
            msg_tx,
        }
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
        self.current_term.load(Ordering::SeqCst)
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.is_leader.load(Ordering::SeqCst)
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
        let machine = self.state_machine.lock().unwrap().take();
        if let Some(_handle) = machine {
            self.msg_tx.unbounded_send(ActionEv::Kill).unwrap();
            // 这里如果使用了 join 等待线程结束，在测试结束后会等待较长一段时间
            // But why?
            // handle.join().unwrap();
        }
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        //        crate::your_code_here(args)
        let (tx, rx) = channel();

        if !self.msg_tx.is_closed() {
            self.msg_tx
                .clone()
                .unbounded_send(ActionEv::RequestVote(args, tx))
                .map_err(|_| ())
                .unwrap();
        }
        Box::new(rx.map_err(|_| labrpc::Error::Other("Request Vote Receive Error".to_owned())))
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> RpcFuture<AppendEntriesReply> {
        // Your code here (2A, 2B).
        let (tx, rx) = channel();

        if !self.msg_tx.is_closed() {
            self.msg_tx
                .clone()
                .unbounded_send(ActionEv::AppendEntries(args, tx))
                .map_err(|_| ())
                .unwrap();
        }
        Box::new(rx.map_err(|_| labrpc::Error::Other("Append Entries Receive Error".to_owned())))
    }
}
