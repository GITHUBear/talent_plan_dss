use std::{
    cmp::{max, min},
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
// Change 100 to 50, according to Raft Paper broadcastTime << electionTimeout
// The heartbeat timeout needs to be an order of magnitude smaller
// than the election timeout.
// Fix the problem that test_figure_8_unreliable_2c fails occasionally
const HEARTBEAT_TIMEOUT: u64 = 50;

type LogSyncReply = (Result<AppendEntriesReply>, usize, usize, usize);
type SnapSyncReply = (Result<InstallSnapshotReply>, usize, usize);

// Use one enumeration type to manage two heartbeat synchronizations.
enum SyncReply {
    Log(LogSyncReply),
    Snapshot(SnapSyncReply),
}

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
    pub command_term: u64,
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
    // 3B: Since KvServer cannot directly access the persister
    // and each apply sends its own db data down to the node,
    // the performance is too poor.
    //
    // Therefore, consider setting an atomic value to
    // hold the amount of data in the persister.
    persist_size: Arc<AtomicU64>,
    // this peer's index into peers[]
    me: usize,
    //    state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role, // the role of a node
    is_leader: Arc<AtomicBool>,

    // The following five items persist on the server.
    voted_for: Option<u64>, // candidateId that received vote in current term
    current_term: Arc<AtomicU64>,
    // Each log contains the command and the term number
    // associated with the command.
    logs: Vec<Log>,
    // Snapshot needs to persist the index and term of the last log.
    last_included_index: usize,
    last_included_term: u64,

    // The following two items change frequently on the server.
    commit_index: usize,
    last_applied: usize,

    // Become a Leader and start maintenance.
    next_index: Vec<usize>,
    match_index: Vec<usize>, // Initialize to 0 after becoming leader.

    msg_tx: UnboundedSender<ActionEv>,
    msg_rx: UnboundedReceiver<ActionEv>,

    apply_ch: UnboundedSender<ApplyMsg>,
}

// Several methods are needed in Part 3B to
// implement the index transformation of logs.
impl Raft {
    /// Input a a relative index of logs, converted to an absolute index
    /// which is based on a snapshot.
    fn absolute_index(&self, index: usize) -> usize {
        index + self.last_included_index
    }

    /// Input the absolute index and return a relative index of logs.
    /// Since it is possible to get a negative index,
    /// consider returning an Option-wrapped result.
    /// If None is returned, the calculation result is an invalid negative value.
    fn relative_index(&self, index: usize) -> Option<usize> {
        if index >= self.last_included_index {
            Some(index - self.last_included_index)
        } else {
            None
        }
    }
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
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let (tx, rx) = unbounded::<ActionEv>();
        // Your initialization code here (2A, 2B, 2C).
        let group_num = peers.len();
        let mut rf = Raft {
            peers,
            persister,
            persist_size: Arc::new(AtomicU64::new(raft_state.len() as u64)),
            me,
            //            state: Arc::default(),
            role: Role::Follower,
            voted_for: None,

            current_term: Arc::new(AtomicU64::new(1)),
            is_leader: Arc::new(AtomicBool::new(false)),

            // add a dummy log
            logs: vec![Log {
                term: 0,
                index: 0,
                command: vec![],
            }],

            // Are both initialized to 0,
            // meaning that the dummy log has been included in the snapshot.
            last_included_index: 0,
            last_included_term: 0,

            commit_index: 0,
            last_applied: 0,

            next_index: vec![0; group_num],
            match_index: vec![0; group_num],

            msg_tx: tx,
            msg_rx: rx,

            apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        let current_term = self.current_term.load(Ordering::SeqCst);
        let state = RaftState {
            current_term,
            voted_for: self.voted_for.clone().map(raft_state::VotedFor::Voted),
            entries: self.logs.clone(),
            last_included_index: self.last_included_index as u64,
            last_included_term: self.last_included_term,
        };
        let mut data: Vec<u8> = vec![];
        labcodec::encode(&state, &mut data).unwrap();
        self.persister.save_raft_state(data);
        // 3B: Update persist_size
        let size = self.persister.raft_state().len() as u64;
        self.persist_size.store(size, Ordering::SeqCst);
    }

    /// save Raft's persistent state and snapshot to stable storage,
    /// where it can later be retrieved after a crash and restart.
    fn persist_state_and_snapshot(&mut self, snapshot: Vec<u8>) {
        let current_term = self.current_term.load(Ordering::SeqCst);
        let state = RaftState {
            current_term,
            voted_for: self.voted_for.clone().map(raft_state::VotedFor::Voted),
            entries: self.logs.clone(),
            last_included_index: self.last_included_index as u64,
            last_included_term: self.last_included_term,
        };
        let mut data: Vec<u8> = vec![];
        labcodec::encode(&state, &mut data).unwrap();
        self.persister.save_state_and_snapshot(data, snapshot);
        // 3B: Update persist_size
        let size = self.persister.raft_state().len() as u64;
        self.persist_size.store(size, Ordering::SeqCst);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        if let Ok(raft_state) = labcodec::decode(data) {
            let raft_state: RaftState = raft_state;
            self.current_term
                .store(raft_state.current_term, Ordering::SeqCst);
            self.voted_for = raft_state
                .voted_for
                .clone()
                .map(|raft_state::VotedFor::Voted(n)| n);
            self.logs = raft_state.entries;
            self.last_included_index = raft_state.last_included_index as usize;
            self.last_included_term = raft_state.last_included_term;
        }
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
        let (tx, rx) = channel::<Result<RequestVoteReply>>();
        let peer = &self.peers[server];
        peer.spawn(
            peer.request_vote(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    if !tx.is_canceled() {
                        tx.send(res).unwrap_or_else(|_| ());
                    }
                    ok(())
                }),
        );
        rx
    }

    // 2B: Modify the value sent in the channel to include the `server` number,
    // `prev_log_index`, and `length of entries`,
    // so that the sender of heartbeat can update the specified `next_index` and
    // `match_index` after receiving the response.
    fn send_heartbeat(&self, server: usize, args: &AppendEntriesArgs) -> Receiver<SyncReply> {
        debug!(
            "[StateFuture {}] send AppendEntriesArgs: [\
            term: {}, leader_id: {}, prev_log_index: {}, prev_log_term: {}, leader_commit: {} \
            ] to {}",
            self.me,
            args.term,
            args.leader_id,
            args.prev_log_index,
            args.prev_log_term,
            args.leader_commit,
            server
        );
        let (tx, rx) = channel::<SyncReply>();
        let peer = &self.peers[server];
        let prev_log_index = args.prev_log_index;
        let entries_len = args.entries.len();
        peer.spawn(
            peer.append_entries(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    if !tx.is_canceled() {
                        tx.send(SyncReply::Log((
                            res,
                            server,
                            prev_log_index as usize,
                            entries_len,
                        )))
                        .unwrap_or_else(|_| ());
                    }
                    ok(())
                }),
        );
        rx
    }

    // 3B: Send InstallSnapshot RPC to the specified server,
    // the receiving end will be responsible for receiving the `reply` of this RPC,
    // `server` and `last_included_index` to update `match_index` and `next_index`.
    fn send_install_snapshot(
        &self,
        server: usize,
        args: &InstallSnapshotArgs,
    ) -> Receiver<SyncReply> {
        debug!(
            "[StateFuture {}] send InstallSnapshotArgs: [\
            term: {}, leader_id: {}, last_included_index: {}, last_included_term: {}]\
            to {}",
            self.me,
            args.term,
            args.leader_id,
            args.last_included_index,
            args.last_included_term,
            server
        );
        let (tx, rx) = channel::<SyncReply>();
        let peer = &self.peers[server];
        let last_included_index = args.last_included_index;
        peer.spawn(
            peer.install_snapshot(args)
                .map_err(Error::Rpc)
                .then(move |res| {
                    if !tx.is_canceled() {
                        tx.send(SyncReply::Snapshot((
                            res,
                            server,
                            last_included_index as usize,
                        )))
                        .unwrap_or_else(|_| ());
                    }
                    ok(())
                }),
        );
        rx
    }

    fn start(&mut self, command: Vec<u8>) -> Result<(u64, u64)> {
        // The log index should be an absolute index and needs to be converted.
        let index = self.absolute_index(self.logs.len());
        let term = self.current_term.load(Ordering::SeqCst);
        let is_leader = self.is_leader.load(Ordering::SeqCst);
        // Your code here (2B).

        if is_leader {
            self.logs.push(Log {
                term,
                index: index as u64,
                command,
            });
            self.persist();
            info!(
                "[StatusFuture {}] start a cmd at term: {}, index: {}",
                self.me, term, index
            );
            Ok((index as u64, term as u64))
        } else {
            Err(Error::NotLeader)
        }
    }
}

impl Raft {
    /// Determines whether the logs of RequestVote RPC Caller is older than this node,
    /// If it is old, return true, otherwise false (including the same as new).
    ///
    /// `args_last_term` is the term of the last entry in Caller
    /// `args_last_index` is the index of the last entry in Caller
    fn is_newer(&self, args_last_term: u64, args_last_index: u64) -> bool {
        // Added a dummy log during initialization, so can be unwrap directly.
        // Since the last log is still retained after snapshot,
        // it is still safe to use last () here.
        let log = self.logs.last().unwrap();
        let (self_last_term, self_last_index) = (log.term, log.index);

        if self_last_term == args_last_term {
            // term is same, compare length
            self_last_index > args_last_index
        } else {
            // compare term
            self_last_term > args_last_term
        }
    }

    /// Determine whether the prev log of the AppendEntries RPC Caller
    /// matches the current node's log.
    /// If match returns true, otherwise it returns false.
    ///
    /// `args_prev_term` is the log term that Caller wants to match
    /// `args_prev_index` is the log index that Caller wants to match
    fn is_match(&self, args_prev_term: u64, args_prev_index: u64) -> bool {
        // `args_prev_index` is an absolute index,
        // accessing logs requires a relative index, so requires conversion
        let rel_index = self.relative_index(args_prev_index as usize);

        // Considering the existence of the partition problem,
        // after the old leader in a network partition is recovered,
        // it may send an AppendEntries request to the partition
        // where the new leader is located, and it will execute the `is_match` function,
        // so it may match the snapshot in the new partition.
        // Leading to negative index access.
        if rel_index.is_none() {
            // It returns true directly, in fact, it can also return false here.
            // it's because the term of the old leader must be smaller than that of the new leader,
            // it is impossible to append successfully.
            return true;
        }
        let rel_index = rel_index.unwrap();
        match self.logs.get(rel_index) {
            Some(log) => {
                assert_eq!(log.index, args_prev_index as u64);
                log.term == args_prev_term
            }
            None => {
                // Means the requester has more logs than the node's log.
                false
            }
        }
    }

    fn be_follower(&mut self) {
        self.is_leader.store(false, Ordering::SeqCst);
        self.role = Role::Follower;
        self.persist();
    }

    fn be_candidate(&mut self) {
        self.is_leader.store(false, Ordering::SeqCst);
        self.current_term.fetch_add(1, Ordering::SeqCst);
        self.role = Role::Candidate;
        self.voted_for = Some(self.me as u64);
        self.persist();

        self.send_request_vote_all();
    }

    fn be_leader(&mut self) {
        // 2B: Add initialization of `next_index` and `match_index`.
        // `next_index` is obviously also an absolute index and needs to be converted.
        let log_len = self.absolute_index(self.logs.len());
        for index in self.next_index.iter_mut() {
            *index = log_len;
        }
        for index in self.match_index.iter_mut() {
            *index = 0;
        }
        self.is_leader.store(true, Ordering::SeqCst);
        self.role = Role::Leader;
        self.persist();
    }

    /// Processes RPC RequestVote requests,
    /// returns a Reply and a bool value indicating whether args.term is greater than term.
    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> (RequestVoteReply, bool) {
        // RequestVoteArgs { term, candidate_id }
        let term = self.current_term.load(Ordering::SeqCst);

        // 2A: Anyone who finds that the requester's term
        // is larger than himself, agrees to vote.
        // 2B: Raft Paper 5.4.1 election restrictions.
        // Regardless, args.term updates its term as long as it is bigger than itself.
        // A candidate promotes its term multiple times when the network is blocked,
        // This is useful for updating the cluster's term immediately after recovery.
        if args.term > term {
            self.current_term.store(args.term, Ordering::SeqCst);
        }

        let vote_granted =
            if args.term < term || self.is_newer(args.last_log_term, args.last_log_index) {
                // Refuse to vote as long as the requester's term
                // is found to be smaller than himself.
                // Refuse to vote as long as the requester's log
                // is found to be older.
                false
            } else if args.term > term {
                // Requester's log is newer and larger than his own.
                true
            } else {
                // I have voted for the requester during this term
                // and still agree to vote.
                match self.voted_for {
                    Some(peer) => peer == args.candidate_id,
                    None => true,
                }
            };

        if vote_granted {
            // Agree to vote, update `voted_for`
            self.voted_for = Some(args.candidate_id);
        }
        self.persist();

        (RequestVoteReply { term, vote_granted }, args.term > term)
    }

    /// Processes RPC AppendEntries requests, returns a Reply
    /// and a bool value indicating whether args.term is greater than or equal to term.
    fn handle_append_entries(&mut self, mut args: AppendEntriesArgs) -> (AppendEntriesReply, bool) {
        // AppendEntriesArgs { term, leader_id }
        // Here `prev_index` is the absolute index passed by the caller
        let prev_index = args.prev_log_index;
        let prev_term = args.prev_log_term;
        let term = self.current_term.load(Ordering::SeqCst);

        if args.term > term {
            self.current_term.store(args.term, Ordering::SeqCst);
        }
        // There is no problem passing in the absolute index here,
        // and the interface conventions are consistent.
        let log_match = self.is_match(prev_term, prev_index);
        let success = args.term >= term && log_match;
        if success {
            if !args.entries.is_empty() {
                // Matches and `entries` are not empty
                // There is no more search for conflict locations here,
                // and the log of length `prev_index + 1` is directly truncated.
                // Accessing logs requires relative indexing.
                let rel_index = self.relative_index(prev_index as usize);
                // Able to ensure that the relative index is positive.
                if rel_index.is_some() {
                    self.logs.truncate(rel_index.unwrap() + 1);
                    self.logs.append(&mut args.entries);
                }
            }
            // Match, and after adding a new entry,
            // judge the `commit_index` sent by the leader.
            if args.leader_commit > self.commit_index as u64 {
                // Update `commit_index` to the smaller of `args.leader_commit`
                // and the index value of the new log entry.
                self.commit_index = min(
                    args.leader_commit as usize,
                    prev_index as usize + args.entries.len(),
                );
            }
            self.persist();
        }

        if log_match {
            (
                AppendEntriesReply {
                    term,
                    success,
                    conflict_index: 0,
                    conflict_term: 0,
                },
                args.term >= term,
            )
        } else {
            // For the same reason, you need to convert to a relative index.
            let rel_index = self.relative_index(prev_index as usize);
            assert!(rel_index.is_some());
            let prev_index = rel_index.unwrap();
            let (conflict_index, conflict_term) = match self.logs.get(prev_index) {
                Some(log) => {
                    // Go over all log entries that conflict with that term
                    // and find the earliest log index for that term.
                    let mut index = prev_index;
                    for i in (0..=prev_index).rev() {
                        if self.logs[i as usize].term != log.term {
                            index = i + 1;
                            break;
                        }
                    }
                    // Convert to absolute index.
                    (self.absolute_index(index), log.term)
                }
                None => {
                    // Means the requester has more logs than the node's log.
                    (self.absolute_index(self.logs.len()), 0)
                }
            };
            (
                AppendEntriesReply {
                    term,
                    success,
                    conflict_index: conflict_index as u64,
                    conflict_term,
                },
                args.term >= term,
            )
        }
    }

    /// Process RPC InstallSnapshot request, return Reply.
    fn handle_install_snapshot(
        &mut self,
        args: InstallSnapshotArgs,
    ) -> (InstallSnapshotReply, bool) {
        let last_index = args.last_included_index;
        let last_term = args.last_included_term;
        let term = self.current_term.load(Ordering::SeqCst);

        if args.term > term {
            self.current_term.store(args.term, Ordering::SeqCst);
        }
        // success indicates whether the snapshot can be successfully installed.
        let success = args.term >= term && last_index > (self.last_included_index as u64);
        if success {
            if last_index < (self.absolute_index(self.logs.len() - 1) as u64) {
                self.logs
                    .drain(..self.relative_index(last_index as usize).unwrap());
            } else {
                // It means that the existing log has been completely covered by snapshot.
                self.logs = vec![Log {
                    term: last_term,
                    index: last_index,
                    command: vec![],
                }];
            }
            // Update and persist snapshot.
            self.last_included_index = last_index as usize;
            self.last_included_term = last_term;
            self.persist_state_and_snapshot(args.data.clone());
            // Update here is different from append_commit_index and last_applied.
            // install snapshot is very simple.
            self.commit_index = max(self.commit_index, last_index as usize);
            self.last_applied = max(self.last_applied, last_index as usize);

            let msg = ApplyMsg {
                command_valid: false,
                command: args.data,
                // The following two don't matter
                command_index: 0,
                command_term: 0,
            };
            self.apply_ch.unbounded_send(msg).unwrap_or_else(|_| ());
        }

        (InstallSnapshotReply { term }, args.term >= term)
    }

    /// Send voting requests to all peers.
    fn send_request_vote_all(&self) {
        let term = self.current_term.load(Ordering::SeqCst);
        // Using last () is safe
        let log = self.logs.last().unwrap();
        let args = RequestVoteArgs {
            term,
            candidate_id: self.me as u64,
            last_log_index: log.index,
            last_log_term: log.term,
        };

        let me = self.me;
        // RequestVote Reply receivers.
        let result_rxs: Vec<Receiver<Result<RequestVoteReply>>> = self
            .peers
            .iter()
            .enumerate()
            .filter(|(id, _)| {
                // Sift yourself
                *id != me
            })
            .map(|(id, _)| self.send_request_vote(id, &args))
            .collect();

        // Initialize to 1, vote for yourself by default
        let mut vote_cnt = 1 as usize;
        let group_num = self.peers.len();
        let tx = self.msg_tx.clone();

        // Here `take_while` will terminate the execution of the stream after reaching half,
        // As a result, some rx ends in result_rxs are destroyed in advance,
        // So an unwrap on a `Err` exception will appear in the send_request_vote function.
        // Need to ignore error in send_request_vote.
        let stream = futures::stream::futures_unordered(result_rxs)
            .take_while(move |reply| {
                // Err appears due to the interference of the virtual network,
                // ignore Err's Reply.
                if let Ok(reply) = reply {
                    info!("[send_request_vote_all {}] get vote reply: {:?}", me, reply);
                    if reply.vote_granted {
                        // Agree to vote
                        vote_cnt += 1;
                        if vote_cnt * 2 > group_num {
                            // More than half.
                            // Send a `SuccessElection` message to the state machine
                            tx.unbounded_send(ActionEv::SuccessElection(term))
                                .map_err(|e| {
                                    debug!(
                                        "[send_request_vote_all {}] send Success Election fail {}",
                                        me, e
                                    );
                                })
                                .unwrap_or_else(|_| ());
                            // stream finish
                            ok(false)
                        } else {
                            ok(true)
                        }
                    } else {
                        // Disagree to vote, see response term:
                        // Disagree reason 1: reply.term > term
                        if reply.term > term {
                            // Send a Fail message to the state machine.
                            tx.unbounded_send(ActionEv::Fail(reply.term))
                                .map_err(|e| {
                                    debug!(
                                        "[send_request_vote_all {}] send ActionEv::Fail fail {}",
                                        me, e
                                    );
                                })
                                .unwrap_or_else(|_| ());
                            // stream finish
                            ok(false)
                        } else {
                            // Disagree Reason 2: The log of this node is not new enough.
                            ok(true)
                        }
                    }
                } else {
                    ok(true)
                }
            })
            .for_each(|_| ok(()))
            // Canceled Err on the send side should not appear.
            .map_err(|_| ());

        tokio::spawn(stream);
    }

    /// Help send_heartbeat_all calculate the AppendEntries RPC parameters sent to the server.
    fn set_append_entries_arg(&self, term: u64, server: usize) -> AppendEntriesArgs {
        let prev_log_index = self.next_index[server] - 1;
        // The absolute index stored in next_index needs to be converted to a relative index.
        // Here it is required to check whether `prev_log_index`
        // does not fall within the scope of snapshot when calling this function.
        let rel_index = self.relative_index(prev_log_index);
        assert!(rel_index.is_some());
        let rel_index = rel_index.unwrap();
        let prev_log_term = self.logs[rel_index].term;

        let entries = if self.next_index[server] > self.match_index[server] + 1 {
            Vec::with_capacity(0)
        } else {
            Vec::from(&self.logs[(rel_index + 1)..])
        };

        AppendEntriesArgs {
            term,
            leader_id: self.me as u64,
            prev_log_index: prev_log_index as u64,
            prev_log_term,
            leader_commit: self.commit_index as u64,
            entries,
        }
    }

    /// Help send_heartbeat_all calculate the InstallSnapshot RPC parameters sent to the server.
    fn set_install_snapshot_arg(&self, term: u64) -> InstallSnapshotArgs {
        InstallSnapshotArgs {
            term,
            leader_id: self.me as u64,
            last_included_index: self.last_included_index as u64,
            last_included_term: self.last_included_term,
            data: self.persister.snapshot(),
        }
    }

    /// Send AppendEntries RPC to all peers.
    fn send_heartbeat_all(&self) {
        let term = self.current_term.load(Ordering::SeqCst);

        let me = self.me;
        // AppendEntries Reply receivers
        let result_rxs: Vec<Receiver<SyncReply>> = self
            .peers
            .iter()
            .enumerate()
            .filter(|(id, _)| {
                // Sift yourself
                *id != me
            })
            .map(|(id, _)| {
                if self.next_index[id] <= self.last_included_index {
                    // The required next_index is already included in the snapshot of this node.
                    // send snapshot
                    self.send_install_snapshot(id, &self.set_install_snapshot_arg(term))
                } else {
                    // Send append entries as usual.
                    self.send_heartbeat(id, &self.set_append_entries_arg(term, id))
                }
            })
            .collect();

        let tx = self.msg_tx.clone();
        let stream = futures::stream::futures_unordered(result_rxs)
            .for_each(move |sync_reply| {
                match sync_reply {
                    SyncReply::Log((reply, id, prev_index, entries_len)) => {
                        if let Ok(reply) = reply {
                            info!(
                                "[send_heartbeat_all {}] get AppendEntries reply from StateFuture {}: {:?}",
                                me, id, reply
                            );

                            if reply.success {
                                // The log matches and the local term is greater than
                                // or equal to the counterpart term.
                                // Because of the limitation of move, send the message
                                // that updates `match_index` and `next_index` to StateFuture.
                                debug!(
                                    "[send_heartbeat_all {}] StateFuture {}'s log match with me",
                                    me, id
                                );
                                tx.unbounded_send(ActionEv::UpdateIndex(
                                    term,
                                    id,
                                    Ok(prev_index + entries_len + 1),
                                ))
                                    .map_err(|e| {
                                        debug!(
                                            "[send_heartbeat_all {}] send ActionEv::UpdateIndex fail {}",
                                            me, e
                                        );
                                    })
                                    .unwrap_or_else(|_| ());
                            } else if reply.term > term {
                                // Return to follower immediately.
                                tx.unbounded_send(ActionEv::Fail(reply.term))
                                    .map_err(|e| {
                                        debug!(
                                            "[send_heartbeat_all {}] send ActionEv::Fail fail {}",
                                            me, e
                                        );
                                    })
                                    .unwrap_or_else(|_| ());
                            } else {
                                // Unmatched
                                // Send a message updating `next_index` to StateFuture.
                                debug!(
                                    "[send_heartbeat_all {}] StateFuture {}'s log mis-match with me",
                                    me, id
                                );
                                tx.unbounded_send(ActionEv::UpdateIndex(
                                    term,
                                    id,
                                    Err(reply.conflict_index as usize),
                                ))
                                    .map_err(|e| {
                                        debug!(
                                            "[send_heartbeat_all {}] send ActionEv::UpdateIndex fail {}",
                                            me, e
                                        );
                                    })
                                    .unwrap_or_else(|_| ());
                            }
                        }
                        ok(())
                    },
                    SyncReply::Snapshot((reply, id, last_index)) => {
                        if let Ok(reply) = reply {
                            info!(
                                "[send_heartbeat_all {}] get InstallSnapshot reply from StateFuture {}: {:?}",
                                me, id, reply
                            );
                            if reply.term > term {
                                // Return to follower immediately.
                                tx.unbounded_send(ActionEv::Fail(reply.term))
                                    .map_err(|e| {
                                        debug!(
                                            "[send_heartbeat_all {}] send ActionEv::Fail fail {}",
                                            me, e
                                        );
                                    })
                                    .unwrap_or_else(|_| ());
                            } else {
                                // Update `match_index` and `next_index`
                                tx.unbounded_send(ActionEv::UpdateIndex(
                                    term,
                                    id,
                                    Ok(last_index + 1),
                                ))
                                    .map_err(|e| {
                                        debug!(
                                            "[send_heartbeat_all {}] send ActionEv::UpdateIndex fail {}",
                                            me, e
                                        );
                                    })
                                    .unwrap_or_else(|_| ());
                            }
                        }
                        ok(())
                    }
                }
            })
            .map_err(|_| ());

        tokio::spawn(stream);
    }

    /// Try to update the commit_index every time the leader's match_index changes.
    fn update_commit_index(&mut self) {
        // 1. Most matchIndex [i] ≥ N holds
        let mut tmp_match_index = self.match_index.clone();
        // Since match_index is also stored as an absolute index, it needs to be converted.
        tmp_match_index[self.me] = self.absolute_index(self.logs.len()) - 1;
        tmp_match_index.sort_unstable();

        let group_num = self.peers.len();
        // Then the possible range of N falls
        // at 0 .. (group_num-1) / 2 index position of `tmp_match_index`.
        tmp_match_index.truncate((group_num + 1) / 2);

        let current_term = self.current_term.load(Ordering::SeqCst);
        let new_commit = tmp_match_index
            .into_iter()
            .filter(|n| {
                // 2. N > commitIndex
                // 3. log[N].term == currentTerm
                // Access logs using relative index, conversion.
                let rel_index = self.relative_index(*n);
                match rel_index {
                    Some(rel_index) => {
                        *n > self.commit_index && self.logs[rel_index].term == current_term
                    }
                    None => false,
                }
            })
            .max();

        if let Some(n) = new_commit {
            debug!(
                "[StateFuture {}] update commit_index from {} to {}",
                self.me, self.commit_index, n
            );
            self.commit_index = n;
            // Updated commit_index to try apply log.
            self.apply_msg();
        }
    }

    /// Submit log to application layer and change `last_applied`.
    fn apply_msg(&mut self) {
        while self.last_applied < self.commit_index {
            let apply_idx = self.last_applied + 1;
            // Convert to relative index.
            let rel_index = self.relative_index(apply_idx);
            if rel_index.is_none() {
                self.last_applied += 1;
                continue;
            }
            let rel_index = rel_index.unwrap();
            let msg = ApplyMsg {
                command_valid: true,
                command: self.logs[rel_index].command.clone(),
                command_index: apply_idx as u64,
                command_term: self.logs[rel_index].term,
            };
            self.apply_ch.unbounded_send(msg).unwrap_or_else(|_| ());
            self.last_applied += 1;
        }
    }
}

/// Define state machine events.
enum TimeoutEv {
    /// Election Timeout
    Election,
    /// Heartbeat Timeout
    Heartbeat,
}

enum ActionEv {
    /// Node receives RequestVote RPC from other nodes
    /// Use oneshot to send results to the asynchronously waiting receiver
    RequestVote(RequestVoteArgs, Sender<RequestVoteReply>),
    /// Node receives AppendEntries RPC from other nodes
    AppendEntries(AppendEntriesArgs, Sender<AppendEntriesReply>),
    /// Node receives InstallSnapshot RPC from other nodes
    InstallSnapshot(InstallSnapshotArgs, Sender<InstallSnapshotReply>),
    /// Win in an election, including a winning term.
    SuccessElection(u64),
    /// Failure in an election
    /// including the term of the sender that caused the failure.
    Fail(u64),
    /// After broadcasting the heartbeat, update `match_index` and `next_index`
    /// Ok (usize) means the new next_index after a successful match,
    /// and update match_index accordingly
    /// Err (usize) means the new next_index after the match fails,
    /// no need to update match_index.
    /// u64 stores term, same as above
    /// usize stores id
    UpdateIndex(u64, usize, std::result::Result<usize, usize>),
    StartCmd(Vec<u8>, Sender<Result<(u64, u64)>>),
    LocalSnapshot(usize, Vec<u8>),
    /// shut down state machine
    Kill,
}

/// `StateFuture` implement `Stream` Trait
/// Driven by the executor, the function of the asynchronous state machine will be completed.
/// `StateFuture` treats events such as timeout and other nodes' RPCs as `Future`
/// `StateFuture` asynchronously waits for the event to arrive
/// and changes the Raft state accordingly.
///
/// So `StateFuture` includes ownership of` Raft`
/// RPC event asynchronous receivers are included in the Raft structure.
/// The `timeout` event is implemented by the internal `Delay`
/// `timeout_ev` means timeout event
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
                        // Call Raft::handle_request_vote, return reply
                        // Send reply via tx
                        info!("[StateFuture {}] get RequestVote event", self.raft.me);
                        let (reply, args_term_gtr) = self.raft.handle_request_vote(args);
                        let vote_granted = reply.vote_granted;
                        tx.send(reply).unwrap_or_else(|_| {
                            debug!("[StateFuture {}] send RequestVoteReply error", self.raft.me);
                        });
                        // 2B: vote_granted can no longer fully represent args.term> term
                        // So modify the handle_request_vote interface.
                        if args_term_gtr || vote_granted {
                            self.raft.be_follower();
                            // Reset timeout, set the next timeout election
                            self.timeout.reset(StateFuture::rand_election_timeout());
                            self.timeout_ev = TimeoutEv::Election;
                        }
                    }
                    ActionEv::AppendEntries(args, tx) => {
                        // Call Raft::handle_append_entries, return reply
                        info!("[StateFuture {}] get AppendEntries event", self.raft.me);
                        let (reply, args_term_ge) = self.raft.handle_append_entries(args);
                        tx.send(reply).unwrap_or_else(|_| {
                            debug!(
                                "[StateFuture {}] send AppendEntriesReply error",
                                self.raft.me
                            );
                        });
                        if args_term_ge {
                            info!(
                                "[StateFuture {}] After Handle AppendEntries => follower",
                                self.raft.me
                            );
                            // Vote marked as true, change raft status to follower.
                            self.raft.be_follower();
                            // Reset timeout, set the next timeout election.
                            self.timeout.reset(StateFuture::rand_election_timeout());
                            self.timeout_ev = TimeoutEv::Election;
                            // Update last_applied and submit logs to the application layer.
                            self.raft.apply_msg();
                        }
                    }
                    ActionEv::InstallSnapshot(args, tx) => {
                        // Call Raft::handle_install_snapshot, return reply.
                        info!("[StateFuture {}] get InstallSnapshot event", self.raft.me);
                        let (reply, args_term_ge) = self.raft.handle_install_snapshot(args);
                        tx.send(reply).unwrap_or_else(|_| {
                            debug!(
                                "[StateFuture {}] send InstallSnapshotReply error",
                                self.raft.me
                            );
                        });
                        if args_term_ge {
                            info!(
                                "[StateFuture {}] After Handle InstallSnapshot => follower",
                                self.raft.me
                            );
                            self.raft.be_follower();
                            self.timeout.reset(StateFuture::rand_election_timeout());
                            self.timeout_ev = TimeoutEv::Election;
                        }
                    }
                    ActionEv::SuccessElection(term) => {
                        info!("[StateFuture {}] get SuccessElection event", self.raft.me);
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
                    ActionEv::UpdateIndex(term, id, res) => {
                        info!("[StateFuture {}] get UpdateIndex event", self.raft.me);
                        if term == self.raft.current_term.load(Ordering::SeqCst) {
                            match res {
                                Ok(new_index) => {
                                    let prev_match_index = self.raft.match_index[id];
                                    if new_index - 1 > prev_match_index {
                                        self.raft.match_index[id] = new_index - 1;
                                        self.raft.next_index[id] = new_index;
                                        // match_index 发生变化 更新 commit_index
                                        self.raft.update_commit_index();
                                    }
                                }
                                Err(new_index) => {
                                    if new_index < self.raft.next_index[id] {
                                        self.raft.next_index[id] = new_index;
                                    }
                                }
                            }
                        }
                    }
                    ActionEv::StartCmd(cmd, tx) => {
                        let res = self.raft.start(cmd);
                        tx.send(res).unwrap_or_else(|_| {
                            debug!("[StateFuture {}] send StartCmd result error", self.raft.me);
                        });
                    }
                    ActionEv::LocalSnapshot(applied_index, snapshot) => {
                        let rel_index = self.raft.relative_index(applied_index);
                        if let Some(index) = rel_index {
                            self.raft.logs.drain(..index);
                            self.raft.last_included_index = applied_index;
                            self.raft.last_included_term = self.raft.logs[0].term;
                            self.raft.persist_state_and_snapshot(snapshot);
                        }
                    }
                    ActionEv::Kill => {
                        // Stream finish
                        info!("[StateFuture {}] killed", self.raft.me);
                        return Ok(Async::Ready(None));
                    }
                }
                return Ok(Async::Ready(Some(())));
            }
            Ok(Async::Ready(None)) => {
                // The action sender is closed, indicating that the Stream is complete.
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
                        // Change raft status to candidate.
                        self.raft.be_candidate();
                        // Reset timeout, set the next timeout election.
                        self.timeout.reset(StateFuture::rand_election_timeout());
                        self.timeout_ev = TimeoutEv::Election;
                    }
                    TimeoutEv::Heartbeat => {
                        info!(
                            "[StateFuture {}] Heartbeat Timeout at local term: {}",
                            self.raft.me,
                            self.raft.current_term.load(Ordering::SeqCst)
                        );
                        // send heartbeat
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
/// state_machine puts the `StateFuture` asynchronous state machine into a separate thread.
///
/// current_term and is_leader share ownership of corresponding
/// members in Raft (for term and is_leader methods).
///
/// msg_tx is obtained from the corresponding member clone in the Raft structure,
/// which is used to capture RPC calls and send messages to `StateFuture`.
#[derive(Clone)]
pub struct Node {
    // Your code here.
    state_machine: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    pub current_term: Arc<AtomicU64>,
    pub is_leader: Arc<AtomicBool>,
    persist_size: Arc<AtomicU64>,
    msg_tx: UnboundedSender<ActionEv>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        info!("[Node::new] node {} init", raft.me);
        let current_term = Arc::clone(&raft.current_term);
        let is_leader = Arc::clone(&raft.is_leader);
        let persist_size = Arc::clone(&raft.persist_size);
        let msg_tx = raft.msg_tx.clone();
        let state_future = StateFuture::new(raft);

        let handle = thread::spawn(move || tokio::run(state_future.for_each(|_| ok(()))));

        Node {
            state_machine: Arc::new(Mutex::new(Some(handle))),
            current_term,
            is_leader,
            persist_size,
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
        if !self.is_leader() {
            return Err(Error::NotLeader);
        }

        let (tx, rx) = channel();
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;

        if !self.msg_tx.is_closed() {
            self.msg_tx
                .clone()
                .unbounded_send(ActionEv::StartCmd(buf, tx))
                .map_err(|_| ())
                .unwrap_or_else(|_| ());
        } else {
            return Err(Error::NotLeader);
        }

        if let Ok(res) = rx.wait() {
            res
        } else {
            Err(Error::NotLeader)
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        self.current_term.load(Ordering::SeqCst)
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        self.is_leader.load(Ordering::SeqCst)
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// Get the amount of data in the persister
    ///
    /// It should be noted that the actual amount of data in the persister
    /// and the change in persist_size do not guarantee atomicity.
    ///
    /// So what is read may be the amount of data before the data is changed,
    /// so when determining whether a snapshot is needed, you need to set a
    /// trigger value lower than the maximum threshold, such as 80% -90%.
    pub fn persist_size(&self) -> u64 {
        self.persist_size.load(Ordering::SeqCst)
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
            self.msg_tx
                .unbounded_send(ActionEv::Kill)
                .unwrap_or_else(|_| ());
            // If you use join to wait for the thread to end,
            // you will wait for a long time after the test is finished.
            // Because there are new threads in send_heartbeat_all and
            // send_request_vote_all that have not ended.
            //
            // Currently, Elegant downtime has not been achieved.
            //            handle.join().unwrap();
        }
    }

    /// Call from KvServer, snapshot locally.
    ///
    /// applied_index is the last log index of this snapshot
    pub fn local_snapshot(&self, applied_index: usize, snapshot: Vec<u8>) {
        if !self.msg_tx.is_closed() {
            self.msg_tx
                .clone()
                .unbounded_send(ActionEv::LocalSnapshot(applied_index, snapshot))
                .map_err(|_| ())
                .unwrap_or_else(|_| ());
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
                .unwrap_or_else(|_| ());
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
                .unwrap_or_else(|_| ());
        }
        Box::new(rx.map_err(|_| labrpc::Error::Other("Append Entries Receive Error".to_owned())))
    }

    fn install_snapshot(&self, args: InstallSnapshotArgs) -> RpcFuture<InstallSnapshotReply> {
        let (tx, rx) = channel();

        if !self.msg_tx.is_closed() {
            self.msg_tx
                .clone()
                .unbounded_send(ActionEv::InstallSnapshot(args, tx))
                .map_err(|_| ())
                .unwrap_or_else(|_| ());
        }
        Box::new(rx.map_err(|_| labrpc::Error::Other("Install Snapshot Receive Error".to_owned())))
    }
}
