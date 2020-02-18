# Raft 实验报告
## Part 2a
### 协议
在 Raft 实验 2a 部分，主要要求实现领导者选举，并能应
对节点宕机的情况。所以需要实现将围绕如下两个 `RPC`：

- RequestVote
- AppendEntries

在 [Raft Paper][Raft Paper] 中给出了两种 `RPC` 的
完整参数和返回值的定义，但是鉴于目前的需求仅仅是实现领导人
选举所以并不需要全部的参数。

[Raft Paper]:https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md

对于 `RequestVote` 来说，由于单纯的选举不需要对 `log` 
进行维护，所以无需考虑候选人持有的日志集是否比本节点的新，
故 `RequestVote` 定义如下：

```
message RequestVoteArgs {
    // Your data here (2A, 2B).
    uint64 term = 1;                // candidate's term
    uint64 candidate_id = 2;        // candidate requesting vote
}

message RequestVoteReply {
    // Your data here (2A).
    uint64 term = 1;                // currentTerm, for candidate to update itself
    bool vote_granted = 2;          // true means candidate received vote
}
```

对于 `AppendEntries` 来说，同样的，不需要维护和存储 `log`
的字段，所以 `AppendEntries` 的定义目前也很简单：

```
message AppendEntriesArgs {
    uint64 term = 1;                // leader's term
    uint64 leader_id = 2;           // so followers can redirect clients
}

message AppendEntriesReply {
    uint64 term = 1;                // currentTerm, for leader to update itself
    bool success = 2;               // true if follower contained entry match
}
```

### 状态机
去年暑假做过一次 `The Raft Lab` ，当时主要是使用 `std::thread`
来实现的，并且对于并发数据共享的处理采用的是粗粒度的 `Arc<Mutex<>>`
对数据加锁，导致性能不是很高，总之个人不是很满意。

这一次重新实现，是想借助最近学习的异步编程组件 [futures][futures] 和
[tokio][tokio]这两个 `crate` 来获得更好的性能。在实现的时候 `futures`
已经到了 0.3 版本，同时 `tokio` 也发布了 0.2 版本。其中 `futures` 提供
了 `select!` 宏

[tokio]:https://docs.rs/tokio/0.1.22/tokio/index.html
[futures]:https://docs.rs/futures/0.1.29/futures/