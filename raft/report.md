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
了 `select!` 宏，对于实现异步等待多个事件中最先完成的一个这样的需求带来很大
的方便，在实现 `Raft` 状态机的时候也是非常方便的：比如编写一个loop，不断地
等待超时、`RequestVote` RPC 和 `AppendEntries` RPC 这三个事件中到来的
一个事件，然后做相应的处理。

[tokio]:https://docs.rs/tokio/0.1.22/tokio/index.html
[futures]:https://docs.rs/futures/0.1.29/futures/

但是不幸的是，实验框架依赖于 `futures-0.1`，就不能使用上述非常直接的状态机
实现方式。由于前不久在看一个 `futures` 和 `tokio` 的[教程][futures_tutorial]，
里面的例子是用 `futures::stream` 实现了一个 `Interval`，使我联想到一个状态机
也是可以描述为一个 `Stream` 的。

[futures_tutorial]:https://rust.cc/article?id=2d7447ab-f1b7-4f24-95c8-13c216c56974

状态机是一个这样的 `Stream`：异步等待多个事件并在其中任一事件发生后被唤醒，进行
状态改变的操作，并在未接收到终止事件之前一直无休止地做着这样的任务。

所以我定义了如下的结构：

```Rust
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
```

