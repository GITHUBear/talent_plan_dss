# Raft 实验报告

## 当前版本存在的一些问题

1. 目前未实现优雅地停机
2. 不优雅的停机导致通道发送与接收会出现彼端已经销毁的错误，对于这些错误的处理
也不够优雅，基本就是 `unwrap_or_else` 对错误进行忽略
3. 由于停机不优雅，导致 3a 部分 `linearizability` 的一个测试有较低概率在
如下代码处出现 unwrap err(kvraft/tests.rs:549)：

    ```rust
    Arc::try_unwrap(operations).unwrap().into_inner().unwrap()
    ```

   猜测可能是 `KvServer` 尚未被 kill 使得锁未释放导致的，所以我在之前调用
drop，销毁 cfg，使之调用 KvServer 的 kill 方法来结束状态机，之后测试基本
不再出现这种情况。

4. 框架代码自身还有问题，在 labrpc 的测试中，`test_killed` 不能稳定通过，错误
如下图所示，个人觉得这不是什么大问题：

![labrpc_error](./error.jpg)

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
对数据加锁，导致性能不是很高且稍不留神就可能导致死锁，总之个人不是很满意。

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

为了验证这样的设计方案是否可行，我编写了一个简单的测试程序：一个受控的可重置超时的 `interval`：

```rust
struct AsyncEvent {
    timeout: Delay,
    action_ev: UnboundedReceiver<()>,
}

impl AsyncEvent {
    fn new(rx: UnboundedReceiver<()>) -> Self {
        AsyncEvent {
            timeout: Delay::new(Duration::from_millis(2000)),
            action_ev: rx,
        }
    }
}

impl Stream for AsyncEvent {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<()>, ()> {
        match self.action_ev.poll() {
            Ok(Async::Ready(Some(()))) => {
                let rand_dur = rand::thread_rng().gen_range(1, 4);
                info!("Get action event, reset timeout : {}", rand_dur);
                self.timeout
                    .reset(Duration::from_millis(rand_dur * 1000));
                return Ok(Async::Ready(Some(())));
            }
            Ok(Async::Ready(None)) => {
                return Ok(Async::Ready(None));
            }
            Ok(Async::NotReady) => {}
            Err(()) => {
                error!("channel Error");
            }
        }
        match self.timeout.poll() {
            Ok(Async::Ready(())) => {
                let rand_dur = rand::thread_rng().gen_range(1, 4);
                info!("timeout, reset timeout : {}", rand_dur);
                self.timeout
                    .reset(Duration::from_millis(rand_dur * 1000));
                return Ok(Async::Ready(Some(())));
            }
            Ok(Async::NotReady) => {}
            Err(e) => {
                error!("{:?}", e);
            }
        }

        Ok(Async::NotReady)
    }
}

fn main() {
    env_logger::init();

    let (tx, rx) = unbounded::<()>();
    let event = AsyncEvent::new(rx);

    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(3000));
        info!("Send action event");
        tx.unbounded_send(()).unwrap();
    });

    let handle = thread::spawn(move || {
        tokio::run(event.take(10).for_each(|_| {
            info!("beep");
            ok(())
        }))
    });

    handle.join().unwrap();
}
```

构建一个处理 `Delay` 和通道接收事件的 `Stream`，创建一个线程周期性地发送消息，在另一个线程中通过 tokio 
执行者运行这个 `Stream`，运行日志如下，符合设计预期：

```
[2020-02-28T06:20:07Z INFO  test_code_for_raft] timeout, reset timeout : 2
[2020-02-28T06:20:07Z INFO  test_code_for_raft] beep
[2020-02-28T06:20:08Z INFO  test_code_for_raft] Send action event
[2020-02-28T06:20:08Z INFO  test_code_for_raft] Get action event, reset timeout : 3
[2020-02-28T06:20:08Z INFO  test_code_for_raft] beep
[2020-02-28T06:20:11Z INFO  test_code_for_raft] Send action event
[2020-02-28T06:20:11Z INFO  test_code_for_raft] Get action event, reset timeout : 1
[2020-02-28T06:20:11Z INFO  test_code_for_raft] beep
[2020-02-28T06:20:12Z INFO  test_code_for_raft] timeout, reset timeout : 3
[2020-02-28T06:20:12Z INFO  test_code_for_raft] beep
[2020-02-28T06:20:14Z INFO  test_code_for_raft] Send action event
[2020-02-28T06:20:14Z INFO  test_code_for_raft] Get action event, reset timeout : 3
[2020-02-28T06:20:14Z INFO  test_code_for_raft] beep
[2020-02-28T06:20:17Z INFO  test_code_for_raft] Send action event
[2020-02-28T06:20:17Z INFO  test_code_for_raft] Get action event, reset timeout : 3
[2020-02-28T06:20:17Z INFO  test_code_for_raft] beep
[2020-02-28T06:20:20Z INFO  test_code_for_raft] Send action event
[2020-02-28T06:20:20Z INFO  test_code_for_raft] Get action event, reset timeout : 1
[2020-02-28T06:20:20Z INFO  test_code_for_raft] beep
[2020-02-28T06:20:21Z INFO  test_code_for_raft] timeout, reset timeout : 2
[2020-02-28T06:20:21Z INFO  test_code_for_raft] beep
[2020-02-28T06:20:23Z INFO  test_code_for_raft] Send action event
[2020-02-28T06:20:23Z INFO  test_code_for_raft] Get action event, reset timeout : 1
[2020-02-28T06:20:23Z INFO  test_code_for_raft] beep
[2020-02-28T06:20:24Z INFO  test_code_for_raft] timeout, reset timeout : 2
[2020-02-28T06:20:24Z INFO  test_code_for_raft] beep
```

这样，有了 `StateFuture` 这个中间层之后，`Node`、`StateFuture` 和 `Raft` 三个结构的模块关系就可以
设计出来，用下图来表示：

```
+-----------------------------------------------------+
|    +---------+                                      |
|    |   Raft  |  note: maintain state & persist      |
|    +---------+                                      |
|         A                                           |
|         |  control                                  |
|         +---------- StateFuture                     |
+-----------------------------------------------------+
                          A
                          | send message
                  +---------------+
                  |      Node     |
                  +---------------+  
```

`Node` 负责提供 Raft 服务接口，接收 RPC 调用发送消息给 `StateFuture` 处理调用并最终控制 Raft 状态的
改变和持久化。

不过，如果 `StateFuture` 直接拥有了 `Raft` 的所有权，那么 `Node` 在每次调用 `is_leader` 和 `term` 
的时候都必须以发送消息的形式通过 `StateFuture` 来得到，这样做性能上并不高。所以考虑数据共享的方式，同时
鉴于 `atomic` 的效率要高于 `Mutex`，于是 `Node` 最终结构设计为：

```rust
pub struct Node {
    // Your code here.
    state_machine: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    current_term: Arc<AtomicU64>,
    is_leader: Arc<AtomicBool>,
    msg_tx: UnboundedSender<ActionEv>,
}
```

`current_term` 和 `is_leader` 就是上面提到的设计，`state_machine` 则是为了实现停机设计的，使用 `Mutex`
是因为 Node::kill 接口使用的是 &Self，不可变，就通过 `Mutex` 来实现内部可变性，Option 则是为了防止二次 kill
以及防止 moved from borrowed value 错误。`msg_tx` 很简单，就是通过它来向 `StateFuture` 发送消息的。

### 实现细节

#### 1.并发地向集群发送消息

Raft Paper 对于 `RequestVote` RPC 的发送者和接收者应该作何操作给出了非常详细地介绍，这里的细节实现并不是想
再次阐述这些过程，而是介绍一下诸如发送者并行发送投票请求并检查是否超过半数这些行为我是如何实现的。

显然如果集群中有 n 个节点，就创建 n - 1 个线程发送并处理应答，然后各个线程通过共享一个 `AtomicU64` 来进行半数
检查这样的办法是不合理的，os 未必能够提供 n - 1 个线程且维护线程的上下文切换代价也很大，其次，达到半数之后还有大半
的线程实际上无需再执行了，通知这些线程立即结束的代价也很大更不用说浪费了近一半的线程资源。

观察实验框架给出的 `send_request_vote` 的推荐实现和接口定义：

```rust
fn send_request_vote(
        &self,
        server: usize,
        args: &RequestVoteArgs,
) -> Receiver<Result<RequestVoteReply>>{
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
}
```

可以看到方法内部启动一个执行者来执行 request_vote 这个 Future，并在得到结果后通过管道将结果返回，顺便一提如果使用
`futures::sync::oneshot::channel` 其接收端 Receiver 是实现了 Future Trait 的，也就是说，可以把发送 `RequestVote`
RPC 并等待应答的返回作为一个 Future，那么多个这样的 Future 就可以看成是 `Stream`，就可以在这样的一个 `Stream`
上异步等待各个 Future 的完成，完成一个就对投票计数进行加1操作，对应了 `for_each` 的行为。

那么如何把多个 `Future` 转换为 `Stream`，我接触到的有2种办法(虽然我后面还试了很多其他办法)：

- futures::stream::iter_ok
- futures::stream::futures_unordered

针对这些方案，我也编写了测试，其中没达到预期的方法在下面的测试中进行了注释：

```rust
fn main() {
    env_logger::init();

    let (tx1, rx1) = channel::<(u64, u64)>();
    let (tx2, rx2) = channel::<(u64, u64)>();
    let (tx3, rx3) = channel::<(u64, u64)>();
    let (tx4, rx4) = channel::<(u64, u64)>();

    let recv_vec = vec![rx1, rx2, rx3, rx4];

    let mut cnt = 0;
    let futs = futures::stream::futures_unordered(recv_vec)
        .for_each(move |(rx, id)| {
            cnt += rx;
            info!("receive {} from id: {} cnt: {}", rx, id, cnt);
            ok(())
            //        tokio::spawn(fut);
            //        ok::<_, ()>(())
        })
        .map_err(|_| ());

    //    let fut = ok::<_, ()>(())
    //        .and_then(|()| {
    //        for fut in recv_vec {
    //            tokio::spawn(fut);
    //        }
    //        ok(())
    //    });

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(3000));
        info!("[thread 1] send");
        tx1.send((1, 4)).unwrap();
    });

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(2000));
        info!("[thread 2] send");
        tx2.send((2, 3)).unwrap();
    });

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(1000));
        info!("[thread 3] send");
        tx3.send((3, 1)).unwrap();
    });

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(2000));
        info!("[thread 4] send");
        tx4.send((4, 2)).unwrap();
    });

    //    let futs = futures::stream::iter_ok::<_, ()>()
    //    let futs =
    //        futures::stream::futures_unordered(recv_vec);
    //    let fut = futs
    //        .for_each(|n| {
    //            info!("receive {}", n);
    //            ok(())
    //        });

    //    futures::future::join_all(recv_vec);
    tokio::run(futs);
}
```

测试代码希望找到一种能够比较严格地按照时序处理发送来的消息的实现方式(虽然这看起来貌似不太重要)。设置了4个线程，通过 sleep 的时长来设置消息的时序，
3号睡眠1s后发送，2、4睡眠2s后发送，1号睡眠3s后发送，希望找到一个方案，能够在1s后处理3号，接着2、4、1。
最后确定了 `future_unordered` 有时序行为。测试结果如下：

```
[2020-02-28T07:35:35Z INFO  test_code_for_raft] [thread 3] send
[2020-02-28T07:35:35Z INFO  test_code_for_raft] receive 3 from id: 1 cnt: 3
[2020-02-28T07:35:36Z INFO  test_code_for_raft] [thread 2] send
[2020-02-28T07:35:36Z INFO  test_code_for_raft] [thread 4] send
[2020-02-28T07:35:36Z INFO  test_code_for_raft] receive 2 from id: 3 cnt: 5
[2020-02-28T07:35:36Z INFO  test_code_for_raft] receive 4 from id: 2 cnt: 9
[2020-02-28T07:35:37Z INFO  test_code_for_raft] [thread 1] send
[2020-02-28T07:35:37Z INFO  test_code_for_raft] receive 1 from id: 4 cnt: 10
```

另外，对于如何在得到半数之后就结束，既然是 `Stream` 了就可以使用 `take_while` 轻松地做到这一点。

#### 2.并发的安全性

上面的问题将会进一步要求我们思考是否需要在 `Stream` 处理的过程中，投票达到半数后是否应该改变 raft 状态的问题。
事实上为了能够实现计数，在 `for_each` 的 closure 参数中加入了 move 的要求，而修改 raft 状态是需要 `&mut Self` 的，显然做不到。

但是，如果考虑 heartbeat RPC 的处理呢？move 的要求不存在了，是否就意味着可以根据回应修改 raft 状态呢？
需要注意的是每次心跳超时都会通过 tokio::spawn 在 tokio 提供的绿色线程池中运行消息发送、等待回应、处理的操作，
如果进而再进行状态改变，显然是可能导致数据竞争的。这样就带来了不安全性。

为了解决这个问题，退而求其次，考虑将所有状态的改变都交给 `StateFuture` 的异步消息等待过程去处理，
保证 raft 状态改变由 `StateFuture` 独占，而上面的 RPC 调用与应答 `Stream` 将通过发送消息的方式
通知 `StateFuture` 操作 raft 的状态。

但是问题又来了，因为不能保证发送的状态改变消息能够立即被 `StateFuture` 处理。比如有可能会出现一个稳定了的 leader
又一次收到选举成功的消息(由于之前某个任期的选举成功消息没有被及时收到，导致重启了另一次选举然后成为 leader)。

这个问题在 2a 部分不显著，但是如果再次改变状态为 leader，一个可能丢失 leadership 的节点将会莫名其妙地成为
leader，一个任期出现多个 leader 就会被允许。

实现中采用的解决方法是发送的状态改变的消息，需要附加当前的 `term`。虽然这样做还是有可能导致一个节点不能及时成为
leader 的情况存在，但是不安全的状态完全消除，而且在 unreliable net 的情况下，这种不及时更新状态的情况，可以
视为与网络超时性质类似的情况。

牺牲立即更新来换取系统的并发安全性，我认为是值得的。

## Part 2b

## Part 2c

考虑到 Raft Paper 对协议进行了详细地阐述，而且在 Part 2a 中已经大致讲明了设计的代码组织形式，添加新的 RPC 是比较
容易的。这里就不再对 b、c 部分进行详述。

# KvRaft 实验报告

## Part 3a

### 概述

part 3 部分对于具体实现并没有给出明确参考，自由度较高，不过大致的方向是明确的：

- `KvClient` 发送操作请求至 `KvServer`，并等待结果响应
- `KvServer` 通过 start 接口将操作转换为日志交由下层 `Raft` 处理
- `KvServer` 在 apply_ch 接收到了该操作日志的应用请求后，执行该操作，此时才真正返回操作结果

在 part 2 中，通过 Raft 共识算法达到了日志的复制一致性，但是 part 3a 中需要实现的线性一致性是一种强一致性，也就是说
上面 3 步朴素的方式要实现线性一致性是不足够的。

要实现线性一致性在 Server 端需要做到如下三点：

- 操作需要具有原子性
- 操作发生在 Invocation 和 Response 之间且只能发生一次
- 一旦更新被观察到，其他 client 在这之后必须也同样观察到更新

下面来分析一下朴素方式违反了哪些要求：

1. 由于网络的不可靠性的存在，一个已经被 `Raft` 提交到应用层的日志不能及时送达 client 端，导致用户再一次发送了相同的
请求，那么最终会导致 server 端误以为这是一个新的操作日志，继续交由 `Raft` 进行一致性处理并执行，这就违反了操作的
原子性以及 Invocation 和 Response 之间只执行一次的要求。

   解决方法是找到一种方式标记一个 client 的 Invocation 和 Response 区间，也就是意味着对于一个 client 的 put_append
和 get 调用每个这样的一个操作都只能分配一个唯一的标识，那么简单的做法就是设置一个在 client 单调递增的序列号，每次执行
调用的时候就递增序列号，并将该序列号作为本操作(包括之后因为失败而反复尝试的)的标记。然后在 server 端需要设置一个 client 到
其最新提交并应用的操作序列号的一个映射，来防止 server 对一个过旧的重复提交的操作反复执行。

2. 读请求不能直接地在 KvServer 上进行响应。考虑到网络分区的问题的存在，一个处于旧分区当中的过时 leader，如果直接响应
来自客户端的读请求显然会出现脏读，不能及时读到最新的数据，就会违反第三点。

   解决这个问题在本实验中采用了最简单的方式，把读请求当做操作日志类似处理即可，当然还存在着 `Read Index`、`Lease Read` 等
做法，本质上都是确保在接收到读请求的时候 server 仍是一个 leader，在应用完了读请求到来时的所有待应用操作日志来保证读取的一定是
当时最新的值。

3. 有关系统可用性。考虑一个 client 发送了一个操作给一个存在于旧网络分区中的过时 leader，那么这个操作在网络分区恢复之前绝对
不可能在这个旧 leader 领导的分区当中达到一致并提交应用。如果没有一个超时机制的存在这个 client 将会一直等待直到网络分区修复，
这显然是不能接受的。

   添加超时机制有两种选择：在 client 和 server 上，我没有想到这两种方式孰优孰劣。在实现的时候采用的是在 server 端进行。
   
### 实现细节

有了 `Raft` 的设计经验，`KvRaft` 部分的设计基本上也是如法炮制。`Clerk` 的设计比较简单，记得维护一个序列号以及一个之前成功执行操作
的 server id 即可。

`KvServer` 相对来说复杂一些，主要需要防止 RPC 调用的阻塞，服务器端不能死等一个操作日志被 apply，为了提高服务器端的并发度，
我的解决方法还是使用异步状态机。

类似的，服务器端在接收到一个 RPC 调用后，将发送对应的 RPC 处理消息到服务器中运行的状态机(包含一个处理结果的发送端，在状态机处理
完毕后发送结果)，而该 RPC 调用立即返回一个接收端的 Future。

#### KvServer 结构设计

`KvServer` 定义如下：

```rust
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
```

其中的 `log_index_channel_map` 是在之前的分析中尚未出现的，因为采用了 RPC 调用返回接收端的 Future，就需要
在 `apply_ch` 接收到一个来自 Raft 的操作日志的时候，分辨这个操作日志应该是哪次 RPC 调用的请求，所以就需要将
操作日志的索引与发送端建立一个映射进行保存，当然仅仅保存发送端并不足够，因为要求能够分辨出在服务器端处理相应日志的
过程中 leadership 是否发生了转移，所以需要日志 term、客户端名以及客户端序列号，操作方式等来唯一地确定这个日志
是否发生了覆盖。

在实际实现中，对于 leadership 的丢失检查其实不是绝对严格的，允许一个 leader 在失去领导后，再一次获得领导权，
然后提交了之前一个任期中尚未提交的日志，以减少客户端的等待，当然也极有可能在这样一个过程中，client 已经接收到了
超时操作的应答。

#### 服务器端处理超时

可以想到如果将超时处理放到 `KvServer` 的状态机中进行处理，会带来很多实现上的困难，所以在实际实现的时候，采用了在
RPC 调用方法中添加一个 Delay Future。

具体实现代码如下：

```rust
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
```

使用 select 方法处理异步等待两个事件中的一个完成的情况，通过 map 将 Delay 超时事件先发生情况下需要发送的
Reply 发送给 client，这样 client 就会选择其他服务器提交操作，而不是在一段未知时间内等待网络分区恢复。

## Part 3b

由于采用状态机的设计方式，代码扩展变得比较容易，对于 Part 3B 仅仅是做一下过程梳理：

`KvServer` 端：

- 在 `KvServer` 处增加根据当前 db 状态创建 snapshot 的方法，以及从 persister 中恢复 db 状态的方法
- 在每次 apply_msg 中有日志达成一致并提交成功应用之后，就检查 persister 中的日志数据量大小，如果达到阈值
就，创建 snapshot，将最后一条应用的日志的索引与 snapshot bytes 交给 `Raft` 进行处理，下面称之为 `local_snapshot` 事件
- 如果 apply_msg 中发送来的是 snapshot，就将 db 置为指定 snapshot 的状态

`Raft` 端：

- 支持以 snapshot 的 last_included_index 为基准的 log 访问
- 增加对于 `local_snapshot` 事件的处理，同样根据 Part 2a 中的并发安全性，这里也是将这一事件交由状态机来处理
- 在 send_heartbeat_all 中增加对于 next_index 被 leader 的 snapshot 包含的情况处理，发生这种情况该发送 `install snapshot` RPC
- 添加 `install snapshot` RPC 的发送方参数设置逻辑，以及接收方的处理逻辑

另外还需要说明的是，`KvServer` 结构中并不包含 persister，也就是说 `KvServer` 每次检查 persister 的日志数据量
的大小都必须通过 Raft 来获得，为了并发安全，Raft 由状态机独占，如果把数据量查询也设计为发送消息并返回的方式显然是不
太合理的，所以实际实现的时候，我又设置了一个共享所有权的原子量记录数据量。

但是原子量仅仅保证自己的原子更新，不能保证 persister 的实际数据量的改变和原子量的改变不是同步的，就会导致实际上 persister 的
数量已经超过阈值但是读取原子值尚未超过，所以实现的时候将阈值设置为了给定阈值的 60%。
