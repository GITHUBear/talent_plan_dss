use futures::Future;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::proto::kvraftpb::*;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    // get 和 put_append 的接口为 &self, 需要获得内部可变性
    pub ex_leader: AtomicU64, // 保存前一次成功得到 RPC 应答的 server id
    pub seq: AtomicU64, // 保存提交到服务端的请求的序列号，防止在同一个服务端提交多次同一个请求
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk {
            name,
            servers,
            ex_leader: AtomicU64::new(0),
            seq: AtomicU64::new(1),
        }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        //        crate::your_code_here(key)
        let group_num = self.servers.len();
        let mut idx = self.ex_leader.load(Ordering::SeqCst);
        let cur_seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let args = GetRequest {
            key,
            seq: cur_seq,
            name: self.name.clone(),
        };
        loop {
            if let Ok(reply) = self.servers[idx as usize].get(&args).wait() {
                if !reply.wrong_leader {
                    // 说明在消息发送到服务端时该节点还是 leader
                    if reply.err.is_empty() {
                        // 在完成 commit 期间始终保持 leader 状态
                        self.ex_leader.store(idx, Ordering::SeqCst);
                        return reply.value;
                    }
                    // 发生 leader 转移
                }
            }
            // 否则说明 can't reach, 尝试下一个
            idx = (idx + 1) % (group_num as u64);
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let group_num = self.servers.len();
        let mut idx = self.ex_leader.load(Ordering::SeqCst);
        let cur_seq = self.seq.fetch_add(1, Ordering::SeqCst);
        let args = match op {
            Op::Put(key, value) => PutAppendRequest {
                key,
                value,
                op: 1,
                seq: cur_seq,
                name: self.name.clone(),
            },
            Op::Append(key, append) => PutAppendRequest {
                key,
                value: append,
                op: 2,
                seq: cur_seq,
                name: self.name.clone(),
            },
        };
        loop {
            if let Ok(reply) = self.servers[idx as usize].put_append(&args).wait() {
                if !reply.wrong_leader {
                    // 说明在消息发送到服务端时该节点还是 leader
                    if reply.err.is_empty() {
                        // 在完成 commit 期间始终保持 leader 状态
                        self.ex_leader.store(idx, Ordering::SeqCst);
                        return;
                    }
                    // 发生 leader 转移
                }
            }

            idx = (idx + 1) % (group_num as u64);
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
