# MIT 6.824 Distributed Systems Labs

### (Updated to Spring 2022 Course Labs)

Course website: https://pdos.csail.mit.edu/6.824/

- [x] Lab 1: MapReduce

- [x] Lab 2: Raft Consensus Algorithm
  - [x] Lab 2A: Raft Leader Election
  - [x] Lab 2B: Raft Log Entries Append
  - [x] Lab 2C: Raft state persistence
  
- [x] Lab 3: Fault-tolerant Key/Value Service
  - [x] Lab 3A: Key/value Service Without Log Compaction
  - [x] Lab 3B: Key/value Service With Log Compaction

- [x] Lab 4: Sharded Key/Value Service

### Lab 1: MapReduce
在Lab1部分，构建了一个 MapReduce 系统。实现了一个调用应用程序 Map 和 Reduce 函数并处理读写文件的工作进程，以及一个将任务分发给 workers 并处理失败的 workers 的协调程序进程。构建了类似于 MapReduce 论文的内容。

### Lab 2: Raft Consensus Algorithm
在Lab2中，实现了基于raft算法的容错键/值存储系统。 Raft算法，一种复制的状态机协议，不同于Paxos算法直接从分布式一致性问题出发推导出来，Raft算法则是从多副本状态机的角度提出，用于管理多副本状态机的日志复制。Raft实现了和Paxos相同的功能，它将一致性分解为多个子问题：Leader选举（Leader election）、日志同步（Log replication）、安全性（Safety）、日志压缩（Log compaction）、成员变更（Membership change）等。同时，Raft算法使用了更强的假设来减少了需要考虑的状态，使之变的易于理解和实现。

复制服务通过在多个副本服务器上存储其状态（即数据）的完整副本来实现容错。复制允许服务继续运行，即使它的某些服务器遇到故障（崩溃或损坏或不稳定的网络）。挑战在于故障可能会导致副本持有不同的数据副本。

Raft 将客户端请求组织成一个序列，称为日志，并确保所有副本服务器看到相同的日志。每个副本按日志顺序执行客户端请求，将它们应用到服务状态的本地副本。由于所有活动副本看到相同的日志内容，它们都以相同的顺序执行相同的请求，因此继续具有相同的服务状态。如果服务器出现故障但后来恢复了，Raft 会负责更新其日志。只要至少大多数服务器还活着并且可以相互通信，Raft 就会继续运行。如果没有这样的多数，Raft 将不会取得进展，但一旦多数可以再次通信，就会从中断的地方继续。

### Lab 3: Fault-tolerant Key/Value Service

Lab2中实现的raft算法只具有将数据复制到raft集群中，不具备抽象的k/v服务，在Lab3中实现了基于raft算法的k/v服务。客户端可以使用 Get, Put, Append三个api来使用k/v服务。这里读写是保持线性化的，如果一个客户端从服务获得更新请求的成功响应，则保证随后从其他客户端启动的读取可以看到该更新的效果。

* Put(key, value) 替换数据库中特定键的值

* Append(key, arg) 将 arg 附加到键的值

* Get(key) 获取键的当前值，若该key不存在则返回null

### Lab 4: Sharded Key/Value Service

在Lab4中，仍然是一个键/值存储系统，使用方法和Lab3一致，为了使我们的系统具有更高的性能，在Lab4中实现了分片扩展机制，该系统对一组副本组上的键进行“分片”或分区。分片是键/值对的子集；例如，所有以“a”开头的键可能是一个分片，所有以“b”开头的键都是另一个分片，等等。分片的原因是性能。每个副本组仅处理几个分片的放置和获取，并且这些组并行操作；因此，总系统吞吐量（每单位时间的输入和获取）与组数成比例增加。

分片键/值存储将有两个主要组件。首先，一组副本组。每个副本组负责分片的一个子集。副本由少数服务器组成，这些服务器使用 Raft 来复制组的分片。第二个组件是“分片控制器”。分片控制器决定哪个副本组应该为每个分片服务；此信息称为配置。配置随时间变化。客户端咨询分片控制器以找到密钥的副本组，而副本组咨询控制器以找出要服务的分片。整个系统只有一个分片控制器，使用 Raft 作为容错服务实现。

分片存储系统必须能够在副本组之间转移分片。一个原因是一些组可能比其他组负载更多，因此需要移动分片来平衡负载。另一个原因是副本组可能会加入和离开系统：可能会添加新的副本组以增加容量，或者现有的副本组可能会因维修或退役而宕机。

