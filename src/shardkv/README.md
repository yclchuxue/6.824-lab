## 分片键/值服务器
    这个模块实现的是一个分片kv存储中的Group部分，每个Group由多台服务器组成，而每个Group负责一个或多个分片且能够将这些分片删除（不接收关于该分片的请求）和添加，在lab3实现的kv存储是基于整个key而言的，这里要将key分成固定数量的分片，在每个server中的KVS则也要根据管理的不同的分片而存储信息。所以server中存储kv采用 []KVS 的方式，将各个分片的kv分隔开。

## 接收Client的信息
    由于不同Group管理不同的分片，client发生请求时需要知道该发个那个Group，但可能会发生错误既发生到错误的Group中，服务器该如何判断这种情况呢？Client发生请求的args 中添加该 key 所处的分区 id。server中存储自己所管理的分区，如果所管理的分区中无该 id 则返回 ErrWrongGroup。

## 分片迁移
    在 Group 中的 server 需要实时监控 config 的变化，如果发生涉及到自己的分片迁移，则需要停止该分片上的服务，完成迁移。迁移通过 RPC 完成，如果发现 Group 中多添加了一个分片，则立即查询上一个 config向上一个负责此分片的 Group Leader Server ，获取此分片的 KVS。而发现Group 中删除了一个分区则立即停止该分区的服务（及将server 中的 manageshards[shard] 设为 false）。