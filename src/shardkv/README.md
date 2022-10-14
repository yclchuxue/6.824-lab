## 分片键/值服务器
这个模块实现的是一个分片kv存储中的Group部分，每个Group由多台服务器组成，而每个Group负责一个或多个分片且能够将这些分片删除（不接收关于该分片的请求）和添加，在lab3实现的kv存储是基于整个key而言的，这里要将key分成固定数量的分片，在每个server中的KVS则也要根据管理的不同的分片而存储信息。所以server中存储kv采用 []KVS 的方式，将各个分片的kv分隔开。

## 接收Client的信息
由于不同Group管理不同的分片，client发生请求时需要知道该发个那个Group，但可能会发生错误既发生到错误的Group中，服务器该如何判断这种情况呢？Client发生请求的args 中添加该 key 所处的分区 id。server中存储自己所管理的分区，如果所管理的分区中无该 id 则返回 ErrWrongGroup。

## 分片迁移
在 Group 中的 server 需要实时监控 config 的变化，如果发生涉及到自己的分片迁移，则需要停止该分片上的服务，完成迁移。迁移通过 RPC 完成，如果发现 Group 中多添加了一个分片，则立即查询上一个 config向上一个负责此分片的 Group Leader Server ，获取此分片的 KVS。而发现Group 中删除了一个分区则立即停止该分区的服务（及将server 中的 manageshards[shard] 设为 false）。

分片迁移过程中，需要向其他 Group 获取分片的 KVS。但该向哪一个 Group 发送 RPC 则需要看 之前的 config 信息。但是 kv.config.num - 1 的 config 并不合适，也许该 config 中负责该分片的仍是自己，所以应该遍历之前的 config 找到该分片的负责 Group 不是自己的 Group，并向它发送 RPC 请求 KVS。

config 更新迅速时，上一个拥有该分片的 Group 还未获取到该分片的 KVS 就再次失去了对该分区的所有权。在 Server 中增加一个 KVSMAP map[shard]num，当拥有该分片后发送 RPC 请求分片 KVS， 请求到 KVS 后修改这个 KVSMAP 的 value 为拥有该分片的 config 的 num。当接收到请求分片 KVS 的 RPC 时，先检查 args.num 是否与 KVSMAP 中该分片的 num 一致，若一致则将该分片的 KVS 发送给请求端 server， 若不一致则让对方稍后再试。

若 config 更新后 leader 获得分片的 KVS 将 manageshards 更新后开始对该分区服务，但跟随者们并未获取到 KVS， 则这些 server 中的 KVS 就出现了不一致的现象，但从日志中是无法发现的。当其他 server 成为 Leader 后则会导致提供错误的 kv 服务。解决方法是当 leader 获取到新加入分片的 KVS 时，将这些信息写入日志，从管道读出后再处理（例如，将 KVS 写入日志，从管道读出时再写入 []KVS 并修改 manageshards 和 KVSMAP），使用这种方式后只需要 Leader 发生获取 KVS 的请求，跟随者则通过领导者发生，无需太多跨 Group 的 RPC。

shard 从 G1 转移到 G2 再转移到 G1 ，但 G1 未察觉 shard 被 G2 管理过，不会向 G2 请求该 shard 的 KVS。

G1 在 num1 时管理 shard1，被重启后 manageshards 和 KVSMAP 清空， config 更新到 num2， G1 会向 num1 时的自己请求数据，

不同的 Group 之间获取 kvs 时直接使用 rpc 获取到 kvs，但这种操作会破坏掉线性化，如果刚刚接收一个快照，快照后的日志并未来得及处理就收到其他 Group 的请求，这时候获取到的 kvs 是错误的，使用要将 Group 之间的操作也写入到日志中，这样的化就保证了线性化。

迁移后若操作已完成，但client不知道，会重新发生请求到另一个server，会导致重复执行，

KVSMAP【8】 = 4， 若 num = 5 时 shard 8 的 kvs 而config更新，num = 6， 当num = 6 的rpc的num减到小于4时，num=5的rpc成功则会导致一些错误。

## 数据恢复
当 server 恢复数据时，通过快照和日志将 KVS 恢复，如果不保存 config 等信息，则会导致需要重新获取 config 而 KVSMAP 中缺少对每个 shard 的 num 的保存。 导致需要向其他 group 获取分片的 KVS 失败，一直到 num 为 0。虽然这样似乎也可以，但在数据恢复时会浪费很多时间。

## 快照
当 server 的 config 、KVSMAP 、manageshards 更新后， 如果这些不存入快照，则导致 manageshards 等数据会超前 KVS 等数据，会导致一些错误。 所以需要将这些信息也存入快照中。同时恢复时可以大幅度减少获取 config 等信息的时间。

订正上条，首先在 shardkv 中去掉了 manageshards[shard] （是否开启该分片的服务）的设定，通过其他已有数据的组合替代，减少结构体大小，同时修正将 config 加入快照的方式，若 config 到 8 且发起 
SendGetKvs 操作并返回，会检查 kv.config 和 The_num（发起请求时的 kv.config.num）, 检查这条是否有必要写入（若不相等，即 config 更新， kv.config > The_Num， 但我的判断条件为 ！= 而不是 > ）则不需要写入。但 config 为 7 时快照，config 8 发起请求，收到快照后修改 config 到 7 ，读取快照后则导致 kv.config.num != The_Num 发生，则导致此条 kvs 不被写入，当 config 再次更新到 8 时则 kv.KVSGET[num](表示 config.num 是否已发送过 SendGetKvs) 表示已经发送过 SendGetKvs 请求, 则不会再次发送，导致超过最大测试时间。

## 管道读取
按道理管会根据谁先调用读，谁后调用读来排队读取数据，但由于start的read 管道之间的时间内会发送误差，导致start和read的顺序打乱。