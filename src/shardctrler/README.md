## 分片控制器
    在 shardctrler/server.go和client.go中实现分片控制器。

##  Join 
    添加新的副本 组。它的参数是一组从唯一的非零副本组标识符 (GID) 到服务器名称列表的映射。shardctrler 应该通过创建一个包含新副本组的新配置来做出反应。新配置应在所有组中尽可能均匀地分配分片，并应移动尽可能少的分片以实现该目标。如果 GID 不是当前配置的一部分，则 shardctrler 应该允许重新使用它（即，应该允许 GID 加入，然后离开，然后再次加入）。

## Leave 
    参数是以前加入的组的 GID 列表。shardctrler 应该创建一个不包括这些组的新配置，并将这些组的分片分配给剩余的组。新配置应在组之间尽可能均匀地划分分片，并应移动尽可能少的分片以实现该目标。

## Move 
    参数是一个分片号和一个 GID。shardctrler 应该创建一个新配置，其中将分片分配给组。Move的目的 是让我们能够测试您的软件。移动之后 的加入或离开可能会取消移动，因为 加入和离开重新平衡。

## Query 
    参数是一个配置号 。shardctrler 回复具有该编号的配置。如果该数字为 -1 或大于已知的最大配置数字，则 shardctrler 应回复最新配置。Query(-1)的结果应该反映 shardctrler 在收到Query(-1) RPC 之前完成处理的每个Join、Leave或Move RPC。

## 均匀分配不采用排序方式
    排序方式可能会导致不同节点上的信息不同步，因为同样是0，0，0，0 ，则之间可能为乱序

## 效率
    超时时间限制了效率