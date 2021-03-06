## 实现分布式KV存储
    利用lab2中的raft算法来实现分布式的kv存储，在lab3中我们实现基于lab2中raft的kvraft来完成这一操作

## 思考

### 保证执行操作是线性执行

    put、get、append操作都会连接到leader执行，单服务器保证了操作的线性执行，get操作虽然未进行对数据库的修改，但为了保证线性执行我们将它也提交日志。

### 每个Client的操作不可重复执行

    由于超时等原因，put、get、append等操作常常会失败，但这些操作却已在server中执行，数据已经修改，我们需要对每个操作都有一个唯一标识，保证操作只执行一次。这里对每个客户端维护一个Cli_index和Cmd_index，它们分别是Client的唯一Index和每个客户端中递增的命令条数Index。在Leader，server中维护了一个CCM的map用于保存每个Client中执行的操作的最新的Index。

### Server收到操作后start，却成为了follower

    通过raft中的getstate方法来判断Server自己是否是Leader，如果不是则返回ErrWrongLeader。而Leader的操作都会按顺序从applych中读取到（即使快照后也会将操作重新写入applych中）。只有Leader——Server会收到并start来自客户端的操作请求，若Server在返回前由leader变为follower则操作失败。（由于只有leader才能处理请求则在server中也应只有leader可以唤醒其他线程--------不确定------若不唤醒则可能导致请求一直阻塞）。由于需要设置超时机制，我修改了唤醒操作将使用管道的方式来同时实现唤醒和超时。不需要在并发读写一个channel时加锁。

    可以将修改CCM的地方该到读取applych处，若在唤醒前为leader及已操作该请求，但唤醒后抢到锁后不为leader则返回ErrWrongLeader，当再次请求该操作就会have done this cmd。

### 超时机制
    当leader——server收到请求后不再是leader，且在apply日志前不再是leader，则RPC将等待，不返回，我们需要设置一个超时机制来实现返回。通过select 读取管道判断操作是否完成和超时，替代原来的唤醒机制。采用putadd和get管道代替唤醒操作后会出现读写管道阻塞的情况，暂时尝试使用select实现无阻塞读写操作。收到请求后超时，但操作已经start则再次收到请求会导致多次start及多次执行同一操作 ? 

    收到请求后start，一个跟随者添加log，该跟随者成为leader，raft只能提交自己当前任期的日志，若无新日志来，则无法提交，（对server采用每隔一段时间若无信息从管道读出，则start 空 CMD）。

    [-] 收到请求后start，随后变为follower，其他节点成为leader，此条日志被删除，该节点又成为leader后，出现do not have this cmd。

    [-] 节点恢复时少applied一条日志，当leader提交（applied）后节点被重新启动（其他节点未applied），依靠日志恢复时少会发一条。

## 快照
    使用快照可以提高kvserver的恢复效率

### 基本设计
    每隔一段时间检查一次raftstatesize，将raftstatesize通过管道Snap写入，读出后判断条件是否符合发送快照。

### 节点恢复
    当节点被killed时，节点中长期运行协程也需要被killed，否则当节点重启（恢复）后会影响到。

### 快照抢不到锁
    重启后恢复快照和日志，日志仍有较长，频繁写入管道applych，导致快照抢不到锁，raft的长度（不含日志）会逐渐变大，可能会超过最大限度8*max。这里对每apply固定条数cmd则检查一次是否应该发送快照，原来采用过定时睡眠，来让CheckSnap有时间抢锁，但效果不明显，抢锁仍会发生，不如线性执行CheckSnap，减少锁的竞争。

### 快照后的日志（raft bug）
    在raft的实现中我们遍历mathindex来判断超过半数的index数，来更新commitindex，这里的mathindex使用的不是整体的logindex，而是小于len(log)，大于等于0的值，当接收快照后应该更新mathindex，之前未更新mathindex，导致并未被大多数提交的日志被提交。需要在每次接收日志后将mathindex修改。

### 持久化不及时（raft bug）
    在raft中我们采用以下方式来持久化 raftsate 和 snapshot ，但这种方式可能会持久化不及时，计划不使用协程来做持久化(不于采用)。

### mathindex 更新问题
    如果采用 0 <= mathindex <= len(rf.log) -1 ，mathindex 在Snapshot到来时会更新,但如果是在发生心跳前为更新，发生心跳后更新mathindex，但心跳RPC返回后会造成mathindex更改，不符合实际要求。最终将mathindex改为rf.log[x].logindex。