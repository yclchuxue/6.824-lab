package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"

	//"fmt"
	"log"
	"sync"
	"sync/atomic"
	//"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cli_index int64
	Cmd_index int64
	Operate   string
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	cond    sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	KVS map[string]string
	CCM map[int64]int64

	aplplyindex    int
	cmd_nums_count int
	cmd_done_index int

	// Cli_cmd	Op
	Apl_cmd Op

	Leader bool

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DEBUG(dLeader, "S%d <-- C%v Get key(%v) test%v\n", kv.me, args.CIndex, args.Key, args.Test)
	kv.mu.Lock()

	if kv.killed() {
		kv.mu.Unlock()
		return
	}

	in, okk := kv.CCM[args.CIndex]
	if okk && in == args.OIndex {
		reply.Err = OK //had done
		DEBUG(dLeader, "S%d had done key(%v) from(C%v) Oindex(%v) test%v\n", kv.me, args.Key, args.CIndex, args.OIndex, args.Test)
		val, ok := kv.KVS[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	O := Op{
		Cli_index: args.CIndex,
		Cmd_index: args.OIndex,
		Operate:   "Get",
		Key:       args.Key,
	}
	// var start time.Time
	// start = time.Now()
	index, _, isLeader := kv.rf.Start(O)
	reply.Index = index
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		DEBUG(dLeader, "S%d <-- C%v Get key(%v) but not leader test%v\n", kv.me, args.CIndex, args.Key, args.Test)
		kv.Leader = false
		kv.mu.Unlock()
	} else {
		kv.mu.Lock()
		kv.Leader = true
		// kv.Cli_cmd = O
		for index > kv.aplplyindex {
			DEBUG(dLeader, "S%d <-- C%v Get key(%v) wait %v\n", kv.me, args.CIndex, args.Key, args.Test)
			// go kv.timeout()
			kv.cond.Wait()
			if _, isLdeader := kv.rf.GetState(); !isLdeader {
				DEBUG(dLeader, "S%d is not leader\n", kv.me)
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			DEBUG(dLeader, "S%d Get index(%v) applyindex(%v) in Op(%v) the cmd_index(%v) from(C%v)\n", kv.me, index, kv.aplplyindex, kv.Apl_cmd.Operate, args.OIndex, args.CIndex)
		}

		DEBUG(dLeader, "S%d kvs(%v) index(%v) from(%v)\n", kv.me, kv.KVS, index, kv.me)
		// ti := time.Since(start).Milliseconds()
		// DEBUG("time to raft = ", ti)
		val, ok := kv.KVS[O.Key]
		if ok {
			DEBUG(dLeader, "S%d %v key(%v) value(%v) OK from(C%v)\n", kv.me, O.Operate, O.Key, O.Value, args.CIndex)
			reply.Err = OK
			reply.Value = val
			if kv.cmd_done_index < index {
				kv.cmd_done_index = index
			}
		} else {
			DEBUG(dLeader, "S%d %v key(%v) value(%v) this map do not have value map %v from(C%v)\n", kv.me, O.Operate, O.Key, O.Value, kv.KVS, args.CIndex)
			reply.Err = ErrNoKey
		}

		kv.mu.Unlock()

	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DEBUG(dLeader, "S%d <-- C%v %v key(%v) value(%v) test%v\n", kv.me, args.CIndex, args.Op, args.Key, args.Value, args.Test)
	kv.mu.Lock()

	if kv.killed() {
		kv.mu.Unlock()
		return
	}

	in, okk := kv.CCM[args.CIndex]
	if okk && in == args.OIndex {
		reply.Err = OK //had done
		DEBUG(dLeader, "S%d had done key(%v) value(%v) Op(%v) from(C%d) OIndex(%d) test%v\n", kv.me, args.Key, args.Value, args.Op, args.CIndex, args.OIndex, args.Test)
		reply.Index = -1
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	O := Op{
		Cli_index: args.CIndex,
		Cmd_index: args.OIndex,
		Operate:   args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}
	// var start time.Time
	// start = time.Now()

	index, _, isLeader := kv.rf.Start(O)
	reply.Index = index
	if !isLeader {
		DEBUG(dLeader, "S%d <-- C%v %v key(%v) value(%v) but not leader test%v\n", kv.me, args.CIndex, args.Op, args.Key, args.Value, args.Test)
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		kv.Leader = false
		kv.mu.Unlock()
		//DEBUG("this server not leader")
	} else {
		kv.mu.Lock()
		//kv.Cli_cmd = O
		kv.Leader = true
		for index > kv.aplplyindex {
			DEBUG(dLeader, "S%d <-- C%v %v key(%v) value(%v) index(%v) wait test%v\n", kv.me, args.CIndex, args.Op, args.Key, args.Value, index, args.Test)
			// go kv.timeout()
			kv.cond.Wait()
			DEBUG(dLeader, "S%d kv.Apl_cmd.Operate(%v) from(%v) index(%v) applyindex(%v)\n", kv.me, kv.Apl_cmd.Operate, args.CIndex, index, kv.aplplyindex)
			if _, isLdeader := kv.rf.GetState(); !isLdeader {
				DEBUG(dLeader, "S%d is not leader\n", kv.me)
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			DEBUG(dLeader, "S%d %v index(%v) applyindex(%v)  this cmd_index(%v) from(C%v)\n", kv.me, kv.Apl_cmd.Operate, index, kv.aplplyindex, args.OIndex, args.CIndex)
		}

		reply.Err = OK

		// DEBUG(O.Operate, O.Key, O.Value)
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.KVS = make(map[string]string)
	kv.CCM = make(map[int64]int64)
	kv.mu = sync.Mutex{}
	kv.cond = *sync.NewCond(&kv.mu)
	kv.aplplyindex = 0
	kv.cmd_done_index = 0
	kv.cmd_nums_count = 0
	kv.Leader = false

	LOGinit()

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for m := range kv.applyCh {
			if !kv.killed() {
				kv.mu.Lock()

				O := m.Command.(Op)
				DEBUG(dLog, "S%d TTT CommandValid(%v) applyindex(%v) CommandIndex(%v) %v key(%v) value(%v)\n", kv.me, m.CommandValid, kv.aplplyindex, m.CommandIndex, O.Operate, O.Key, O.Value)

				if m.CommandValid && kv.aplplyindex+1 == m.CommandIndex {
					kv.aplplyindex = m.CommandIndex
					kv.Apl_cmd = m.Command.(Op)
					DEBUG(dLeader, "S%d update CCM[%v] from %v to %v\n", kv.me, O.Cli_index, kv.CCM[O.Cli_index], O.Cmd_index)
					kv.CCM[O.Cli_index] = O.Cmd_index
					kv.cond.Broadcast()
					if O.Operate == "Append" {
						val, ok := kv.KVS[O.Key]
						if ok {
							DEBUG(dLog, "S%d BBBBBBB append Key(%v) from %v to %v from(me)\n", kv.me, O.Key, kv.KVS[O.Key], kv.KVS[O.Key]+O.Value)
							kv.KVS[O.Key] = val + O.Value
						} else {
							DEBUG(dLog, "S%d BBBBBBB append key(%v) from nil to %v from(me)\n", kv.me, O.Key, O.Value)
							kv.KVS[O.Key] = O.Value
						}
					} else if O.Operate == "Put" {
						_, ok := kv.KVS[O.Key]
						if ok {
							DEBUG(dLog, "S%d AAAAAAA put key(%v) from %v to %v from(me)\n", kv.me, O.Key, kv.KVS[O.Key], O.Value)
							kv.KVS[O.Key] = O.Value
						} else {
							DEBUG(dLog, "S%d AAAAAAA put key(%v) from nil to %v from(me)\n", kv.me, O.Key, O.Value)
							kv.KVS[O.Key] = O.Value
						}
					}
				}
				kv.mu.Unlock()
			}

		}
	}()

	// go func() {
	// 	for {
	// 		_, isLeader := kv.rf.GetState()
	// 		if !isLeader {
	// 			kv.mu.Lock()
	// 			// DEBUG(dLog, "S%d this server not leader\n", kv.me)
	// 			kv.cond.Broadcast()
	// 			kv.mu.Unlock()
	// 		}
	// 		time.Sleep(time.Microsecond * 50)

	// 	}
	// }()

	// You may need initialization code here.

	return kv
}

// func (kv *KVServer) timeout() {
// 	time.Sleep(time.Microsecond * 25)
// 	_, isLeader := kv.rf.GetState()
// 	if !isLeader {
// 		kv.mu.Lock()
// 		// DEBUG(dLog, "S%d this server not leader\n", kv.me)
// 		kv.cond.Broadcast()
// 		kv.mu.Unlock()
// 	}
// }
