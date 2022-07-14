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
	Operate string
	Key     string
	Value   string
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
	cmd_done_count int

	Leader bool

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	kv.mu.Lock()
	in, okk := kv.CCM[args.CIndex]
	if okk && in == args.OIndex {
		reply.Err = OK //had done
		DEBUG(dLeader, "S%d had done key(%v) from(C%v) Oindex(%v)\n", kv.me, args.Key, args.CIndex, args.OIndex)
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
		Operate: "Get",
		Key:     args.Key,
	}
	// var start time.Time
	// start = time.Now()
	index, _, isLeader := kv.rf.Start(O)
	reply.Index = index
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		kv.Leader = false
		kv.mu.Unlock()
	} else {
		kv.mu.Lock()
		kv.Leader = true
		for index > kv.aplplyindex {
			kv.cond.Wait()
			DEBUG(dLeader, "S%d get index(%v) applyindex(%v) in Get the cmd_index(%v) from(C%v)\n", kv.me, index, kv.aplplyindex, args.OIndex, args.CIndex)
		}

		kv.CCM[args.CIndex] = args.OIndex
		DEBUG(dLeader, "S%d kvs(%v) index(%v) from(%v)\n", kv.me, kv.KVS, index, kv.me)
		// ti := time.Since(start).Milliseconds()
		// DEBUG("time to raft = ", ti)
		val, ok := kv.KVS[O.Key]
		if ok {
			DEBUG(dLeader, "S%d %v key(%v) value(%v) OK from(C%v)\n", kv.me, O.Operate, O.Key, O.Value, args.CIndex)
			reply.Err = OK
			reply.Value = val
		} else {
			DEBUG(dLeader, "S%d %v key(%v) value(%v) this map do not have value map %v from(C%v)\n", kv.me, O.Operate, O.Key, O.Value, kv.KVS, args.CIndex)
			reply.Err = ErrNoKey
		}

		kv.mu.Unlock()

	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	kv.mu.Lock()
	in, okk := kv.CCM[args.CIndex]
	if okk && in == args.OIndex {
		reply.Err = OK //had done
		DEBUG(dLeader, "S%d had done key(%v) value(%v) Op(%v) from(C%d) OIndex(%d)\n", kv.me, args.Key, args.Value, args.Op, args.CIndex, args.OIndex)
		reply.Index = -1
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	O := Op{
		Operate: args.Op,
		Key:     args.Key,
		Value:   args.Value,
	}
	// var start time.Time
	// start = time.Now()

	index, _, isLeader := kv.rf.Start(O)
	reply.Index = index
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		kv.Leader = false
		kv.mu.Unlock()
		//DEBUG("this server not leader")
	} else {
		kv.mu.Lock()
		kv.Leader = true
		for index > kv.aplplyindex {
			kv.cond.Wait()
			DEBUG(dLeader, "S%d putappend index(%v) applyindex(%v) Op(%v) this cmd_index(%v) from(C%v)\n", kv.me, index, kv.aplplyindex, args.Op, args.OIndex, args.CIndex)
		}

		// ti := time.Since(start).Milliseconds()
		// DEBUG("time to raft = ", ti)
		if O.Operate == "Append" {
			kv.CCM[args.CIndex] = args.OIndex
			val, ok := kv.KVS[O.Key]
			if ok {
				reply.Err = OK
				DEBUG(dLeader, "S%d BBBBBBB append Key(%v) from %v to %v from(C%v)\n", kv.me, O.Key, kv.KVS[O.Key], kv.KVS[O.Key]+val, args.CIndex)
				kv.KVS[O.Key] = val + O.Value
			} else {
				reply.Err = OK
				DEBUG(dLeader, "S%d BBBBBBB append key(%v) from nil to %v from(C%v)\n", kv.me, O.Key, O.Value, args.CIndex)
				kv.KVS[O.Key] = O.Value
			}
		} else {
			kv.CCM[args.CIndex] = args.OIndex
			_, ok := kv.KVS[O.Key]
			if ok {
				reply.Err = OK
				DEBUG(dLeader, "S%d AAAAAAA put key(%v) from %v to %v from(C%v)\n", kv.me, O.Key, kv.KVS[O.Key], O.Value, args.CIndex)
				kv.KVS[O.Key] = O.Value
			} else {
				reply.Err = OK
				DEBUG(dLeader, "S%d AAAAAAA put key(%v) from nil to %v from(C%v)\n", kv.me, O.Key, O.Value, args.CIndex)
				kv.KVS[O.Key] = O.Value
			}
		}

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
	kv.cmd_done_count = 0
	kv.cmd_nums_count = 0
	kv.Leader = false

	LOGinit()

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for m := range kv.applyCh {
			kv.mu.Lock()
			DEBUG(dLog, "S%d CommandValid(%v) applyindex(%v) CommandIndex(%v)\n", kv.me, m.CommandValid, kv.aplplyindex, m.CommandIndex)
			if m.CommandValid && kv.aplplyindex < m.CommandIndex {
				kv.aplplyindex = m.CommandIndex
				kv.cond.Broadcast()

				if !kv.Leader {
					O := m.Command.(Op)
					if O.Operate == "Append" {
						val, ok := kv.KVS[O.Key]
						if ok {
							DEBUG(dLog, "S%d BBBBBBB append Key(%v) from %v to %v from(me)\n", kv.me, O.Key, kv.KVS[O.Key], kv.KVS[O.Key]+val)
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
			}
			kv.mu.Unlock()
		}
	}()

	// You may need initialization code here.

	return kv
}
