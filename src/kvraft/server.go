package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"

	//"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const TIMEOUT = 1000 * 1000

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
	Ser_index int64
	Operate   string
	Key       string
	Value     string
}

type COMD struct {
	index int
	O     Op
}

type KVServer struct {
	mu      sync.Mutex
	cond    sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	putAdd  chan COMD
	get     chan COMD
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	KVS map[string]string
	CSM map[int64]int64
	CDM map[int64]int64

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

	in1, okk1 := kv.CDM[args.CIndex]
	if okk1 && in1 == args.OIndex {
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
	} else if !okk1 {
		kv.CDM[args.CIndex] = 0
	}
	kv.mu.Unlock()
	var isLeader bool
	var index int
	O := Op{
		Ser_index: int64(kv.me),
		Cli_index: args.CIndex,
		Cmd_index: args.OIndex,
		Operate:   "Get",
		Key:       args.Key,
	}
	in2, okk2 := kv.CSM[args.CIndex]
	if !okk2 {
		kv.CSM[args.CIndex] = 0
	} else if in2 == args.OIndex {
		_, isLeader = kv.rf.GetState()
		index = -args.Test
	} else {
		DEBUG(dLeader, "S%d start Get key(%v)\n", kv.me, args.Key)
		index, _, isLeader = kv.rf.Start(O)
		reply.Index = index
	}

	if !isLeader {
		reply.Err = ErrWrongLeader
		DEBUG(dLeader, "S%d <-- C%v Get key(%v) but not leader test%v\n", kv.me, args.CIndex, args.Key, args.Test)
	} else {

		OS := kv.rf.Find(index)
		if OS == nil {
			DEBUG(dLeader, "S%d do not have this log(%v)\n", kv.me, O)
		}else{
			P := OS.(Op)
			DEBUG(dLeader, "S%d have this log(%v) in raft\n", kv.me, P)
		}
		
		kv.mu.Lock()
		lastindex, ok := kv.CSM[args.CIndex]
		if !ok {
			kv.CSM[args.CIndex] = 0
		}
		kv.CSM[args.CIndex] = args.OIndex
		kv.mu.Unlock()
		DEBUG(dLeader, "S%d <-- C%v Get key(%v) wait %v\n", kv.me, args.CIndex, args.Key, args.Test)
		for {
			select {
			case out := <-kv.get:
				if index <= out.index && out.O == O {
					DEBUG(dLeader, "S%d kvs(%v) index(%v) from(%v)\n", kv.me, kv.KVS, index, kv.me)
					kv.mu.Lock()
					val, ok := kv.KVS[args.Key]
					kv.mu.Unlock()
					if ok {
						DEBUG(dLeader, "S%d Get key(%v) value(%v) OK from(C%v)\n", kv.me, args.Key, val, args.CIndex)
						reply.Err = OK
						reply.Value = val
						return
					} else {
						DEBUG(dLeader, "S%d Get key(%v) value(%v) this map do not have value map %v from(C%v)\n", kv.me, args.Key, val, kv.KVS, args.CIndex)
						reply.Err = ErrNoKey
						return
					}
					// }
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				DEBUG(dLeader, "S%d is time out\n", kv.me)
				reply.Err = TOUT
				if _, isLeader := kv.rf.GetState(); !isLeader {
					reply.Err = ErrWrongLeader
					reply.Index = index
					kv.CSM[args.CIndex] = lastindex
				}
				return

			}
		}

	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DEBUG(dLeader, "S%d <-- C%v %v key(%v) value(%v) test%v\n", kv.me, args.CIndex, args.Op, args.Key, args.Value, args.Test)
	kv.mu.Lock()

	in1, okk1 := kv.CDM[args.CIndex]
	if okk1 && in1 == args.OIndex {
		reply.Err = OK //had done
		DEBUG(dLeader, "S%d had done key(%v) value(%v) Op(%v) from(C%d) OIndex(%d) test%v\n", kv.me, args.Key, args.Value, args.Op, args.CIndex, args.OIndex, args.Test)
		reply.Index = -1
		kv.mu.Unlock()
		return
	} else if !okk1 {
		kv.CDM[args.CIndex] = 0
	}
	kv.mu.Unlock()

	var isLeader bool
	var index int
	O := Op{
		Ser_index: int64(kv.me),
		Cli_index: args.CIndex,
		Cmd_index: args.OIndex,
		Operate:   args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}
	in2, okk2 := kv.CSM[args.CIndex]
	if !okk2 {
		kv.CSM[args.CIndex] = 0
	} else if in2 == args.OIndex {
		_, isLeader = kv.rf.GetState()
		index = args.Test
	} else {
		index, _, isLeader = kv.rf.Start(O)
		reply.Index = index
	}

	if !isLeader {
		DEBUG(dLeader, "S%d <-- C%v %v key(%v) value(%v) but not leader test%v\n", kv.me, args.CIndex, args.Op, args.Key, args.Value, args.Test)
		reply.Err = ErrWrongLeader
	} else {
		OS := kv.rf.Find(index)
		if OS == nil {
			DEBUG(dLeader, "S%d do not have this log(%v)\n", kv.me, O)
		}else{
			P := OS.(Op)
			DEBUG(dLeader, "S%d have this log(%v) in raft\n", kv.me, P)
		}

		kv.mu.Lock()
		lastindex, ok := kv.CSM[args.CIndex]
		if !ok {
			kv.CSM[args.CIndex] = 0
		}
		kv.CSM[args.CIndex] = args.OIndex
		kv.mu.Unlock()

		DEBUG(dLeader, "S%d <-- C%v %v key(%v) value(%v) index(%v) wait test%v\n", kv.me, args.CIndex, args.Op, args.Key, args.Value, index, args.Test)
		for {
			select {
			case out := <-kv.putAdd:
				if index <= out.index && out.O == O {
					DEBUG(dLeader, "S%d %v index(%v) applyindex(%v)  this cmd_index(%v) key(%v) value(%v) from(C%v)\n", kv.me, kv.Apl_cmd.Operate, index, out.index, args.OIndex, args.Key, args.Value, args.CIndex)
					reply.Err = OK
					return
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				DEBUG(dLeader, "S%d time out\n", kv.me)
				reply.Err = TOUT
				if _, isLeader := kv.rf.GetState(); !isLeader {
					reply.Err = ErrWrongLeader
					reply.Index = index
					kv.CSM[args.CIndex] = lastindex
				}
				return
			}
		}
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
	kv.CSM = make(map[int64]int64)
	kv.CDM = make(map[int64]int64)
	kv.mu = sync.Mutex{}
	kv.cond = *sync.NewCond(&kv.mu)
	kv.aplplyindex = 0
	kv.cmd_done_index = 0
	kv.cmd_nums_count = 0
	kv.Leader = false

	LOGinit()

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.putAdd = make(chan COMD)
	kv.get = make(chan COMD)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func() {
		for m := range kv.applyCh {
			if !kv.killed() {
				kv.mu.Lock()

				O := m.Command.(Op)
				DEBUG(dLog, "S%d TTT CommandValid(%v) applyindex(%v) CommandIndex(%v) %v key(%v) value(%v) CDM[C%v](%v) M(%v) from(%v)\n", kv.me, m.CommandValid, kv.aplplyindex, m.CommandIndex, O.Operate, O.Key, O.Value, O.Cli_index, kv.CDM[O.Cli_index], O.Cmd_index, O.Ser_index)

				if m.CommandValid && kv.aplplyindex+1 == m.CommandIndex {
					if kv.CDM[O.Cli_index] < O.Cmd_index {
						kv.aplplyindex = m.CommandIndex
						kv.Apl_cmd = m.Command.(Op)
						DEBUG(dLeader, "S%d update CDM[%v] from %v to %v\n", kv.me, O.Cli_index, kv.CDM[O.Cli_index], O.Cmd_index)
						kv.CDM[O.Cli_index] = O.Cmd_index
						if O.Operate == "Append" {

							select {
							case kv.putAdd <- COMD{index: m.CommandIndex, O: O}:
								DEBUG(dLog, "S%d write putAdd in(%v)\n", kv.me, m.CommandIndex)
							default:
								DEBUG(dLog, "S%d can not write putAdd in(%v)\n", kv.me, m.CommandIndex)
							}

							val, ok := kv.KVS[O.Key]
							if ok {
								// DEBUG(dLog, "S%d BBBBBBB append Key(%v) from %v to %v from(me)\n", kv.me, O.Key, kv.KVS[O.Key], kv.KVS[O.Key]+O.Value)
								DEBUG(dLog, "S%d BBBBBBB append Key(%v) from %v to value(%v) from(%v)\n", kv.me, O.Key, kv.KVS[O.Key], O.Value, O.Ser_index)
								kv.KVS[O.Key] = val + O.Value
							} else {
								DEBUG(dLog, "S%d BBBBBBB append key(%v) from nil to %v from(%v)\n", kv.me, O.Key, O.Value, O.Ser_index)
								kv.KVS[O.Key] = O.Value
							}
						} else if O.Operate == "Put" {

							select {
							case kv.putAdd <- COMD{index: m.CommandIndex, O: O}:
								DEBUG(dLog, "S%d write putAdd in(%v)\n", kv.me, m.CommandIndex)
							default:
								DEBUG(dLog, "S%d can not write putAdd in(%v)\n", kv.me, m.CommandIndex)
							}

							_, ok := kv.KVS[O.Key]
							if ok {
								DEBUG(dLog, "S%d AAAAAAA put key(%v) from %v to %v from(%v)\n", kv.me, O.Key, kv.KVS[O.Key], O.Value, O.Ser_index)
								kv.KVS[O.Key] = O.Value
							} else {
								DEBUG(dLog, "S%d AAAAAAA put key(%v) from nil to %v from(%v)\n", kv.me, O.Key, O.Value, O.Ser_index)
								kv.KVS[O.Key] = O.Value
							}
						} else if O.Operate == "Get" {

							select {
							case kv.get <- COMD{index: m.CommandIndex, O: O}:
								DEBUG(dLog, "S%d write get in(%v)\n", kv.me, m.CommandIndex)
							default:
								DEBUG(dLog, "S%d can not write get in(%v)\n", kv.me, m.CommandIndex)
							}
						}
					}else if kv.CDM[O.Cli_index] == O.Cmd_index{
						kv.aplplyindex = m.CommandIndex
						DEBUG(dLog2, "S%d this cmd had done, the log had two %v\n", kv.me, kv.Apl_cmd == O)
					}
				}
				kv.mu.Unlock()
			}

		}
	}()

	return kv
}
