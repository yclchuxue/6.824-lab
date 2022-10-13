package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const TIMEOUT = 1000 * 1000

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cli_index int64
	Cmd_index int64
	Ser_index int64
	Gro_index int64
	Operate   string
	Shard     int
	Num       int
	KVS       map[string]string
	CSM       map[int64]int64
	CDM       map[int64]int64
	Key       string
	Value     string
}

type COMD struct {
	index   int
	csm     map[int64]int64
	cdm     map[int64]int64
	kvs     map[string]string
	num     int
	The_num int
}

type ShardKV struct {
	mu           sync.Mutex
	mu1          sync.Mutex
	mu2			 sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardctrler.Clerk
	dead         int32 // set by Kill()

	putAdd     chan COMD
	get        chan COMD
	getkvs     chan COMD
	num_change chan COMD

	// Your definitions here.
	KVS []map[string]string
	CSM []map[int64]int64
	CDM []map[int64]int64

	CON map[int]shardctrler.Config   //configs
	ChanComd map[int]COMD            //管道getkvs的消息队列


	applyindex int
	// rpcindex   int
	Now_Num int
	check bool
	config  shardctrler.Config
	KVSMAP  map[int]int
	KVSGET  map[int]int
}

type SnapShot struct {
	Kvs    []map[string]string
	Csm    []map[int64]int64
	Cdm    []map[int64]int64
	KVSMAP map[int]int
	Config shardctrler.Config
	// Rpcindex    int
	Apliedindex int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DEBUG(dLeader, "S%d G%d <-- C%v Get key(%v) args.num(%v) shard(%v)\n", kv.me, kv.gid, args.CIndex, args.Key, args.Num, args.Shard)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DEBUG(dLog, "S%d G%d this is not leader\n", kv.me, kv.gid)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	DEBUG(dLog, "S%d G%d lock 89\n", kv.me, kv.gid)
	if kv.config.Num != kv.KVSMAP[args.Shard] || kv.config.Num != args.Num || !kv.check{
		DEBUG(dLog, "S%d G%d not have this shard(%v) KVSMAP(%v) config.NUM(%v) args.Num(%v)\n", kv.me, kv.gid, args.Shard, kv.KVSMAP[args.Shard], kv.config.Num, args.Num)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		for {
			select {
			case out := <-kv.num_change:
				if out.num == args.Num {
					DEBUG(dLog, "S%d G%d num change to shard(%v) args.NUM(%v) from(%v) cmd(%v)\n", kv.me, kv.gid, args.Shard, args.Num, args.CIndex, args.OIndex)
					return
				}
			case <-time.After(200 * time.Millisecond):
				DEBUG(dLog, "S%d G%d time out to shard(%v) args.NUM(%v) from(%v) cmd(%v)\n", kv.me, kv.gid, args.Shard, args.Num, args.CIndex, args.OIndex)
				return
			}
		}
		// time.Sleep(200*time.Millisecond)
		// return
	}

	if kv.applyindex == 0 {
		DEBUG(dLog, "S%d G%d the snap not applied applyindex is %v\n", kv.me, kv.gid, kv.applyindex)
		kv.mu.Unlock()
		reply.Err = ErrTimeOut
		time.Sleep(200 * time.Millisecond)
		return
	}

	in1, okk1 := kv.CDM[args.Shard][args.CIndex]
	if okk1 && in1 == args.OIndex {
		reply.Err = OK //had done
		DEBUG(dLeader, "S%d G%d had done key(%v) from(C%v) Oindex(%v)\n", kv.me, kv.gid, args.Key, args.CIndex, args.OIndex)
		val, ok := kv.KVS[args.Shard][args.Key]
		if ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
		return
	} else if !okk1 {
		kv.CDM[args.Shard][args.CIndex] = 0
	}
	kv.mu.Unlock()

	var index int
	O := Op{
		Ser_index: int64(kv.me),
		Gro_index: int64(kv.gid),
		Cli_index: args.CIndex,
		Cmd_index: args.OIndex,
		Operate:   "Get",
		Shard:     args.Shard,
		Key:       args.Key,
	}
	kv.mu.Lock()
	DEBUG(dLog, "S%d G%d lock 145\n", kv.me, kv.gid)
	in2, okk2 := kv.CSM[args.Shard][args.CIndex]
	if !okk2 {
		kv.CSM[args.Shard][args.CIndex] = 0
	}
	kv.mu.Unlock()

	if in2 == args.OIndex {
		_, isLeader = kv.rf.GetState()
	} else {
		DEBUG(dLeader, "S%d G%d start Get key(%v)\n", kv.me, kv.gid, args.Key)
		index, _, isLeader = kv.rf.Start(O)
		// go kv.SendToSnap()
	}

	if !isLeader {
		reply.Err = ErrWrongLeader
		DEBUG(dLeader, "S%d G%d <-- C%v Get key(%v) but not leader\n", kv.me, kv.gid, args.CIndex, args.Key)
	} else {

		// OS := kv.rf.Find(index)
		// if OS == nil {
		// 	DEBUG(dLeader, "S%d do not have this log(%v)\n", kv.me, O)
		// } else {
		// 	P := OS.(Op)
		// 	DEBUG(dLeader, "S%d have this log(%v) in raft\n", kv.me, P)
		// }

		kv.mu.Lock()
		DEBUG(dLog, "S%d G%d lock 174\n", kv.me, kv.gid)
		lastindex, ok := kv.CSM[args.Shard][args.CIndex]
		if !ok {
			kv.CSM[args.Shard][args.CIndex] = 0
		}
		kv.CSM[args.Shard][args.CIndex] = args.OIndex
		kv.mu.Unlock()

		// DEBUG(dLeader, "S%d G%d <-- C%v Get key(%v) wait index(%v)\n", kv.me, kv.gid, args.CIndex, args.Key, index)
		for {
			select {
			case out := <-kv.get:
				if index == out.index {
					kv.mu.Lock()
					DEBUG(dLog, "S%d G%d lock 188\n", kv.me, kv.gid)
					// DEBUG(dLeader, "S%d G%d kvs(%v) index(%v) from(%v)\n", kv.me, kv.gid, kv.KVS, index, kv.me)
					val, ok := kv.KVS[args.Shard][args.Key]
					if ok {
						// DEBUG(dLeader, "S%d G%d Get key(%v) value(%v) OK from(C%v)\n", kv.me, kv.gid, args.Key, val, args.CIndex)
						reply.Err = OK
						reply.Value = val
						kv.mu.Unlock()
						return
					} else {
						// DEBUG(dLeader, "S%d G%d Get key(%v) value(%v) this map do not have value map %v from(C%v)\n", kv.me, kv.gid, args.Key, val, kv.KVS, args.CIndex)
						reply.Err = ErrNoKey
						kv.mu.Unlock()
						return
					}
				}else{
					DEBUG(dLog, "S%d G%d index != out.index get %d != %d\n", kv.me, kv.gid, index, out.index)
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				_, isLeader := kv.rf.GetState()
				kv.mu.Lock()
				DEBUG(dLog, "S%d G%d lock 207\n", kv.me, kv.gid)
				DEBUG(dLeader, "S%d G%d is time out\n", kv.me, kv.gid)
				reply.Err = ErrTimeOut
				if !isLeader {
					reply.Err = ErrWrongLeader
					kv.CSM[args.Shard][args.CIndex] = lastindex
				}
				kv.mu.Unlock()
				return

			}
		}

	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DEBUG(dLeader, "S%d G%d <-- C%v putappend key(%v) value(%v) args.num(%v) shard(%v)\n", kv.me, kv.gid, args.CIndex, args.Key, args.Value, args.Num, args.Shard)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DEBUG(dLog, "S%d G%d this is not leader\n", kv.me, kv.gid)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	DEBUG(dLog, "S%d G%d lock in 234\n", kv.me, kv.gid)
	if kv.config.Num != kv.KVSMAP[args.Shard] || kv.config.Num != args.Num || !kv.check{
		DEBUG(dLog, "S%d G%d not have this shard(%v) KVSMAP(%v) config.NUM(%v) args.Num(%v)\n", kv.me, kv.gid, args.Shard, kv.KVSMAP[args.Shard], kv.config.Num, args.Num)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		for {
			select {
			case out := <-kv.num_change:
				if out.num == args.Num {
					return
				}
			case <-time.After(200 * time.Millisecond):
				return
			}
		}
		// time.Sleep(200*time.Millisecond)
		// return
	}

	if kv.applyindex == 0 {
		DEBUG(dLog, "S%d G%d the snap not applied applyindex is %v\n", kv.me, kv.gid, kv.applyindex)
		kv.mu.Unlock()
		reply.Err = ErrTimeOut
		time.Sleep(200 * time.Millisecond)
		return
	}

	in1, okk1 := kv.CDM[args.Shard][args.CIndex]
	if okk1 && in1 == args.OIndex {
		reply.Err = OK //had done
		// DEBUG(dLeader, "S%d G%d had done key(%v) value(%v) Op(%v) from(C%d) OIndex(%d)\n", kv.me, kv.gid, args.Key, args.Value, args.Op, args.CIndex, args.OIndex)
		kv.mu.Unlock()
		return
	} else if !okk1 {
		kv.CDM[args.Shard][args.CIndex] = 0
	}
	kv.mu.Unlock()

	var index int
	O := Op{
		Ser_index: int64(kv.me),
		Gro_index: int64(kv.gid),
		Cli_index: args.CIndex,
		Cmd_index: args.OIndex,
		Operate:   args.Op,
		Shard:     args.Shard,
		Key:       args.Key,
		Value:     args.Value,
	}

	kv.mu.Lock()
	DEBUG(dLog, "S%d G%d lock 285\n", kv.me, kv.gid)
	in2, okk2 := kv.CSM[args.Shard][args.CIndex]
	if !okk2 {
		kv.CSM[args.Shard][args.CIndex] = 0
	}
	kv.mu.Unlock()

	if in2 == args.OIndex {
		_, isLeader = kv.rf.GetState()
	} else {
		index, _, isLeader = kv.rf.Start(O)
		// go kv.SendToSnap()
	}

	if !isLeader {
		// DEBUG(dLeader, "S%d %d <-- C%v %v key(%v) value(%v) but not leader\n", kv.me, kv.gid, args.CIndex, args.Op, args.Key, args.Value)
		reply.Err = ErrWrongLeader
	} else {
		// OS := kv.rf.Find(index)
		// if OS == nil {
		// 	DEBUG(dLeader, "S%d do not have this log(%v) the index is %v\n", kv.me, O, index)
		// } else {
		// 	P := OS.(Op)
		// 	DEBUG(dLeader, "S%d have this log(%v) in raft the index is %v\n", kv.me, P, index)
		// }

		kv.mu.Lock()
		DEBUG(dLog, "S%d G%d lock 312\n", kv.me, kv.gid)
		lastindex, ok := kv.CSM[args.Shard][args.CIndex]
		if !ok {
			kv.CSM[args.Shard][args.CIndex] = 0
		}
		kv.CSM[args.Shard][args.CIndex] = args.OIndex
		kv.mu.Unlock()

		// DEBUG(dLeader, "S%d G%d <-- C%v %v key(%v) value(%v) index(%v) wait\n", kv.me, kv.gid, args.CIndex, args.Op, args.Key, args.Value, index)
		for {
			select {
			case out := <-kv.putAdd:
				if index == out.index {
					// DEBUG(dLeader, "S%d G%d %v index(%v) applyindex(%v)  this cmd_index(%v) key(%v) value(%v) from(C%v)\n", kv.me, kv.gid, args.Op, index, out.index, args.OIndex, args.Key, args.Value, args.CIndex)
					reply.Err = OK
					return
				}else{
					DEBUG(dLog, "S%d G%d index != out.index pytappend %d != %d\n", kv.me, kv.gid, index, out.index)
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				_, isLeader := kv.rf.GetState()
				kv.mu.Lock()
				DEBUG(dLog, "S%d G%d lock 332\n", kv.me, kv.gid)
				DEBUG(dLeader, "S%d G%d time out\n", kv.me, kv.gid)
				reply.Err = ErrTimeOut
				if !isLeader {
					reply.Err = ErrWrongLeader
					kv.CSM[args.Shard][args.CIndex] = lastindex
				}
				kv.mu.Unlock()
				return
			}
		}
	}

}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	DEBUG(dLog, "S%d G%d kill\n", kv.me, kv.gid)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) SendSnapShot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	S := SnapShot{
		Kvs:    kv.KVS,
		Csm:    kv.CSM,
		Cdm:    kv.CDM,
		KVSMAP: kv.KVSMAP,
		Config: kv.config,
		// Rpcindex:    kv.rpcindex,
		Apliedindex: kv.applyindex,
	}
	e.Encode(S)
	DEBUG(dSnap, "S%d G%d the size need to snap\n", kv.me, kv.gid)
	data := w.Bytes()
	go kv.rf.Snapshot(S.Apliedindex, data)
	// X, num := kv.rf.RaftSize()
	// fmt.Println("S", kv.me, "raftsize", num, "snap.lastindex.X", X)
}

func (kv *ShardKV) CheckSnap() {
	// kv.mu.Lock()

	// DEBUG(dLog, "S%d G%d kvs is %v\n", kv.me, kv.gid, kv.KVS)
	// DEBUG(dLog, "S%d G%d config is %v\n", kv.me, kv.gid, kv.config)
	// DEBUG(dLog, "S%d G%d KVSMAP(%v)\n", kv.me, kv.gid, kv.KVSMAP)

	X, num := kv.rf.RaftSize()
	DEBUG(dSnap, "S%d G%d the size is (%v) applidindex(%v) X(%v)\n", kv.me, kv.gid, num, kv.applyindex, X)
	if num >= int(float64(kv.maxraftstate)) {
		if kv.applyindex == 0 || kv.applyindex <= X {
			// kv.mu.Unlock()
			return
		}
		kv.SendSnapShot()
	}
	// kv.mu.Unlock()
}

func (kv *ShardKV) GetConfig(args *GetConfigArgs, reply *GetConfigReply) {
	// config := kv.mck.Query(-1)

	DEBUG(dLog, "S%d G%d <--- S%d G%d get shard(%v) num(%v) kvs\n", kv.me, kv.gid, args.SIndex, args.GIndex, args.Shard, args.Num)
	oldconfig := shardctrler.Config{}

	if args.Num > 0 {
		DEBUG(dLog2, "S%d G%d shard(%v) before mu1 lock 426\n", kv.me, kv.gid, args.Shard)
		// start := time.Now()
		kv.mu1.Lock()
		DEBUG(dLog2, "S%d G%d shard(%v) mu1 lock 437\n", kv.me, kv.gid, args.Shard)
		con, ice := kv.CON[args.Num]
		kv.mu1.Unlock()
		DEBUG(dLog2, "S%d G%d shard(%v) mu1 Unlock 440\n", kv.me, kv.gid, args.Shard)
		if ice {
			oldconfig = con
		} else {
			oldconfig = kv.mck.Query(args.Num)
			go kv.AppendCON(oldconfig)
		}
		// ti := time.Since(start).Milliseconds()
		// DEBUG(dLog2, "S%d G%d shard(%v) after lock 426 ti(%v)\n", kv.me, kv.gid, args.Shard, ti)
	} else {
		oldconfig = shardctrler.Config{}
	}
	DEBUG(dLog2, "S%d G%d shard(%v) before getstatus\n", kv.me, kv.gid, args.Shard)
	_, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		DEBUG(dLog, "S%d G%d shard(%v) this server is not leader\n", kv.me, kv.gid, args.Shard)
		return
	}

	if oldconfig.Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		DEBUG(dLog, "S%d G%d shard(%v) this is a never print log config(%v)\n", kv.me, kv.gid, args.Shard, oldconfig)
		return
	}

	DEBUG(dLog2, "S%d G%d shard(%v) before lock 452 \n", kv.me, kv.gid, args.Shard)
	var start time.Time
	start = time.Now()
	DEBUG(dLog, "S%d G%d try lock 424\n", kv.me, kv.gid)
	kv.mu.Lock()
	ti := time.Since(start).Milliseconds()
	DEBUG(dLog2, "S%d G%d PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP %d\n", kv.me, kv.gid, ti)
	DEBUG(dLog, "S%d G%d success lock 424\n", kv.me, kv.gid)

	// if args.The_num == kv.config.Num {

	// }

	// if args.The_num >= kv.config.Num {
	// 	DEBUG(dLog, "S%d G%d shard(%v) args.The_num(%v) >= kv.config.Num(%v)\n", kv.me, kv.gid, args.Shard, args.The_num, kv.config.Num)
	// 	kv.mu.Unlock()
	// 	DEBUG(dLog, "S%d G%d Unlock 500\n", kv.me, kv.gid)
	// 	start = time.Now()
	// 	newconfig := kv.mck.Query(-1)
	// 	ti := time.Since(start).Milliseconds()
	// 	DEBUG(dLog2, "S%d G%d Qurey -1 time is %d\n", kv.me, kv.gid, ti)
	// 	kv.mu.Lock()
	// 	kv.config = newconfig
	// 	go kv.AppendCON(oldconfig)
	// }

	if args.The_num >= kv.config.Num {
		DEBUG(dLog, "S%d G%d shard(%v) args.The_num(%v) >= kv.config.Num(%v)\n", kv.me, kv.gid, args.Shard, args.The_num, kv.config.Num)
		kv.check = false
		go kv.CheckConfig()
	}

	if kv.KVSMAP[args.Shard] < args.Num && kv.config.Num > args.Num {
		reply.Err = ErrWrongGroup
		reply.The_num = kv.config.Num
		reply.Kvs_num = kv.KVSMAP[args.Shard]
		DEBUG(dLog, "S%d G%d the KVSMAP[%v](%v) < args.num(%v) < config.num(%v)\n", kv.me, kv.gid, args.Shard, kv.KVSMAP[args.Shard], args.Num, kv.config.Num)
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	O := Op{
		Ser_index: int64(args.SIndex),
		Gro_index: int64(args.GIndex),
		Cli_index: -3,
		Operate:   "GetKvs",
		Shard:     args.Shard,
		Num:       args.Num,
	}

	index, _, isLeader := kv.rf.Start(O)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DEBUG(dLog, "S%d G%d this server is not leader\n", kv.me, kv.gid)
		return
	}
	DEBUG(dInfo, "S%d G%d the S%d G%d get shardkvs wait shard is %v the index(%v)\n", kv.me, kv.gid, args.SIndex, args.GIndex, args.Shard, index)

	for {
		select {
		case out := <-kv.getkvs:

			DEBUG(dLeader, "S%d G%d read from get kvs me.shard(%v) index(%v) out.index(%v)\n", kv.me, kv.gid, args.Shard, index, out.index)
			if index != out.index{
				DEBUG(dLog, "S%d G%d chancomd add index(%d)\n", kv.me, kv.gid, out.index);
				kv.mu2.Lock()
				kv.ChanComd[out.index] = out;
				kv.mu2.Unlock()
				for{
					kv.mu2.Lock()
					con, ok := kv.ChanComd[index]
					kv.mu2.Unlock()
					DEBUG(dLog, "S%d G%d wait from ChanComd the index(%d)\n", kv.me, kv.gid, index);
					if ok {
						out = con
						break;
					}
					time.Sleep(50 * time.Microsecond)
				}
			}

			if index == out.index {

				if out.num >= args.Num {
					DEBUG(dLog, "S%d G%d the kvs is %v\n", kv.me, kv.gid, kv.KVS)
					DEBUG(dLog, "S%d G%d success return to S%d G%d the kvs(%v) shard is %v KVSMAP[shard](%v) args.Num(%v) index%v\n", kv.me, kv.gid, args.SIndex, args.GIndex, out.kvs, args.Shard, kv.KVSMAP[args.Shard], args.Num, index)

					reply.Kvs = out.kvs
					reply.Cdm = out.cdm
					reply.Csm = out.csm
					reply.Shard = args.Shard
					reply.Err = OK
					return
				} else if out.num < args.Num && out.The_num > args.Num {
					DEBUG(dLog, "S%d G%d the KVSMAP[%v](%v) < args.num(%v) < config.num(%v)\n", kv.me, kv.gid, args.Shard, kv.KVSMAP[args.Shard], args.Num, kv.config.Num)
					reply.Err = ErrWrongGroup
					reply.The_num = out.The_num
					reply.Kvs_num = out.num
					return
				} else {
					DEBUG(dLog, "S%d G%d this group kvs have not get over KVSMAP(%v) shard(%d) args.num(%v) index%v\n", kv.me, kv.gid, kv.KVSMAP, args.Shard, args.Num, index)
					index, _, isLeader = kv.rf.Start(O)
					if !isLeader {
						reply.Err = ErrWrongLeader
						DEBUG(dLog, "S%d G%d this server is not leader\n", kv.me, kv.gid)
						return
					}
					continue
				}
			}
		// else{
		// 	select {
		// 		case kv.getkvs <- out:
		// 			DEBUG(dLog, "S%d G%d write index in(%v) read wrong\n", kv.me, kv.gid, out.index)
		// 		default:
		// 			DEBUG(dLog, "S%d G%d can not write index in(%v) read wrong\n", kv.me, kv.gid, out.index)
		// 		}
		// }

		//超时可加入判断该请求是否过期

		case <-time.After(1000 * time.Millisecond):
			DEBUG(dLog2, "S%d G%d long time shard(%v) num(%v) from S%d G%d have not get the out\n", kv.me, kv.gid, args.Shard, args.Num, args.SIndex, args.GIndex)
			// index, _, isLeader = kv.rf.Start(O)
			// if !isLeader {
			// 	reply.Err = ErrWrongLeader
			// 	DEBUG(dLog, "S%d G%d this server is not leader\n", kv.me, kv.gid)
			// 	return
			// }
			continue
		}
	}

}

func (kv *ShardKV) StartOp(O Op) {
	for {
		kv.mu.Lock()
		DEBUG(dLog, "S%d G%d lock 583\n", kv.me, kv.gid)
		if kv.config.Num != O.Num {
			DEBUG(dLog, "S%d G%d kv.config.num(%v) != The_num(%v)\n", kv.me, kv.gid, kv.config.Num, O.Num)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		_, _, Leader := kv.rf.Start(O)
		if Leader {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) SendToGetConfig(num int, shard int) {

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DEBUG(dLog, "S%d G%d shard(%v) is not leader in SendToGetConfig\n", kv.me, kv.gid, shard)
		return
	}

	// var start time.Time
	// start = time.Now()
	// kv.mu.Lock()
	// ti := time.Since(start).Milliseconds()
	// DEBUG(dLog2, "S%d G%d HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH %d\n", kv.me, kv.gid, ti)

	args := GetConfigArgs{}
	args.Shard = shard
	args.SIndex = kv.me
	args.GIndex = kv.gid
	args.The_num = num
	// kv.mu.Unlock()

	oldconfig := shardctrler.Config{}
	The_Num := num
	num--

	kv.mu1.Lock()
	con, ice := kv.CON[num]
	kv.mu1.Unlock()
	if ice {
		oldconfig = con
	} else {
		oldconfig = kv.mck.Query(num)
		go kv.AppendCON(oldconfig)
	}

	for {
		kv.mu1.Lock()
		con, ice := kv.CON[num]
		kv.mu1.Unlock()
		if ice {
			oldconfig = con
		} else {
			oldconfig = kv.mck.Query(num)
			go kv.AppendCON(oldconfig)
		}
		// var start time.Time
		// start = time.Now()
		// ti := time.Since(start).Milliseconds()
		// DEBUG(dLog2, "S%d G%d DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD %d\n", kv.me, kv.gid, ti)

		if num == 0 {
			O := Op{
				Ser_index: int64(kv.me),
				Gro_index: int64(kv.gid),
				Cli_index: -4,
				Operate:   "OwnCon",
				Shard:     shard,
				Num:       The_Num,
			}
			_, _, Leader := kv.rf.Start(O)
			DEBUG(dInfo, "S%d G%d shard(%v) Start OWNCON old num = 0 The_num is %v %v before\n", kv.me, kv.gid, shard, The_Num, Leader)
			if !Leader {
				kv.StartOp(O)
			}
			return
		}
		// DEBUG(dLog, "S%d G%d oldconfig.Shard[%v](%v)\n", kv.me, kv.gid, shard, oldconfig.Shards[shard])
		if oldconfig.Shards[shard] == kv.gid {
			kv.mu.Lock()
			DEBUG(dLog, "S%d G%d lock 546\n", kv.me, kv.gid)
			if kv.KVSMAP[shard] == num {
				O := Op{
					Ser_index: int64(kv.me),
					Gro_index: int64(kv.gid),
					Cli_index: -4,
					Operate:   "OwnCon",
					Shard:     shard,
					Num:       The_Num,
				}
				kv.mu.Unlock()
				DEBUG(dLog, "S%d G%d Unlock 719\n", kv.me, kv.gid)
				_, _, Leader := kv.rf.Start(O)
				DEBUG(dInfo, "S%d G%d shard(%v) Start num(%v) == KVSMAP[%v] %v before\n", kv.me, kv.gid, shard, num, shard, Leader)
				if !Leader {
					kv.StartOp(O)
				}
				return
			} else if kv.KVSMAP[shard] < num {
				// if kv.KVSGET[num] == 1 {
				// 	DEBUG(dLog, "S%d G%d wait shard(%d) last num(%d)\n", kv.me, kv.gid, shard, num)
				// 	kv.mu.Unlock()
				// 	time.Sleep(10*time.Microsecond)
				// 	continue
				// }else{
				DEBUG(dLog, "S%d G%d shard(%v) The_num(%v) KVSMAP(%v) num from %v to %v\n", kv.me, kv.gid, shard, The_Num, kv.KVSMAP[shard], num, num-1)
				num--
				// }
			} // 若该节点已请求num的kvs但暂时未收到，当发出后又收到

			kv.mu.Unlock()
			DEBUG(dLog, "S%d G%d Unlock 739\n", kv.me, kv.gid)
		} else {
			break
		}
	}

	// TNM := make(map[int]int)
	KNM := make(map[int]int)

	try_num := 3
	for {

		args.Num = num
		gid := oldconfig.Shards[shard]
		// DEBUG(dLog, "S%d G%d shard(%d) num(%d) the oldconfig is %v\n", kv.me, kv.gid, shard, num, oldconfig)
		if servers, ok := oldconfig.Groups[gid]; ok {

			// DEBUG(dLog, "S%d G%d need send to get shard %v num is %d\n", kv.me, kv.gid, shard, num)

			for si := 0; si < len(servers); {

				kv.mu.Lock()
				DEBUG(dLog, "S%d G%d lock 583\n", kv.me, kv.gid)
				if kv.config.Num != The_Num {
					DEBUG(dLog, "S%d G%d shard(%v) kv.config.num(%v) != The_num(%v)\n", kv.me, kv.gid, shard, kv.config.Num, The_Num)
					kv.mu.Unlock()
					DEBUG(dLog, "S%d G%d Unlock 765\n", kv.me, kv.gid)
					return
				}

				kv.mu.Unlock()
				DEBUG(dLog, "S%d G%d Unlock 770\n", kv.me, kv.gid)

				srv := kv.make_end(servers[si])
				var reply GetConfigReply
				DEBUG(dLog, "S%d G%d send to get shard(%v) num(%v) S%v G%d The_num(%v) SSSSS\n", kv.me, kv.gid, shard, num, si, gid, The_Num)
				ok := srv.Call("ShardKV.GetConfig", &args, &reply)

				kv.mu.Lock()
				DEBUG(dLog, "S%d G%d lock 622\n", kv.me, kv.gid)
				if kv.config.Num != The_Num {
					DEBUG(dLog, "S%d G%d shard(%v) kv.config.num(%v) != The_num(%v)\n", kv.me, kv.gid, shard, kv.config.Num, The_Num)
					kv.mu.Unlock()
					DEBUG(dLog, "S%d G%d Unlock 782\n", kv.me, kv.gid)
					return
				}

				kv.mu.Unlock()
				DEBUG(dLog, "S%d G%d Unlock 787\n", kv.me, kv.gid)

				if ok {
					try_num = 3
				}
				if ok && reply.Err == OK {
					DEBUG(dLog, "S%d G%d success get shard%v kvs(%v)\n", kv.me, kv.gid, shard, reply.Kvs)
					// kvs := make(map[string]string)
					// for k, v := range reply.Kvs {
					// 	kvs[k] = v
					// }
					// csm := make(map[int64]int64)
					// for k, v := range reply.Csm {
					// 	csm[k] = v
					// }

					// cdm := make(map[int64]int64)
					// for k, v := range reply.Cdm {
					// 	cdm[k] = v
					// }
					O := Op{
						Ser_index: int64(kv.me),
						Cli_index: -2,
						Gro_index: int64(kv.gid),
						Operate:   "Config",
						KVS:       reply.Kvs,
						CDM:       reply.Cdm,
						CSM:       reply.Csm,
						Shard:     shard,
						Num:       The_Num,
					}

					_, _, Leader := kv.rf.Start(O)
					DEBUG(dInfo, "S%d G%d Start CON success get shard%v kvs %v\n", kv.me, kv.gid, shard, Leader)
					if !Leader {
						kv.StartOp(O)
					}
					return
				} else if ok && 1 == 0 { // 处理args.shard != reply.shard的情况

				} else if ok && reply.Err == ErrWrongGroup {

					// TNM[gid] = reply.The_num
					KNM[gid] = reply.Kvs_num

					DEBUG(dLog, "S%d G%d shard(%v) The_num(%v) num from %v to %v FFFFF\n", kv.me, kv.gid, shard, The_Num, num, num-1)
					num--
					for {
						// var start time.Time
						start := time.Now()

						kv.mu1.Lock()
						con, ice := kv.CON[num]
						kv.mu1.Unlock()
						if ice {
							oldconfig = con
						} else {
							oldconfig = kv.mck.Query(num)
							go kv.AppendCON(oldconfig)
						}

						ti := time.Since(start).Milliseconds()
						DEBUG(dLog2, "S%d G%d TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT %d\n", kv.me, kv.gid, ti)

						if num == 0 {
							O := Op{
								Ser_index: int64(kv.me),
								Gro_index: int64(kv.gid),
								Cli_index: -4,
								Operate:   "OwnCon",
								Shard:     shard,
								Num:       The_Num,
							}

							_, _, Leader := kv.rf.Start(O)
							DEBUG(dInfo, "S%d G%d Start shard(%v) OWNCON old num = 0 The_num is %v %v after\n", kv.me, kv.gid, shard, The_Num, Leader)
							if !Leader {
								kv.StartOp(O)
							}
							return
						}
						DEBUG(dLog, "S%d G%d oldconfig.Shard[%v](%v) num(%v)\n", kv.me, kv.gid, shard, oldconfig.Shards[shard], num)
						if oldconfig.Shards[shard] == kv.gid {
							kv.mu.Lock()
							DEBUG(dLog, "S%d G%d lock 640\n", kv.me, kv.gid)
							if kv.KVSMAP[shard] == num {
								O := Op{
									Ser_index: int64(kv.me),
									Gro_index: int64(kv.gid),
									Cli_index: -4,
									Operate:   "OwnCon",
									Shard:     shard,
									Num:       The_Num,
								}
								kv.mu.Unlock()
								DEBUG(dLog, "S%d G%d Unlock 882\n", kv.me, kv.gid)
								_, _, Leader := kv.rf.Start(O)
								DEBUG(dInfo, "S%d G%d Start shard(%v) num(%v) == KVSMAP[%v](%v) %v after\n", kv.me, kv.gid, shard, num, shard, kv.KVSMAP[shard], Leader)
								if !Leader {
									kv.StartOp(O)
								}
								return
							} else {
								DEBUG(dLog, "S%d G%d shard(%v) The_num(%v) num from %v to %v NNNN\n", kv.me, kv.gid, shard, The_Num, num, num-1)
								num--
							}
							kv.mu.Unlock()
							DEBUG(dLog, "S%d G%d Unlock 894\n", kv.me, kv.gid)
						} else {
							DEBUG(dLog2, "S%d G%d in KNM shard(%v) num(%v) gid(%v) KNM(%v)\n", kv.me, kv.gid, shard, num, gid, KNM)

							kv.mu1.Lock()
							con, ice = kv.CON[num]
							kv.mu1.Unlock()
							if ice {
								oldconfig = con
							} else {
								oldconfig = kv.mck.Query(num)
								go kv.AppendCON(oldconfig)
							}

							gid = oldconfig.Shards[shard]
							N2, okk2 := KNM[gid]
							if okk2 && num > N2 {
								num--
							} else {
								break
							}
						}

					}
					break

				} else if ok && reply.Err == ErrWrongLeader {
					DEBUG(dLog, "S%d G%d the S%v is not leader\n", kv.me, si, kv.gid)
					si++
					if si == len(servers) {
						si = 0
					}
					
				} else if !ok || reply.Err == ErrTimeOut {
					DEBUG(dLog, "S%d G%d the TIMEOUT\n", kv.me, kv.gid)
					if try_num > 0 {
						try_num--
					} else {
						si++
						// if si == len(servers) {
						// 	si = 0
						// }
					}
				}

			}
		} else {
			fmt.Println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA num is", num)
			break
		}
		time.Sleep(100 * time.Microsecond)
	}

}

func (kv *ShardKV) AppendCON(config shardctrler.Config) {
	num := config.Num
	kv.mu1.Lock()
	_, ok := kv.CON[num]
	if !ok {
		kv.CON[num] = config
	}
	kv.mu1.Unlock()

	for {
		num--
		kv.mu1.Lock()
		_, ok := kv.CON[num]
		if !ok {
			kv.mu1.Unlock()
			newconfig := kv.mck.Query(num)
			kv.mu1.Lock()
			kv.CON[num] = newconfig
		}
		kv.mu1.Unlock()
		if num < 0 {
			return
		}
	}
}

func (kv *ShardKV) getallconfigs(configs []shardctrler.Config) {
	kv.mu1.Lock()
	for i := 0; i < len(configs); i++ {
		kv.CON[configs[i].Num] = configs[i]
	}
	kv.mu1.Unlock()
}

func (kv *ShardKV) CheckConfig() {

	_, isLeader := kv.rf.GetState()
	configs := kv.mck.QueryAll(-2)
	newconfig := configs[len(configs)-1]

	if kv.Now_Num != newconfig.Num {
		if isLeader {
			// DEBUG(dLog, "S%d G%d the config.num is %v != config1.num is %v\n", kv.me, kv.gid, kv.config.Num, newconfig.Num)
			// DEBUG(dLog, "S%d G%d the config(%v)\n", kv.me, kv.gid, kv.config)
			DEBUG(dLog, "S%d G%d new config(%v)\n", kv.me, kv.gid, newconfig)
		}
		go kv.getallconfigs(configs)
		kv.KVSGET[newconfig.Num] = 0
	}
	// if newconfig.Num == kv.Now_Num && (kv.KVSGET[newconfig.Num] == 1 || !isLeader){
	// 	return
	// }

	// else if newconfig.Num == kv.rpcindex {
	// 	DEBUG(dLog, "S%d G%d num is not change and had send rpc\n", kv.me, kv.gid)
	// 	return
	// }

	kv.mu.Lock()
	// DEBUG(dLog, "S%d G%d newconfig.num and kv.config.num and kv.now_num is %v %v %v\n", kv.me, kv.gid, newconfig.Num, kv.config.Num, kv.Now_Num)
	DEBUG(dLog, "S%d G%d lock 713 %v\n", kv.me, kv.gid, isLeader)
	kv.config = newconfig
	kv.check = true
	num := kv.config.Num
	kv.Now_Num = num
	gid := kv.gid

	if !isLeader {
		kv.mu.Unlock()
		return
	}

	for i, it := range newconfig.Shards {

		if it == gid {
			if kv.KVSMAP[i] != newconfig.Num && kv.KVSGET[newconfig.Num] == 0 { //&& kv.rpcindex != newconfig.Num
				DEBUG(dLog, "S%d G%d add a shard(%d) num(%v)\n", kv.me, kv.gid, i, num)

				go kv.SendToGetConfig(num, i)
			}
		}
	}
	kv.KVSGET[newconfig.Num] = 1
	// kv.rpcindex = newconfig.Num
	kv.mu.Unlock()
	DEBUG(dLog, "S%d G%d Unlock 1036\n", kv.me, kv.gid)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.get = make(chan COMD)
	kv.putAdd = make(chan COMD)
	kv.getkvs = make(chan COMD)
	kv.num_change = make(chan COMD)
	kv.CON = make(map[int]shardctrler.Config)
	kv.ChanComd = make(map[int]COMD)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// kv.KVS = []map[string]string

	kv.KVSMAP = make(map[int]int)
	kv.KVSGET = make(map[int]int)
	kv.mu = sync.Mutex{}
	kv.mu1 = sync.Mutex{}
	kv.mu2 = sync.Mutex{}
	kv.applyindex = 0
	Applyindex := 0
	// kv.rpcindex = 0
	kv.config = kv.mck.Query(-1)
	kv.Now_Num = kv.config.Num
	kv.check = false
	kv.CON[kv.config.Num] = kv.config
	for i := range kv.config.Shards {
		kv.KVSMAP[i] = 0
		kv.KVSGET[i] = 0
		kv.KVS = append(kv.KVS, make(map[string]string))
		kv.CSM = append(kv.CSM, make(map[int64]int64))
		kv.CDM = append(kv.CDM, make(map[int64]int64))
	}
	//kv.GetManageMap()

	kv.CheckConfig()

	DEBUG(dSnap, "S%d G%v ????????? update applyindex to %v the config is %v\n", kv.me, kv.gid, kv.applyindex, kv.config)
	// var NUMS int
	// if maxraftstate > 0 {
	// 	NUMS = maxraftstate / 80
	// } else {
	// 	NUMS = -1
	// }
	LOGinit()
	var command Op

	_, si := kv.rf.RaftSize()
	DEBUG(dTest, "S%d G%d this server start now max(%v) sizeof(Op) is %v the raft size is %v\n", kv.me, kv.gid, int(float64(maxraftstate)), unsafe.Sizeof(command), si)

	go func() {
		for {
			if !kv.killed() {
				kv.CheckConfig()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// go func() {
	// 	if maxraftstate > 0 {
	// 		for {
	// 			kv.CheckSnap()
	// 			time.Sleep(TIMEOUT * time.Microsecond)
	// 		}
	// 	}
	// }()

	go func() {

		for {
			if !kv.killed() {
				select {
				case m := <-kv.applyCh:

					if m.CommandValid {
						var start time.Time
						start = time.Now()
						_, isLeader := kv.rf.GetState()
						DEBUG(dLog, "S%d G%d try lock 847\n", kv.me, kv.gid)
						kv.mu.Lock()
						DEBUG(dLog, "S%d G%d success lock 847\n", kv.me, kv.gid)
						ti := time.Since(start).Milliseconds()
						DEBUG(dLog2, "S%d G%d AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d\n", kv.me, kv.gid, ti)

						O := m.Command.(Op)
						// _, isLeader := kv.rf.GetState()
						// if isLeader {
						DEBUG(dLog, "S%d G%d TTT CommandValid(%v) applyindex(%v) CommandIndex(%v) %v key(%v) value(%v) CDM[C%v](%v) from(%v)\n", kv.me, kv.gid, m.CommandValid, kv.applyindex, m.CommandIndex, O.Operate, O.Key, O.Value, O.Cli_index, O.Cmd_index, O.Ser_index)

						DEBUG(dLog, "S%d G%d kvs is %v\n", kv.me, kv.gid, kv.KVS)
						DEBUG(dLog, "S%d G%d config is %v\n", kv.me, kv.gid, kv.config)
						DEBUG(dLog, "S%d G%d KVSMAP(%v)\n", kv.me, kv.gid, kv.KVSMAP)
						// }
						if kv.applyindex+1 == m.CommandIndex {

							if O.Cli_index == -1 {
								DEBUG(dLog, "S%d G%d for TIMEOUT update applyindex %v to %v\n", kv.me, kv.gid, kv.applyindex, m.CommandIndex)
								kv.applyindex = m.CommandIndex
							} else if O.Cli_index == -2 {

								// var start time.Time
								// start = time.Now()
								if O.Num > kv.KVSMAP[O.Shard] {
									N := kv.KVSMAP[O.Shard]
									kv.KVSMAP[O.Shard] = O.Num

									for k, v := range O.KVS {
										kv.KVS[O.Shard][k] = v
									}

									for k, v := range O.CSM {
										kv.CSM[O.Shard][k] = v
									}

									for k, v := range O.CDM {
										kv.CDM[O.Shard][k] = v
									}

									// kv.KVS[O.Shard] = O.KVS
									// kv.CSM[O.Shard] = O.CSM
									// kv.CDM[O.Shard] = O.CDM

									if isLeader {
										DEBUG(dLog, "S%d G%d %v success Shard(%v)\n", kv.me, kv.gid, O.Operate, O.Shard)
										DEBUG(dInfo, "S%d G%d update KVSMAP[%v] from %v to %v\n", kv.me, kv.gid, O.Shard, N, O.Num)
										DEBUG(dInfo, "S%d G%d BBBB Update Config KVS[%v] to %v\n", kv.me, kv.gid, O.Shard, O.KVS)
										DEBUG(dInfo, "S%d G%d Update Config KVSMAP(%v)\n", kv.me, kv.gid, kv.KVSMAP)
									}
								}

								kv.applyindex = m.CommandIndex

								select {
								case kv.num_change <- COMD{
									num: kv.KVSMAP[O.Shard],
								}:
									DEBUG(dLog, "S%d G%d write num_change in(%v)\n", kv.me, kv.gid, m.CommandIndex)
								default:
									DEBUG(dLog, "S%d G%d can not write num_change in(%v)\n", kv.me, kv.gid, m.CommandIndex)
								}

								// ti := time.Since(start).Milliseconds()
								// DEBUG(dLog2, "S%d G%d -2 BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB%d\n", kv.me, kv.gid, ti)

							} else if O.Cli_index == -3 {
								// var start time.Time
								// start = time.Now()
								DEBUG(dLog, "S%d G%d update applyindex %v to %v\n", kv.me, kv.gid, kv.applyindex, m.CommandIndex)
								kv.applyindex = m.CommandIndex
								if O.Operate == "GetKvs" {

									Kvs := make(map[string]string)
									for k, v := range kv.KVS[O.Shard] {
										Kvs[k] = v
									}
									// if kv.KVSMAP[O.Shard] >= O.Num {
									// 	CIndex := O.Ser_index*1000 + O.Gro_index
									// 	DEBUG(dLog, "S%d G%d update CDM[%v] from %v to %v\n", kv.me, kv.gid, CIndex, kv.CDM[CIndex], O.Cmd_index)
									// 	kv.CDM[O.Shard][CIndex] = O.Cmd_index
									// }
									Csm := make(map[int64]int64)
									for k, v := range kv.CSM[O.Shard] {
										Csm[k] = v
									}

									Cdm := make(map[int64]int64)
									for k, v := range kv.CDM[O.Shard] {
										Cdm[k] = v
									}

									if Applyindex < kv.applyindex {
										select {
										case kv.getkvs <- COMD{index: m.CommandIndex,
											kvs:     Kvs,
											csm:     Csm,
											cdm:     Cdm,
											num:     kv.KVSMAP[O.Shard],
											The_num: kv.config.Num,
										}:
											DEBUG(dLog, "S%d G%d do ret write S%d G%d shard(%v) num(%v) kvs(%v)\n", kv.me, kv.gid, O.Ser_index, O.Gro_index, O.Shard, O.Num, Kvs)
											DEBUG(dLog, "S%d G%d write getkvs in(%v)\n", kv.me, kv.gid, m.CommandIndex)
										default:
											DEBUG(dLog, "S%d G%d do ret not write S%d G%d shard(%v) num(%v) kvs(%v)\n", kv.me, kv.gid, O.Ser_index, O.Gro_index, O.Shard, O.Num, Kvs)
											DEBUG(dLog, "S%d G%d can not write getkvs in(%v)\n", kv.me, kv.gid, m.CommandIndex)
										}
									}
								}

								// ti := time.Since(start).Milliseconds()
								// DEBUG(dLog2, "S%d G%d -3 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d\n", kv.me, kv.gid, ti)

							} else if O.Cli_index == -4 {

								// var start time.Time
								// start = time.Now()

								if O.Num > kv.KVSMAP[O.Shard] {

									N := kv.KVSMAP[O.Shard]
									kv.KVSMAP[O.Shard] = O.Num

									if isLeader {
										DEBUG(dLog, "S%d G%d %v Shard(%v)\n", kv.me, kv.gid, O.Operate, O.Shard)
										DEBUG(dInfo, "S%d G%d update KVSMAP[%v] from %v to %v\n", kv.me, kv.gid, O.Shard, N, O.Num)
										DEBUG(dInfo, "S%d G%d CCCC Update Config KVS[%v] to %v\n", kv.me, kv.gid, O.Shard, kv.KVS[O.Shard])
										DEBUG(dInfo, "S%d G%d Update Config KVSMAP(%v)\n", kv.me, kv.gid, kv.KVSMAP)
									}
								}

								kv.applyindex = m.CommandIndex

								select {
								case kv.num_change <- COMD{
									num: kv.KVSMAP[O.Shard],
								}:
									DEBUG(dLog, "S%d G%d write num_change in(%v)\n", kv.me, kv.gid, m.CommandIndex)
								default:
									DEBUG(dLog, "S%d G%d can not write num_change in(%v)\n", kv.me, kv.gid, m.CommandIndex)
								}
								// ti := time.Since(start).Milliseconds()
								// DEBUG(dLog2, "S%d G%d -4 CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC%d\n", kv.me, kv.gid, ti)

							} else if kv.CDM[O.Shard][O.Cli_index] < O.Cmd_index {
								DEBUG(dLeader, "S%d G%d update CDM[%v] from %v to %v update applyindex %v to %v\n", kv.me, kv.gid, O.Cli_index, kv.CDM[O.Shard][O.Cli_index], O.Cmd_index, kv.applyindex, m.CommandIndex)
								kv.applyindex = m.CommandIndex

								kv.CDM[O.Shard][O.Cli_index] = O.Cmd_index
								if O.Operate == "Append" {

									select {
									case kv.putAdd <- COMD{index: m.CommandIndex}:
										// DEBUG(dLog, "S%d G%d write putAdd in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									default:
										// DEBUG(dLog, "S%d G%d can not write putAdd in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									}

									val, ok := kv.KVS[O.Shard][O.Key]
									if ok {
										DEBUG(dLog, "S%d G%d BBBBBBB append Key(%v) from %v to value(%v) from(%v)\n", kv.me, kv.gid, O.Key, kv.KVS[O.Shard][O.Key], O.Value, O.Ser_index)
										kv.KVS[O.Shard][O.Key] = val + O.Value
									} else {
										DEBUG(dLog, "S%d G%d BBBBBBB append key(%v) from nil to %v from(%v)\n", kv.me, kv.gid, O.Key, O.Value, O.Ser_index)
										kv.KVS[O.Shard][O.Key] = O.Value
									}
								} else if O.Operate == "Put" {

									select {
									case kv.putAdd <- COMD{index: m.CommandIndex}:
										// DEBUG(dLog, "S%d G%d write putAdd in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									default:
										// DEBUG(dLog, "S%d G%d can not write putAdd in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									}

									_, ok := kv.KVS[O.Shard][O.Key]
									if ok {
										DEBUG(dLog, "S%d G%d AAAAAAA put key(%v) from %v to %v from(%v)\n", kv.me, kv.gid, O.Key, kv.KVS[O.Shard][O.Key], O.Value, O.Ser_index)
										kv.KVS[O.Shard][O.Key] = O.Value
									} else {
										DEBUG(dLog, "S%d G%d AAAAAAA put key(%v) from nil to %v from(%v)\n", kv.me, kv.gid, O.Key, O.Value, O.Ser_index)
										kv.KVS[O.Shard][O.Key] = O.Value
									}
								} else if O.Operate == "Get" {

									select {
									case kv.get <- COMD{index: m.CommandIndex}:
										// DEBUG(dLog, "S%d G%d write get in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									default:
										// DEBUG(dLog, "S%d G%d can not write get in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									}
								}
							} else if kv.CDM[O.Shard][O.Cli_index] == O.Cmd_index {
								DEBUG(dLog2, "S%d G%d this cmd had done, the log had two update applyindex %v to %v\n", kv.me, kv.gid, kv.applyindex, m.CommandIndex)
								kv.applyindex = m.CommandIndex
							} else {
								DEBUG(dLog2, "S%d G%d the shard(%v) Cindex(%v) OIndex(%v) < CDM(%v)\n", kv.me, kv.gid, O.Shard, O.Cli_index, O.Cmd_index, kv.CDM[O.Shard][O.Cli_index])
								kv.applyindex = m.CommandIndex
							}

						} else if kv.applyindex+1 < m.CommandIndex {
							DEBUG(dWarn, "S%d G%d the applyindex + 1 (%v) < commandindex(%v)\n", kv.me, kv.gid, kv.applyindex, m.CommandIndex)
							// kv.applyindex = m.CommandIndex
						}

						if maxraftstate > 0 {
							kv.CheckSnap()
						}

						kv.mu.Unlock()
						DEBUG(dLog, "S%d G%d Unlock 1369\n", kv.me, kv.gid)

						// if maxraftstate > 0 {
						// 	go kv.CheckSnap()
						// }

					} else { //read snapshot
						r := bytes.NewBuffer(m.Snapshot)
						d := labgob.NewDecoder(r)
						DEBUG(dSnap, "S%d G%d the snapshot applied\n", kv.me, kv.gid)
						var S SnapShot
						kv.mu.Lock()
						DEBUG(dLog, "S%d G%d lock 1029\n", kv.me, kv.gid)
						if d.Decode(&S) != nil {
							kv.mu.Unlock()
							DEBUG(dLog, "S%d G%d Unlock 1384\n", kv.me, kv.gid)
							DEBUG(dSnap, "S%d G%d labgob fail\n", kv.me, kv.gid)
						} else {
							kv.CDM = S.Cdm
							kv.CSM = S.Csm
							kv.KVS = S.Kvs
							// kv.config = S.Config
							kv.KVSMAP = S.KVSMAP
							// kv.rpcindex = S.Rpcindex
							kv.check = false
							Applyindex = kv.applyindex
							DEBUG(dSnap, "S%d G%d recover by SnapShot update applyindex(%v) to %v the kvs is %v\n", kv.me, kv.gid, kv.applyindex, S.Apliedindex, kv.KVS)
							kv.applyindex = S.Apliedindex
							kv.mu.Unlock()
							DEBUG(dLog, "S%d G%d Unlock 1397\n", kv.me, kv.gid)
							go kv.CheckConfig()
						}

					}

				case <-time.After(TIMEOUT * time.Microsecond):
					O := Op{
						Ser_index: int64(kv.me),
						Cli_index: -1,
						Cmd_index: -1,
						Operate:   "TIMEOUT",
					}
					DEBUG(dLog, "S%d G%d have log time applied\n", kv.me, kv.gid)
					kv.rf.Start(O)
				}
			}
		}

	}()

	return kv
}
