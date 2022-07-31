package shardkv

import (
	"bytes"
	"fmt"
	"sync"
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
	Key       string
	Value     string
}

type COMD struct {
	index int
	kvs   map[string]string
	num   int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	mck          *shardctrler.Clerk

	putAdd chan COMD
	get    chan COMD
	getkvs chan COMD

	// Your definitions here.
	KVS []map[string]string
	CSM map[int64]int64
	CDM map[int64]int64

	applyindex int
	config     shardctrler.Config
	KVSMAP     map[int]int
	rpcindex   int
}

type SnapShot struct {
	Kvs    []map[string]string
	Csm    map[int64]int64
	Cdm    map[int64]int64
	KVSMAP map[int]int
	Config shardctrler.Config

	Apliedindex int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DEBUG(dLeader, "S%d G%d <-- C%v Get key(%v)\n", kv.me, kv.gid, args.CIndex, args.Key)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DEBUG(dLog, "S%d G%d this is not leader\n", kv.me, kv.gid)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	DEBUG(dLog, "S%d G%d config(%v) kvs(%v)\n", kv.me, kv.gid, kv.config, kv.KVS)
	if kv.config.Num != kv.KVSMAP[args.Shard] || kv.config.Num != args.Num {
		DEBUG(dLog, "S%d G%d not have this shard%v KVSMAP(%v) config.NUM(%v) args.Num(%v)\n", kv.me, kv.gid, args.Shard, kv.KVSMAP[args.Shard], kv.config.Num, args.Num)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if kv.applyindex == 0 {
		DEBUG(dLog, "S%d G%d the snap not applied applyindex is %v\n", kv.me, kv.gid, kv.applyindex)
		kv.mu.Unlock()
		reply.Err = ErrTimeOut
		time.Sleep(TIMEOUT * time.Microsecond)
		return
	}

	in1, okk1 := kv.CDM[args.CIndex]
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
		kv.CDM[args.CIndex] = 0
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
	in2, okk2 := kv.CSM[args.CIndex]
	if !okk2 {
		kv.CSM[args.CIndex] = 0
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
		lastindex, ok := kv.CSM[args.CIndex]
		if !ok {
			kv.CSM[args.CIndex] = 0
		}
		kv.CSM[args.CIndex] = args.OIndex
		kv.mu.Unlock()

		DEBUG(dLeader, "S%d G%d <-- C%v Get key(%v) wait index(%v)\n", kv.me, kv.gid, args.CIndex, args.Key, index)
		for {
			select {
			case out := <-kv.get:
				if index == out.index {
					kv.mu.Lock()
					DEBUG(dLeader, "S%d G%d kvs(%v) index(%v) from(%v)\n", kv.me, kv.gid, kv.KVS, index, kv.me)
					val, ok := kv.KVS[args.Shard][args.Key]
					if ok {
						DEBUG(dLeader, "S%d G%d Get key(%v) value(%v) OK from(C%v)\n", kv.me, kv.gid, args.Key, val, args.CIndex)
						reply.Err = OK
						reply.Value = val
						kv.mu.Unlock()
						return
					} else {
						DEBUG(dLeader, "S%d G%d Get key(%v) value(%v) this map do not have value map %v from(C%v)\n", kv.me, kv.gid, args.Key, val, kv.KVS, args.CIndex)
						reply.Err = ErrNoKey
						kv.mu.Unlock()
						return
					}
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				kv.mu.Lock()
				DEBUG(dLeader, "S%d G%d is time out\n", kv.me, kv.gid)
				reply.Err = ErrTimeOut
				if _, isLeader := kv.rf.GetState(); !isLeader {
					reply.Err = ErrWrongLeader
					kv.CSM[args.CIndex] = lastindex
				}
				kv.mu.Unlock()
				return

			}
		}

	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DEBUG(dLeader, "S%d G%d <-- C%v putappend key(%v) value(%v)\n", kv.me, kv.gid, args.CIndex, args.Key, args.Value)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DEBUG(dLog, "S%d G%d this is not leader\n", kv.me, kv.gid)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	DEBUG(dLog, "S%d G%d config(%v) kvs(%v)\n", kv.me, kv.gid, kv.config, kv.KVS)
	if kv.config.Num != kv.KVSMAP[args.Shard] || kv.config.Num != args.Num {
		DEBUG(dLog, "S%d G%d not have this shard%v KVSMAP(%v) config.NUM(%v) args.Num(%v)\n", kv.me, kv.gid, args.Shard, kv.KVSMAP[args.Shard], kv.config.Num, args.Num)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if kv.applyindex == 0 {
		DEBUG(dLog, "S%d G%d the snap not applied applyindex is %v\n", kv.me, kv.gid, kv.applyindex)
		kv.mu.Unlock()
		reply.Err = ErrTimeOut
		time.Sleep(TIMEOUT * time.Microsecond)
		return
	}

	in1, okk1 := kv.CDM[args.CIndex]
	if okk1 && in1 == args.OIndex {
		reply.Err = OK //had done
		DEBUG(dLeader, "S%d G%d had done key(%v) value(%v) Op(%v) from(C%d) OIndex(%d)\n", kv.me, kv.gid, args.Key, args.Value, args.Op, args.CIndex, args.OIndex)
		kv.mu.Unlock()
		return
	} else if !okk1 {
		kv.CDM[args.CIndex] = 0
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
	in2, okk2 := kv.CSM[args.CIndex]
	if !okk2 {
		kv.CSM[args.CIndex] = 0
	}
	kv.mu.Unlock()

	if in2 == args.OIndex {
		_, isLeader = kv.rf.GetState()
	} else {
		index, _, isLeader = kv.rf.Start(O)
		// go kv.SendToSnap()
	}

	if !isLeader {
		DEBUG(dLeader, "S%d %d <-- C%v %v key(%v) value(%v) but not leader\n", kv.me, kv.gid, args.CIndex, args.Op, args.Key, args.Value)
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
		lastindex, ok := kv.CSM[args.CIndex]
		if !ok {
			kv.CSM[args.CIndex] = 0
		}
		kv.CSM[args.CIndex] = args.OIndex
		kv.mu.Unlock()

		DEBUG(dLeader, "S%d G%d <-- C%v %v key(%v) value(%v) index(%v) wait\n", kv.me, kv.gid, args.CIndex, args.Op, args.Key, args.Value, index)
		for {
			select {
			case out := <-kv.putAdd:
				if index == out.index {
					DEBUG(dLeader, "S%d G%d %v index(%v) applyindex(%v)  this cmd_index(%v) key(%v) value(%v) from(C%v)\n", kv.me, kv.gid, args.Op, index, out.index, args.OIndex, args.Key, args.Value, args.CIndex)
					reply.Err = OK
					return
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				kv.mu.Lock()
				DEBUG(dLeader, "S%d G%d time out\n", kv.me, kv.gid)
				reply.Err = ErrTimeOut
				if _, isLeader := kv.rf.GetState(); !isLeader {
					reply.Err = ErrWrongLeader
					kv.CSM[args.CIndex] = lastindex
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
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) SendSnapShot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	S := SnapShot{
		Kvs:         kv.KVS,
		Csm:         kv.CSM,
		Cdm:         kv.CDM,
		KVSMAP:      kv.KVSMAP,
		Config:      kv.config,
		Apliedindex: kv.applyindex,
	}
	e.Encode(S)
	DEBUG(dSnap, "S%d G%d the size need to snap index%v\n", kv.me, S.Apliedindex, kv.gid)
	data := w.Bytes()
	kv.rf.Snapshot(S.Apliedindex, data)
	X, num := kv.rf.RaftSize()
	fmt.Println("S", kv.me, "num", num, "X", X)
}

func (kv *ShardKV) CheckSnap() {
	//kv.mu.Lock()

	DEBUG(dLog, "S%d G%d kvs is %v\n", kv.me, kv.gid, kv.KVS)
	DEBUG(dLog, "S%d G%d config is %v\n", kv.me, kv.gid, kv.config)
	DEBUG(dLog, "S%d G%d KVSMAP(%v)\n", kv.me, kv.gid, kv.KVSMAP)

	X, num := kv.rf.RaftSize()
	DEBUG(dSnap, "S%d G%d the size is (%v) applidindex(%v) X(%v)\n", kv.me, kv.gid, num, kv.applyindex, X)
	if num > int(float64(kv.maxraftstate)*0.8) {
		if kv.applyindex == 0 || kv.applyindex <= X {
			// kv.mu.Unlock()
			return
		}
		kv.SendSnapShot()
	}
	//kv.mu.Unlock()
}

func (kv *ShardKV) GetConfig(args *GetConfigArgs, reply *GetConfigReply) {
	// config := kv.mck.Query(-1)

	DEBUG(dLog, "S%d G%d <--- S%d G%d get shard(%v) num(%v) kvs\n", kv.me, kv.gid, args.SIndex, args.GIndex, args.Shard, args.Num)
	oldconfig := shardctrler.Config{}

	if args.Num > 0 {
		oldconfig = kv.mck.Query(args.Num)
	} else {
		oldconfig = shardctrler.Config{}
	}
	_, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		DEBUG(dLog, "S%d G%d this server is not leader\n", kv.me, kv.gid)
		return
	}

	if oldconfig.Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		DEBUG(dLog, "S%d G%d shard(%v) this is a never print log config(%v)\n", kv.me, kv.gid, args.Shard, oldconfig)
		return
	}

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
			DEBUG(dLeader, "S%d G%d read from get kvs index(%v) out.index(%v)\n", kv.me, kv.gid, index, out.index)
			if index == out.index {

				if out.num == args.Num {
					DEBUG(dLog, "S%d G%d the kvs is %v\n", kv.me, kv.gid, kv.KVS)
					DEBUG(dLog, "S%d G%d success return to S%d G%d the kvs(%v) shard is %v KVSMAP[shard](%v) args.Num(%v) index%v\n", kv.me, kv.gid, args.SIndex, args.GIndex, out.kvs, args.Shard, kv.KVSMAP[args.Shard], args.Num, index)

					reply.Kvs = out.kvs
					reply.Err = OK
					reply.Shard = args.Shard
					reply.Err = OK
					return
				} else {
					DEBUG(dLog, "S%d G%d this group kvs have not get over KVSMAP(%v) shard(%d) args.num(%v) index%v\n", kv.me, kv.gid, kv.KVSMAP, args.Shard, args.Num, index)
					_, _, isLeader := kv.rf.Start(O)
					if !isLeader {
						reply.Err = ErrWrongLeader
						DEBUG(dLog, "S%d G%d this server is not leader\n", kv.me, kv.gid)
						return
					}
					continue
				}
			}

		case <-time.After(TIMEOUT * 100 * time.Microsecond):
			kv.mu.Lock()
			DEBUG(dLeader, "S%d G%d time out index%v\n", kv.me, kv.gid, index)
			reply.Err = ErrTimeOut
			if _, isLeader := kv.rf.GetState(); !isLeader {
				reply.Err = ErrWrongLeader
			}
			kv.mu.Unlock()
			return
		}
	}

}

func (kv *ShardKV) SendToGetConfig(num int, shard int) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}

	args := GetConfigArgs{}
	args.Shard = shard
	args.SIndex = kv.me
	args.GIndex = kv.gid
	oldconfig := shardctrler.Config{}
	The_Num := num
	num--
	oldconfig = kv.mck.Query(num)
	DEBUG(dLog, "S%d G%d in sendgetconfig shrad is %v num is %v\n", kv.me, kv.gid, shard, num)

	for {
		kv.mu.Lock()
		if num == 0 {
			O := Op{
				Ser_index: int64(kv.me),
				Gro_index: int64(kv.gid),
				Cli_index: -4,
				Operate:   "OwnCon",
				Shard:     shard,
				Num:       The_Num,
			}
			kv.mu.Unlock()
			_, _, Leader := kv.rf.Start(O)
			DEBUG(dInfo, "S%d G%d Start CON old num = 0 The_num is %v %v\n", kv.me, kv.gid, The_Num, Leader)
			return
		}
		DEBUG(dLog, "S%d G%d oldconfig.Shard[%v](%v)\n", kv.me, kv.gid, shard, oldconfig.Shards[shard])
		if oldconfig.Shards[shard] == kv.gid {
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
				_, _, Leader := kv.rf.Start(O)
				DEBUG(dInfo, "S%d G%d Start num(%v) == KVSMAP[%v] %v\n", kv.me, kv.gid, num, shard, Leader)
				return
			} else {
				DEBUG(dLog, "S%d G%d num from %v to %v\n", kv.me, kv.gid, num, num-1)
				num--
			}	
		}else{
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
	}

	try_num := 3
	for {

		args.Num = num
		gid := oldconfig.Shards[shard]
		DEBUG(dLog, "S%d G%d shard(%d) num(%d) the oldconfig is %v\n", kv.me, kv.gid, shard, num, oldconfig)
		if servers, ok := oldconfig.Groups[gid]; ok {

			DEBUG(dLog, "S%d G%d need send to get shard num is %d\n", kv.me, kv.gid, num)

			for si := 0; si < len(servers); {

				srv := kv.make_end(servers[si])
				var reply GetConfigReply
				DEBUG(dLog, "S%d G%d send to S%v G%d get shard(%v) num(%v) The_num(%v)\n", kv.me, kv.gid, si, gid, shard, num, The_Num)
				ok := srv.Call("ShardKV.GetConfig", &args, &reply)

				if ok {
					try_num = 3
				}
				if ok && reply.Err == OK {
					DEBUG(dLog, "S%d G%d success get shard%v kvs(%v)\n", kv.me, kv.gid, shard, reply.Kvs)
					kvs := make(map[string]string)
					for k, v := range reply.Kvs {
						kvs[k] = v
					}
					O := Op{
						Ser_index: int64(kv.me),
						Cli_index: -2,
						Gro_index: int64(kv.gid),
						Operate:   "Config",
						KVS:       kvs,
						Shard:     shard,
						Num:       The_Num,
					}

					_, _, Leader := kv.rf.Start(O)
					DEBUG(dInfo, "S%d G%d Start CON success get shard%v kvs %v\n", kv.me, kv.gid, shard, Leader)
					return
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
						if si == len(servers) {
							si = 0
						}
					}
				}

			}
		} else {
			fmt.Println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA num is", num)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (kv *ShardKV) CheckConfig() {

	kv.mu.Lock()

	newconfig := kv.mck.Query(-1)

	if kv.config.Num != newconfig.Num {
		DEBUG(dLog, "S%d G%d the config.num is %v != config1.num is %v\n", kv.me, kv.gid, kv.config.Num, newconfig.Num)
		DEBUG(dLog, "S%d G%d the config(%v)\n", kv.me, kv.gid, kv.config)
		DEBUG(dLog, "S%d G%d new config(%v)\n", kv.me, kv.gid, newconfig)
	} else {
		DEBUG(dLog, "S%d G%d num is not change\n", kv.me, kv.gid)
		kv.mu.Unlock()
		return
	}
	// oldconfig := kv.config
	kv.config = newconfig
	num := kv.config.Num
	gid := kv.gid
	kv.rpcindex++

	for i, it := range newconfig.Shards {

		if it == gid {

			if kv.KVSMAP[i] != kv.config.Num {
				// if kv.KVSMAP[i] ==
				DEBUG(dLog, "S%d G%d add a shard(%d) num(%v) the rpcindex is %v\n", kv.me, kv.gid, i, num, kv.rpcindex)
				go kv.SendToGetConfig(num, i)
			} else {
				DEBUG(dInfo, "S%d G%d update KVSMAP[%v] from %v to %v\n", kv.me, kv.gid, i, kv.KVSMAP[i], newconfig.Num)
				kv.KVSMAP[i] = newconfig.Num
			}
		}
	}

	kv.mu.Unlock()
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
	kv.rpcindex = 0

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.get = make(chan COMD)
	kv.putAdd = make(chan COMD)
	kv.getkvs = make(chan COMD)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// kv.KVS = []map[string]string
	kv.CSM = make(map[int64]int64)
	kv.CDM = make(map[int64]int64)
	kv.KVSMAP = make(map[int]int)
	kv.mu = sync.Mutex{}
	kv.applyindex = 0
	kv.config = kv.mck.Query(-1)
	for i, it := range kv.config.Shards {
		if it == kv.gid {
			kv.KVSMAP[i] = kv.config.Num
			kv.KVS = append(kv.KVS, make(map[string]string))
		} else {
			kv.KVSMAP[i] = kv.config.Num
			kv.KVS = append(kv.KVS, make(map[string]string))
		}
	}
	//kv.GetManageMap()

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
	DEBUG(dTest, "S%d G%d this server start now max(%v) sizeof(Op) is %v the raft size is %v\n", kv.me, kv.gid, int(float64(maxraftstate)*0.8), unsafe.Sizeof(command), si)

	go func() {
		for {
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				continue
			}
			kv.CheckConfig()

			time.Sleep(100 * time.Millisecond)
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
			// if !kv.killed() {
			select {
			case m := <-kv.applyCh:

				if m.CommandValid {
					// var start time.Time
					// start = time.Now()
					kv.mu.Lock()
					// ti := time.Since(start).Milliseconds()
					// DEBUG(dLog2, "S%d G%d AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d\n", kv.me, kv.gid, ti)

					O := m.Command.(Op)
					DEBUG(dLog, "S%d G%d TTT CommandValid(%v) applyindex(%v) CommandIndex(%v) %v key(%v) value(%v) CDM[C%v](%v) M(%v) from(%v)\n", kv.me, kv.gid, m.CommandValid, kv.applyindex, m.CommandIndex, O.Operate, O.Key, O.Value, O.Cli_index, kv.CDM[O.Cli_index], O.Cmd_index, O.Ser_index)

					DEBUG(dLog, "S%d G%d kvs is %v\n", kv.me, kv.gid, kv.KVS)
					DEBUG(dLog, "S%d G%d config is %v\n", kv.me, kv.gid, kv.config)
					DEBUG(dLog, "S%d G%d KVSMAP(%v)\n", kv.me, kv.gid, kv.KVSMAP)

					if kv.applyindex+1 == m.CommandIndex {

						if O.Cli_index == -1 {
							DEBUG(dLog, "S%d G%d for TIMEOUT update applyindex %v to %v\n", kv.me, kv.gid, kv.applyindex, m.CommandIndex)
							kv.applyindex = m.CommandIndex
						} else if O.Cli_index == -2 {

							var start time.Time
							start = time.Now()

							DEBUG(dLog, "S%d G%d %v Shard(%v)\n", kv.me, kv.gid, O.Operate, O.Shard)
							DEBUG(dInfo, "S%d G%d update KVSMAP[%v] from %v to %v\n", kv.me, kv.gid, O.Shard, kv.KVSMAP[O.Shard], O.Num)
							DEBUG(dLog, "S%d G%d update applyindex %v to %v\n", kv.me, kv.gid, kv.applyindex, m.CommandIndex)
							kv.applyindex = m.CommandIndex
							kv.KVSMAP[O.Shard] = O.Num

							for k, v := range O.KVS {
								kv.KVS[O.Shard][k] = v
							}
							DEBUG(dInfo, "S%d G%d BBBB Update Config KVS[%v] to %v\n", kv.me, kv.gid, O.Shard, O.KVS)
							DEBUG(dInfo, "S%d G%d Update Config KVSMAP(%v)\n", kv.me, kv.gid, kv.KVSMAP)

							ti := time.Since(start).Milliseconds()
							DEBUG(dLog2, "S%d G%d -3 BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB%d\n", kv.me, kv.gid, ti)

						} else if O.Cli_index == -3 {
							var start time.Time
							start = time.Now()
							DEBUG(dLog, "S%d G%d update applyindex %v to %v\n", kv.me, kv.gid, kv.applyindex, m.CommandIndex)
							kv.applyindex = m.CommandIndex
							if O.Operate == "GetKvs" {

								Kvs := make(map[string]string)
								for k, v := range kv.KVS[O.Shard] {
									Kvs[k] = v
								}
								DEBUG(dLog, "S%d G%d do ret S%d G%d shard(%v) num(%v) kvs(%v)\n", kv.me, kv.gid, O.Ser_index, O.Gro_index, O.Shard, O.Num, Kvs)
								select {
								case kv.getkvs <- COMD{index: m.CommandIndex,
									kvs: Kvs,
									num: kv.KVSMAP[O.Shard],
								}:
									DEBUG(dLog, "S%d G%d write getkvs in(%v)\n", kv.me, kv.gid, m.CommandIndex)
								default:
									DEBUG(dLog, "S%d G%d can not write getkvs in(%v)\n", kv.me, kv.gid, m.CommandIndex)
								}
							}

							ti := time.Since(start).Milliseconds()
							DEBUG(dLog2, "S%d G%d -3 AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d\n", kv.me, kv.gid, ti)

						} else if O.Cli_index == -4 {
							var start time.Time
							start = time.Now()

							DEBUG(dLog, "S%d G%d %v Shard(%v)\n", kv.me, kv.gid, O.Operate, O.Shard)
							DEBUG(dInfo, "S%d G%d update KVSMAP[%v] from %v to %v\n", kv.me, kv.gid, O.Shard, kv.KVSMAP[O.Shard], O.Num)
							DEBUG(dLog, "S%d G%d update applyindex %v to %v\n", kv.me, kv.gid, kv.applyindex, m.CommandIndex)
							kv.applyindex = m.CommandIndex
							kv.KVSMAP[O.Shard] = O.Num

							// for k, v := range kv.KVS[O.Shard] {
							// 	kv.KVS[O.Shard][k] = v
							// }
							DEBUG(dInfo, "S%d G%d CCCC Update Config KVS[%v] to %v\n", kv.me, kv.gid, O.Shard, kv.KVS[O.Shard])
							DEBUG(dInfo, "S%d G%d Update Config KVSMAP(%v)\n", kv.me, kv.gid, kv.KVSMAP)

							ti := time.Since(start).Milliseconds()
							DEBUG(dLog2, "S%d G%d -4 CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC%d\n", kv.me, kv.gid, ti)

						} else if kv.CDM[O.Cli_index] < O.Cmd_index {
							DEBUG(dLeader, "S%d G%d update CDM[%v] from %v to %v update applyindex %v to %v\n", kv.me, kv.gid, O.Cli_index, kv.CDM[O.Cli_index], O.Cmd_index, kv.applyindex, m.CommandIndex)
							kv.applyindex = m.CommandIndex

							kv.CDM[O.Cli_index] = O.Cmd_index
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
						} else if kv.CDM[O.Cli_index] == O.Cmd_index {
							DEBUG(dLog2, "S%d G%d this cmd had done, the log had two update applyindex %v to %v\n", kv.me, kv.gid, kv.applyindex, m.CommandIndex)
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

				} else { //read snapshot
					r := bytes.NewBuffer(m.Snapshot)
					d := labgob.NewDecoder(r)
					DEBUG(dSnap, "S%d G%d the snapshot applied\n", kv.me, kv.gid)
					var S SnapShot
					kv.mu.Lock()
					if d.Decode(&S) != nil {
						DEBUG(dSnap, "S%d G%d labgob fail\n", kv.me, kv.gid)
						kv.mu.Unlock()
					} else {
						kv.CDM = S.Cdm
						kv.CSM = S.Csm
						kv.KVS = S.Kvs
						kv.config = S.Config
						kv.KVSMAP = S.KVSMAP
						DEBUG(dSnap, "S%d G%d recover by SnapShot update applyindex(%v) to %v the kvs is %v\n", kv.me, kv.gid, kv.applyindex, S.Apliedindex, kv.KVS)
						kv.applyindex = S.Apliedindex
						kv.mu.Unlock()
						kv.CheckConfig()
					}

				}

			case <-time.After(TIMEOUT * 100 * time.Microsecond):
				O := Op{
					Ser_index: int64(kv.me),
					Cli_index: -1,
					Cmd_index: -1,
					Operate:   "TIMEOUT",
				}

				kv.rf.Start(O)
			}
			// } else {
			// 	return
			// }
		}

	}()

	return kv
}
