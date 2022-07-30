package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leader    int
	cli_index int64
	cmd_index int64
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.

	ck.config = ck.sm.Query(-1)
	ck.cli_index = nrand()
	ck.cmd_index = 0
	ck.leader = 0
	LOGinit()

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.cmd_index++
	args := GetArgs{}
	args.Key = key
	args.CIndex = ck.cli_index
	args.OIndex = ck.cmd_index
	try_num := 3
	DEBUG(dClient, "C%d getkey(%v)\n", ck.cli_index, key)
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.Shard = shard
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := ck.leader; si < len(servers); {
				srv := ck.make_end(servers[si])
				var reply GetReply
				DEBUG(dClient, "C%d send to S%v G%v Get key(%v) the cmd_index(%v) shard(%v)\n", ck.cli_index, si, gid, args.Key, ck.cmd_index, shard)
				ok := srv.Call("ShardKV.Get", &args, &reply)

				if ok {
					try_num = 3
				}

				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DEBUG(dClient, "C%d success Get key(%v) value(%v) the cmd_index(%v)\n", ck.cli_index, args.Key, reply.Value, ck.cmd_index)
					return reply.Value
				}else if ok && (reply.Err == ErrWrongGroup) {
					DEBUG(dClient, "C%d the Group ERROR\n", ck.cli_index)
					break
				}else if ok && reply.Err == ErrWrongLeader {
					DEBUG(dClient, "C%d the S%v is not leader\n", ck.cli_index, si)
					si++
					if si == len(servers) {
						si = 0
					}
					// time.Sleep(1000 * time.Microsecond)
				}else if !ok || reply.Err == ErrTimeOut{
					// time.Sleep(1000 * time.Microsecond)
					DEBUG(dClient, "C%d the timeout\n", ck.cli_index)
					if try_num > 0{
						try_num--
					}else{
						si++
						if si == len(servers) {
							si = 0
						}
					}
				}
				// ... not ok, or ErrWrongLeader
			}
		}else{
			DEBUG(dClient, "C%d get gid(%v) is not int Groups(%v)\n", ck.cli_index, gid, ck.config.Groups)
		}
		time.Sleep(500 * time.Microsecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.cmd_index++
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.CIndex = ck.cli_index
	args.OIndex = ck.cmd_index
	DEBUG(dClient, "C%d %vkey(%v)value(%v)\n", ck.cli_index, op, key, value)
	try_num := 3

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.Shard = shard
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := ck.leader; si < len(servers); {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DEBUG(dClient, "C%d send to S%v G%v %v key(%v) value(%v) the cmd_index(%v) shard(%v)\n", ck.cli_index, si, gid, args.Op, args.Key, args.Value, ck.cmd_index, shard)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)

				if ok {
					try_num = 3
				}

				if ok && reply.Err == OK {
					DEBUG(dClient, "C%d success the cmd_index(%v)\n", ck.cli_index, ck.cmd_index)
					return
				}else if ok && reply.Err == ErrWrongGroup {
					DEBUG(dClient, "C%d the Group ERROR\n", ck.cli_index)
					break
				}else if ok && reply.Err == ErrWrongLeader {
					DEBUG(dClient, "C%d the S%v is not leader\n", ck.cli_index, si)
					si++
					if si == len(servers) {
						si = 0
						// time.Sleep(100 * time.Microsecond)
					}
				}else if !ok || reply.Err == ErrTimeOut{
					DEBUG(dClient, "C%d the TIMEOUT\n", ck.cli_index)
					if try_num > 0{
						try_num--
					}else{
						si++
						if si == len(servers) {
							si = 0
						}
					}
				}
				// ... not ok, or ErrWrongLeader
			}
		}else{
			DEBUG(dClient, "C%d putappend gid(%v) is not int Groups(%v)\n", ck.cli_index, gid, ck.config.Groups)
		}
		time.Sleep(500 * time.Microsecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
