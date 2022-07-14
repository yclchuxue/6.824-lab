package kvraft

import (
	"crypto/rand"
	// "fmt"
	// "time"

	//"fmt"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leader    int
	ice       bool
	cli_index int64
	cmd_index int64
	send_try  int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64() % 5
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.send_try = 3

	// node, err := snowflake.NewNode(1)
	// if err != nil {
	// 	DEBUG(err)
	// }
	LOGinit()

	ck.cli_index = nrand()
	// node.Generate().Int64()
	ck.cmd_index = 0
	ck.ice = false
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	// var start time.Time
	// start = time.Now()
	//ti := time.Since(start).Milliseconds()

	ck.cmd_index++

	args := GetArgs{}
	args.Key = key
	args.CIndex = ck.cli_index
	args.OIndex = ck.cmd_index

	reply := GetReply{}

	// i := int(nrand())%(len(ck.servers))

	for i := ck.leader; i < len(ck.servers); {
		// if ck.ice {
			DEBUG(dInfo, "C%d send Get key(%v) Cindex(%v) OIndex(%v)\n", ck.cli_index, args.Key, args.CIndex, args.OIndex)
		// }
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			ck.send_try = 3
			if reply.Err == ErrWrongLeader {
				i++
				if i == len(ck.servers) {
					i = 0
				}
				reply = GetReply{}
				if ck.ice {
					DEBUG(dInfo, "C%d Get fail key(%v) value(%v)\n", ck.cli_index, args.Key, reply.Value)
				}
			} else if reply.Err == OK {
				ck.ice = true
				// if ck.ice {
				// 	ti := time.Since(start).Milliseconds()
				// 	fmt.Println("time = ", ti)
				// 	ck.ice = false
				// }
				ck.leader = i
				DEBUG(dInfo, "C%d Get success key(%v) value(%v) r.index(%v)\n", ck.cli_index, args.Key, reply.Value, reply.Index)
				return reply.Value
			} else {
				ck.ice = true
				ck.leader = i
				// if ck.ice {
				// 	ti := time.Since(start).Milliseconds()
				// 	fmt.Println("time = ", ti)
				// 	ck.ice = false
				// }
				DEBUG(dInfo, "C%d Get nil key(%v) value(%v) r.index(%v)\n", ck.cli_index, args.Key, reply.Value, reply.Index)
				return reply.Value
			}
		} else {
			if ck.send_try != 0 {
				ck.send_try--
			} else {
				i++
				if i == len(ck.servers) {
					i = 0
				}
			}
			DEBUG(dInfo, "C%d ERROR Get Key(%v)\n", ck.cli_index, args.Key)
		}
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// var start time.Time
	// start = time.Now()
	ck.cmd_index++

	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		CIndex: ck.cli_index,
		OIndex: ck.cmd_index,
	}

	reply := PutAppendReply{}

	// i := int(nrand())%(len(ck.servers))

	for i := ck.leader; i < len(ck.servers); {
		// if ck.ice {
			DEBUG(dInfo, "C%d send %v Key(%v) value(%v) CIndex(%v) OIndex(%v)\n", ck.cli_index, args.Op, args.Key, args.Value, args.CIndex, args.OIndex)
		// }
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			ck.send_try = 3
			if reply.Err == ErrWrongLeader {
				i++
				if i == len(ck.servers) {
					i = 0
				}
				reply = PutAppendReply{}
				if ck.ice {
					DEBUG(dInfo, "C%d %v err(%v) fail key(%v) value(%v) r.index(%v)\n", ck.cli_index, args.Op, reply.Err, key, args.Value, reply.Index)
				}
			} else if reply.Err == OK {
				ck.leader = i
				ck.ice = true
				// if ck.ice {
				// 	ti := time.Since(start).Milliseconds()
				// 	fmt.Println("time = ", ti)
				// 	ck.ice = false
				// }
				DEBUG(dInfo, "C%d %v err(%v), success key(%v) value(%v) r.Index(%v)\n", ck.cli_index, args.Op, reply.Err, key, args.Value, reply.Index)
				break
			} else {
				ck.ice = true
				ck.leader = i
				// if ck.ice {
				// 	ti := time.Since(start).Milliseconds()
				// 	fmt.Println("time = ", ti)
				// 	ck.ice = false
				// }
				DEBUG(dInfo, "C%d %v err(%v) nil key(%v) value(%v) r.index(%v)\n", ck.cli_index, args.Op, reply.Err, key, args.Value, reply.Index)
				if args.Op != "Append" {
					break
				}
			}
		} else {
			if ck.send_try != 0 {
				ck.send_try--
			} else {
				i++
				if i == len(ck.servers) {
					i = 0
				}
			}
			DEBUG(dInfo, "C%d ERROR %v key(%v) value(%v)\n", ck.cli_index, args.Op, args.Key, args.Value)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	DEBUG(dInfo, "C%d Put key(%v) value(%v)\n", ck.cli_index, key, value)
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	DEBUG(dInfo, "C%d Append key(%v) value(%v)\n", ck.cli_index, key, value)
	ck.PutAppend(key, value, "Append")
}
