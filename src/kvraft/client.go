package kvraft

import (
	"crypto/rand"
	"fmt"
	"sync"
	//"time"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	mu        sync.Mutex
	leader    int
	ice       bool
	cli_index int64
	cmd_index int64
	//send_try  int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.mu = sync.Mutex{}
	LOGinit()
	ck.cli_index = nrand()
	fmt.Println("have one client made num", ck.cli_index)
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
	ck.mu.Lock()
	DEBUG(dClient, "S%d Get key(%v)\n", ck.cli_index, key)
	DEBUG(dClient, "S%d cmd_index(%v)1\n", ck.cli_index, ck.cmd_index)
	ck.cmd_index = ck.cmd_index + 1
	DEBUG(dClient, "S%d cmd_index(%v)2\n", ck.cli_index, ck.cmd_index)
	send_try := 3
	test := 0
	args := GetArgs{
		Key:    key,
		CIndex: ck.cli_index,
		OIndex: ck.cmd_index,
		Test:   test,
	}
	i := ck.leader
	reply := GetReply{}
	le := len(ck.servers)
	// i := int(nrand())%(len(ck.servers))
	ck.mu.Unlock()

	for i < le {
		args.Test++
		DEBUG(dClient, "S%d send to S%v Get key(%v) Cindex(%v) OIndex(%v) test(%v)\n", ck.cli_index, i, args.Key, args.CIndex, args.OIndex, args.Test)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		DEBUG(dClient, "S%d ok(%v) ERR(%v) from S%v in test%v\n", ck.cli_index, ok, reply.Err, i, args.Test)
		ck.mu.Lock()
		if ok {
			// send_try = 3
			if reply.Err == ErrWrongLeader {
				reply = GetReply{}
				if ck.ice {
					DEBUG(dClient, "S%d Get fail key(%v) value(%v) from(S%v)\n", ck.cli_index, args.Key, reply.Value, i)
				}
				i++
				if i == len(ck.servers) {
					i = 0
				}
			} else if reply.Err == OK {
				ck.ice = true
				// if ck.ice {
				// 	ti := time.Since(start).Milliseconds()
				// 	fmt.Println("time = ", ti)
				// 	ck.ice = false
				// }
				ck.leader = i
				DEBUG(dClient, "S%d Get success key(%v) value(%v) r.index(%v) from(S%v)\n", ck.cli_index, args.Key, reply.Value, reply.Index, i)
				ck.mu.Unlock()
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.ice = true
				ck.leader = i
				DEBUG(dClient, "S%d Get nil key(%v) value(%v) r.index(%v) from(S%v)\n", ck.cli_index, args.Key, reply.Value, reply.Index, i)
				ck.mu.Unlock()
				return reply.Value
			} else if reply.Err == TOUT {
				ck.leader = i
				if send_try > 0 {
					send_try--
				}else{
					i++
					send_try = 3
					if i == le {
						i = 0
					}
				}
				DEBUG(dClient, "S%d Get nil because %v from(S%v)\n", ck.cli_index, reply.Err, i)
			}
		} else {
			DEBUG(dClient, "S%d ERROR Get Key(%v) from(S%v)\n", ck.cli_index, args.Key, i)
			i++
			if i == le {
				i = 0
			}
		}
		ck.mu.Unlock()
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
	ck.mu.Lock()
	DEBUG(dClient, "S%d cmd_index(%v)3\n", ck.cli_index, ck.cmd_index)
	ck.cmd_index = ck.cmd_index + 1
	DEBUG(dClient, "S%d cmd_index(%v)4\n", ck.cli_index, ck.cmd_index)
	send_try := 3
	test := 0
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		CIndex: ck.cli_index,
		OIndex: ck.cmd_index,
		Test:   test,
	}
	i := ck.leader
	le := len(ck.servers)
	// i := int(nrand())%(len(ck.servers))
	ck.mu.Unlock()
	for i < le {
		args.Test++
		DEBUG(dClient, "S%d send to S%v %v Key(%v) value(%v) CIndex(%v) OIndex(%v) test%v\n", ck.cli_index, i, args.Op, args.Key, args.Value, args.CIndex, args.OIndex, args.Test)
		reply := PutAppendReply{}
		//fmt.Println("i = ", i)
		// var start time.Time
		// start = time.Now()
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		// ti := time.Since(start).Milliseconds()
		// fmt.Println("time = ", ti)
		DEBUG(dClient, "S%d ok(%v) ERR(%v) from S%v in test%v\n", ck.cli_index, ok, reply.Err, i, args.Test)
		ck.mu.Lock()
		if ok {
			// send_try = 3
			if reply.Err == ErrWrongLeader {
				reply = PutAppendReply{}
				if ck.ice {
					DEBUG(dClient, "S%d %v err(%v) fail key(%v) value(%v) r.index(%v) from(S%v)1\n", ck.cli_index, args.Op, reply.Err, key, args.Value, reply.Index, i)
				}
				i++
				if i == len(ck.servers) {
					i = 0
				}
			} else if reply.Err == OK {
				ck.leader = i
				ck.ice = true
				// if ck.ice {
				// 	ti := time.Since(start).Milliseconds()
				// 	fmt.Println("time = ", ti)
				// 	ck.ice = false
				// }
				DEBUG(dClient, "S%d %v err(%v), success key(%v) value(%v) r.Index(%v) from(S%v)2\n", ck.cli_index, args.Op, reply.Err, key, args.Value, reply.Index, i)
				ck.mu.Unlock()
				break
			} else if reply.Err == TOUT {
				ck.ice = true
				
				if send_try > 0 {
					send_try--
				}else{
					i++
					send_try = 3
					if i == le {
						i = 0
					}
				}
				ck.leader = i
				DEBUG(dClient, "S%d %v err(%v) nil key(%v) value(%v) r.index(%v) from(S%v)3\n", ck.cli_index, args.Op, reply.Err, key, args.Value, reply.Index, i)
			}
		} else {
			DEBUG(dClient, "S%d ERROR %v key(%v) value(%v) from(S%v)\n", ck.cli_index, args.Op, args.Key, args.Value, i)
			i++
			if i == le {
				i = 0
			}
		}
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	DEBUG(dClient, "S%d Put key(%v) value(%v)\n", ck.cli_index, key, value)
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	DEBUG(dClient, "S%d Append key(%v) value(%v)\n", ck.cli_index, key, value)
	ck.PutAppend(key, value, "Append")
}
