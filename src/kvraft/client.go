package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"6.824/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	leader int
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

	args := GetArgs{}

	args.Key = key

	reply := GetReply{}

	// i := int(nrand())%(len(ck.servers))

	for i := ck.leader; i < len(ck.servers); {
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok {
			
			if reply.Err == ErrWrongLeader {
				i++
				if i == len(ck.servers){
					i = 0
				}
				//fmt.Println("Get fail key = ", key, "value = ", reply.Value)
			} else if reply.Err == OK{
				ck.leader = i
				fmt.Println("Get success key = ", key, "value = ", reply.Value)
				return reply.Value
			}else{
				ck.leader = i
				fmt.Println("Get nil key = ", key, "value = ", reply.Value)
				return reply.Value
			}
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
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
	}

	reply := PutAppendReply{}

	// i := int(nrand())%(len(ck.servers))
	
	for i := ck.leader; i < len(ck.servers); {
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			
			if reply.Err == ErrWrongLeader {
				i++
				if i == len(ck.servers){
					i = 0
				}
				//fmt.Println(args.Op, "PutAppend fail key = ", key, "value = ", args.Value)
			} else if reply.Err == OK{
				ck.leader = i
				fmt.Println(args.Op, "PutAppend success key = ", key, "value = ", args.Value)
				break
			}else{
				ck.leader = i
				fmt.Println(args.Op, "PutAppend nil key = ", key, "value = ", args.Value)
				break
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	// fmt.Println("Put key = ", key, "value = ", value)
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	// fmt.Println("Append key = ", key, "value = ", value)
	ck.PutAppend(key, value, "Append")
}
