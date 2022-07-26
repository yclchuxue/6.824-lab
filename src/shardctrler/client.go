package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	leader int
	cli_index int64
	cmd_index int64
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
	ck.cli_index = nrand()
	ck.cmd_index = 0
	LOGinit()
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	ck.cmd_index++
	// Your code here.
	args.Num = num
	args.CIndex = ck.cli_index
	args.OIndex = ck.cmd_index
	for {
		i := ck.leader
		// try each known server.
		for ; i < len(ck.servers); {
			var reply QueryReply
			ok := ck.servers[i].Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = i
				return reply.Config
			}
		}
		i++
		if i == len(ck.servers) {
			i = 0
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	ck.cmd_index++	
	// Your code here.
	args.Servers = servers
	args.CIndex = ck.cli_index
	args.OIndex = ck.cmd_index
	for {
		i := ck.leader
		// try each known server.
		for ; i < len(ck.servers); {
			var reply JoinReply
			ok := ck.servers[i].Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = i
				return
			}
		}
		i++
		if i == len(ck.servers) {
			i = 0
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	ck.cmd_index++
	// Your code here.
	args.GIDs = gids
	args.CIndex = ck.cli_index
	args.OIndex = ck.cmd_index
	for {
		i := ck.leader
		// try each known server.
		for  ; i < len(ck.servers); {
			var reply LeaveReply
			ok := ck.servers[i].Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = i
				return
			}
		}
		i++
		if i == len(ck.servers) {
			i = 0
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		i := ck.leader
		// try each known server.
		for ; i < len(ck.servers); {
			var reply MoveReply
			ok := ck.servers[i].Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.leader = i
				return
			}
		}
		i++
		if i == len(ck.servers) {
			i = 0
		}
		time.Sleep(100 * time.Millisecond)
	}
}
