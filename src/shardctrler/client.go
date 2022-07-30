package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu sync.Mutex
	leader    int
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
	ck.mu = sync.Mutex{}
	LOGinit()
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	args := &QueryArgs{}
	ck.cmd_index++
	// Your code here.
	args.Num = num
	args.CIndex = ck.cli_index
	args.OIndex = ck.cmd_index
	i := ck.leader
	ck.mu.Unlock()
	// try each known server.
	for i < len(ck.servers) {
		var reply QueryReply
		//  DEBUG(dClient, "C%d send to %v Query num is %v cmd_index is %v\n", ck.cli_index, i, args.Num, ck.cmd_index)
		
		ok := ck.servers[i].Call("ShardCtrler.Query", args, &reply)
		
		if ok && !reply.WrongLeader && reply.Err == OK{
			ck.mu.Lock()
			ck.leader = i
			ck.mu.Unlock()

			//  DEBUG(dClient, "C%d success Config is %v from S%v\n", ck.cli_index, reply.Config, i)
			return reply.Config
		} else {
			//  DEBUG(dClient, "C%d fail WrongLeader(%v) ERR(%v)\n", ck.cli_index, reply.WrongLeader, reply.Err)
		}
		i++
		if i == len(ck.servers) {
			i = 0
		}
		time.Sleep(100 * time.Millisecond)
	}
	return Config{}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	ck.mu.Lock()
	ck.cmd_index++
	// Your code here.
	args.Servers = servers
	args.CIndex = ck.cli_index
	args.OIndex = ck.cmd_index

	i := ck.leader
	ck.mu.Unlock()

	// try each known server.
	for i < len(ck.servers) {
		var reply JoinReply
		//  DEBUG(dClient, "C%d send to %v Join server is %v cmd_index is %v\n", ck.cli_index, i, args.Servers, ck.cmd_index)
		ok := ck.servers[i].Call("ShardCtrler.Join", args, &reply)
		
		if ok && !reply.WrongLeader && reply.Err == OK{
			ck.mu.Lock()
			ck.leader = i
			ck.mu.Unlock()

			//  DEBUG(dClient, "C%d success\n", ck.cli_index)
			return
		} else {
			//  DEBUG(dClient, "C%d fail WrongLeader(%v) ERR(%v)\n", ck.cli_index, reply.WrongLeader, reply.Err)
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

	ck.mu.Lock()

	ck.cmd_index++
	// Your code here.
	args.GIDs = gids
	args.CIndex = ck.cli_index
	args.OIndex = ck.cmd_index
	i := ck.leader

	ck.mu.Unlock()
	// try each known server.
	for i < len(ck.servers) {
		var reply LeaveReply
		//  DEBUG(dClient, "C%d send to %v Leave gids is %v cmd_index is %v\n", ck.cli_index, i, args.GIDs, ck.cmd_index)
		ok := ck.servers[i].Call("ShardCtrler.Leave", args, &reply)
		
		if ok && !reply.WrongLeader && reply.Err == OK{
			ck.mu.Lock()
			ck.leader = i
			ck.mu.Unlock()

			//  DEBUG(dClient, "C%d success\n", ck.cli_index)
			return
		} else {
			//  DEBUG(dClient, "C%d fail WrongLeader(%v) ERR(%v)\n", ck.cli_index, reply.WrongLeader, reply.Err)
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
	
	ck.mu.Lock()

	ck.cmd_index++
	// Your code here.
	args.Shard = shard
	args.CIndex = ck.cli_index
	args.OIndex = ck.cmd_index
	args.GID = gid

	i := ck.leader
	ck.mu.Unlock()
	// try each known server.
	for i < len(ck.servers) {
		var reply MoveReply
		//  DEBUG(dClient, "C%d send to %v Move shard is %v gid is %v cmd_index is %v\n", ck.cli_index, i, args.Shard, args.GID, ck.cmd_index)
		ok := ck.servers[i].Call("ShardCtrler.Move", args, &reply)

		if ok && !reply.WrongLeader && reply.Err == OK{
			ck.mu.Lock()
			ck.leader = i
			ck.mu.Unlock()

			//  DEBUG(dClient, "C%d success\n", ck.cli_index)
			return
		} else {
			//  DEBUG(dClient, "C%d fail WrongLeader(%v) ERR(%v)\n", ck.cli_index, reply.WrongLeader, reply.Err)
		}
		i++
		if i == len(ck.servers) {
			i = 0
		}

		time.Sleep(100 * time.Millisecond)
	}

}
