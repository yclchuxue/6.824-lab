package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut	   = "ErrTimeOut"
	ErrWrongNum	   = "ErrWrongNum"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   	string
	Value 	string
	Op    	string // "Put" or "Append"
	Shard 	int
	CIndex 	int64
	OIndex 	int64
	Num    	int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key 	string
	Shard  	int
	CIndex 	int64
	OIndex 	int64
	Num 	int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type GetConfigReply struct {
	Err 	Err
	Shard 	int
	The_num int
	Kvs_num int
	Csm     map[int64]int64
	Cdm		map[int64]int64
	Kvs 	map[string]string
}

type GetConfigArgs struct {
	The_num int
	Num 	int
	Shard 	int
	SIndex 	int
	GIndex 	int
}