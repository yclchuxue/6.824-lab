package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	CIndex	int64
	OIndex  int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Index int
	Err Err
}

type GetArgs struct {
	Key string
	CIndex int64
	OIndex int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Index int
	Err   Err
	Value string
}
