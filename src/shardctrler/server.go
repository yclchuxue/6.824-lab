package shardctrler

import (
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const TIMEOUT = 1000 * 100

type Op struct {
	// Your data here.
	Ser_index int64
	Cli_index int64
	Cmd_index int64
	Operate   string
	Num       int
	Servers   map[int][]string
	Gids      []int
	Shard     int
	Gid       int
}

type COMD struct {
	index int
	O     Op
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// Your data here.

	CSM map[int64]int64
	CDM map[int64]int64

	query chan COMD
	join  chan COMD
	leave chan COMD
	move  chan COMD

	applyindex int

	configs []Config // indexed by config num
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	in1, okk1 := sc.CDM[args.OIndex]
	if okk1 && in1 == args.OIndex {
		reply.Err = OK
		DEBUG(dLog, "S%d had done Join servers = %v\n", sc.me, args.Servers)
		reply.Err = OK
		sc.mu.Unlock()
		return
	} else if !okk1 {
		sc.CDM[args.CIndex] = 0
	}
	sc.mu.Unlock()

	var index int
	O := Op{
		Ser_index: int64(sc.me),
		Cli_index: args.CIndex,
		Cmd_index: args.OIndex,
		Operate:   "Join",
		Servers:   args.Servers,
	}
	sc.mu.Lock()
	in2, okk2 := sc.CSM[args.CIndex]
	if !okk2 {
		sc.CSM[args.CIndex] = 0
	}
	sc.mu.Unlock()

	if in2 == args.OIndex {
		_, isLeader = sc.rf.GetState()
		index = -1
	} else {
		index, _, isLeader = sc.rf.Start(O)
		// reply.index = index
	}

	if !isLeader {
		reply.WrongLeader = true
	} else {
		OS := sc.rf.Find(index)
		if OS == nil {
			DEBUG(dLeader, "S%d do not have this log(%v)\n", sc.me, O)
		} else {
			P := OS.(Op)
			DEBUG(dLeader, "S%d have this log(%v) in raft\n", sc.me, P)
		}

		sc.mu.Lock()
		lastindex, ok := sc.CSM[args.CIndex]
		if !ok {
			sc.CSM[args.CIndex] = 0
		}
		sc.CSM[args.CIndex] = args.OIndex
		sc.mu.Unlock()

		DEBUG(dLeader, "S%d <-- C%v Join servers(%v) wait index(%v) %v\n", sc.me, args.CIndex, args.Servers, index)
		for {
			select {
			case out := <-sc.join:
				if index <= out.index { // && out.O == O
					sc.mu.Lock()
					DEBUG(dLeader, "S%d index(%v) from(%v) index(%v) out.index(%v)\n", sc.me, index, sc.me, index, out.index)
					reply.Err = OK
					sc.mu.Unlock()
					return
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				sc.mu.Lock()
				DEBUG(dLeader, "S%d is time out\n", sc.me)
				reply.Err = "TIME OUT"
				if _, isLeader := sc.rf.GetState(); !isLeader {
					reply.WrongLeader = true
					// reply.Index = index
					sc.CSM[args.CIndex] = lastindex
				}
				sc.mu.Unlock()
				return

			}
		}
	}

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	in1, okk1 := sc.CDM[args.OIndex]
	if okk1 && in1 == args.OIndex {
		reply.Err = OK
		DEBUG(dLog, "S%d had done Leave GIDS = %v\n", sc.me, args.GIDs)
		reply.Err = OK
		sc.mu.Unlock()
		return
	} else if !okk1 {
		sc.CDM[args.CIndex] = 0
	}
	sc.mu.Unlock()

	var index int
	O := Op{
		Ser_index: int64(sc.me),
		Cli_index: args.CIndex,
		Cmd_index: args.OIndex,
		Operate:   "Leave",
		Gids:      args.GIDs,
	}
	sc.mu.Lock()
	in2, okk2 := sc.CSM[args.CIndex]
	if !okk2 {
		sc.CSM[args.CIndex] = 0
	}
	sc.mu.Unlock()

	if in2 == args.OIndex {
		_, isLeader = sc.rf.GetState()
		index = -1
	} else {
		index, _, isLeader = sc.rf.Start(O)
		// reply.index = index
	}

	if !isLeader {
		reply.WrongLeader = true
	} else {
		OS := sc.rf.Find(index)
		if OS == nil {
			DEBUG(dLeader, "S%d do not have this log(%v)\n", sc.me, O)
		} else {
			P := OS.(Op)
			DEBUG(dLeader, "S%d have this log(%v) in raft\n", sc.me, P)
		}

		sc.mu.Lock()
		lastindex, ok := sc.CSM[args.CIndex]
		if !ok {
			sc.CSM[args.CIndex] = 0
		}
		sc.CSM[args.CIndex] = args.OIndex
		sc.mu.Unlock()

		DEBUG(dLeader, "S%d <-- C%v Leave GIDS(%v) wait index(%v) %v\n", sc.me, args.CIndex, args.GIDs, index)
		for {
			select {
			case out := <-sc.leave:
				if index <= out.index { // && out.O == O
					sc.mu.Lock()
					DEBUG(dLeader, "S%d index(%v) from(%v) index(%v) out.index(%v)\n", sc.me, index, sc.me, index, out.index)
					reply.Err = OK
					sc.mu.Unlock()
					return
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				sc.mu.Lock()
				DEBUG(dLeader, "S%d is time out\n", sc.me)
				reply.Err = "TIME OUT"
				if _, isLeader := sc.rf.GetState(); !isLeader {
					reply.WrongLeader = true
					// reply.Index = index
					sc.CSM[args.CIndex] = lastindex
				}
				sc.mu.Unlock()
				return

			}
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	in1, okk1 := sc.CDM[args.OIndex]
	if okk1 && in1 == args.OIndex {
		reply.Err = OK
		DEBUG(dLog, "S%d had done Move GID = %v\n", sc.me, args.GID)
		reply.Err = OK
		sc.mu.Unlock()
		return
	} else if !okk1 {
		sc.CDM[args.CIndex] = 0
	}
	sc.mu.Unlock()

	var index int
	O := Op{
		Ser_index: int64(sc.me),
		Cli_index: args.CIndex,
		Cmd_index: args.OIndex,
		Operate:   "Move",
		Gid:       args.GID,
	}
	sc.mu.Lock()
	in2, okk2 := sc.CSM[args.CIndex]
	if !okk2 {
		sc.CSM[args.CIndex] = 0
	}
	sc.mu.Unlock()

	if in2 == args.OIndex {
		_, isLeader = sc.rf.GetState()
		index = -1
	} else {
		index, _, isLeader = sc.rf.Start(O)
		// reply.index = index
	}

	if !isLeader {
		reply.WrongLeader = true
	} else {
		OS := sc.rf.Find(index)
		if OS == nil {
			DEBUG(dLeader, "S%d do not have this log(%v)\n", sc.me, O)
		} else {
			P := OS.(Op)
			DEBUG(dLeader, "S%d have this log(%v) in raft\n", sc.me, P)
		}

		sc.mu.Lock()
		lastindex, ok := sc.CSM[args.CIndex]
		if !ok {
			sc.CSM[args.CIndex] = 0
		}
		sc.CSM[args.CIndex] = args.OIndex
		sc.mu.Unlock()

		DEBUG(dLeader, "S%d <-- C%v Move GID(%v) wait index(%v) %v\n", sc.me, args.CIndex, args.GID, index)
		for {
			select {
			case out := <-sc.move:
				if index <= out.index { // && out.O == O
					sc.mu.Lock()
					DEBUG(dLeader, "S%d index(%v) from(%v) index(%v) out.index(%v)\n", sc.me, index, sc.me, index, out.index)
					reply.Err = OK
					sc.mu.Unlock()
					return
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				sc.mu.Lock()
				DEBUG(dLeader, "S%d is time out\n", sc.me)
				reply.Err = "TIME OUT"
				if _, isLeader := sc.rf.GetState(); !isLeader {
					reply.WrongLeader = true
					// reply.Index = index
					sc.CSM[args.CIndex] = lastindex
				}
				sc.mu.Unlock()
				return

			}
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	in1, okk1 := sc.CDM[args.OIndex]
	if okk1 && in1 == args.OIndex {
		reply.Err = OK
		DEBUG(dLog, "S%d had done Query num = %v\n", sc.me, args.Num)
		if args.Num >= 0 {
			reply.Config = sc.configs[args.Num]
		} else if args.Num == -1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		}
		sc.mu.Unlock()
		return
	} else if !okk1 {
		sc.CDM[args.CIndex] = 0
	}
	sc.mu.Unlock()

	var index int
	O := Op{
		Ser_index: int64(sc.me),
		Cli_index: args.CIndex,
		Cmd_index: args.OIndex,
		Operate:   "Query",
		Num:       args.Num,
	}
	sc.mu.Lock()
	in2, okk2 := sc.CSM[args.CIndex]
	if !okk2 {
		sc.CSM[args.CIndex] = 0
	}
	sc.mu.Unlock()

	if in2 == args.OIndex {
		_, isLeader = sc.rf.GetState()
		index = -1
	} else {
		index, _, isLeader = sc.rf.Start(O)
		// reply.index = index
	}

	if !isLeader {
		reply.WrongLeader = true
	} else {
		OS := sc.rf.Find(index)
		if OS == nil {
			DEBUG(dLeader, "S%d do not have this log(%v)\n", sc.me, O)
		} else {
			P := OS.(Op)
			DEBUG(dLeader, "S%d have this log(%v) in raft\n", sc.me, P)
		}

		sc.mu.Lock()
		lastindex, ok := sc.CSM[args.CIndex]
		if !ok {
			sc.CSM[args.CIndex] = 0
		}
		sc.CSM[args.CIndex] = args.OIndex
		sc.mu.Unlock()

		DEBUG(dLeader, "S%d <-- C%v Query num is %v wait index(%v) %v\n", sc.me, args.CIndex, args.Num, index)
		for {
			select {
			case out := <-sc.query:
				if index <= out.index { // && out.O == O
					sc.mu.Lock()
					DEBUG(dLeader, "S%d index(%v) from(%v) index(%v) out.index(%v)\n", sc.me, index, sc.me, index, out.index)
					if args.Num >= 0 {
						reply.Config = sc.configs[args.Num]
					} else if args.Num == -1 {
						reply.Config = sc.configs[len(sc.configs)-1]
					}
					sc.mu.Unlock()
					return
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				sc.mu.Lock()
				DEBUG(dLeader, "S%d is time out\n", sc.me)
				reply.Err = "TIME OUT"
				if _, isLeader := sc.rf.GetState(); !isLeader {
					reply.WrongLeader = true
					// reply.Index = index
					sc.CSM[args.CIndex] = lastindex
				}
				sc.mu.Unlock()
				return

			}
		}
	}

}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.CSM = make(map[int64]int64)
	sc.CDM = make(map[int64]int64)
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.query = make(chan COMD)
	sc.leave = make(chan COMD)
	sc.join = make(chan COMD)
	sc.move = make(chan COMD)
	// Your code here.

	LOGinit()
	labgob.Register(Op{})
	sc.applyindex = 0

	go func() {
		for {

			select {
			case m := <-sc.applyCh:

				if m.CommandValid {
					sc.mu.Lock()
					O := m.Command.(Op)
					DEBUG(dLog, "S%d TTT CommandValid(%v) applyindex(%v) CommandIndex(%v) %v CDM[C%v](%v) M(%v) from(%v)\n", sc.me, m.CommandValid, sc.applyindex, m.CommandIndex, O.Operate, O.Cli_index, sc.CDM[O.Cli_index], O.Cmd_index, O.Ser_index)

					if sc.applyindex+1 == m.CommandIndex {
						if O.Cli_index == -1 {
							DEBUG(dLog, "S%d for TIMEOUT update applyindex %v to %v\n", sc.me, sc.applyindex, m.CommandIndex)
							sc.applyindex = m.CommandIndex
						} else if sc.CDM[O.Cli_index] < O.Cmd_index {
							DEBUG(dLeader, "S%d update CDM[%v] from %v to %v update applyindex %v to %v\n", sc.me, O.Cli_index, sc.CDM[O.Cli_index], O.Cmd_index, sc.applyindex, m.CommandIndex)
							sc.applyindex = m.CommandIndex

							sc.CDM[O.Cli_index] = O.Cmd_index
							if O.Operate == "Join" {

								select {
								case sc.join <- COMD{index: m.CommandIndex, O: O}:
									DEBUG(dLog, "S%d write join in(%v)\n", sc.me, m.CommandIndex)
								default:
									DEBUG(dLog, "S%d can not write join in(%v)\n", sc.me, m.CommandIndex)
								}
								// le := len(sc.configs)
								// sc.configs = append(sc.configs, Config{
								// 	Num: le,
								// })
								// le_shard := len(sc.configs[le-1].Shards)
								// le_group := len(sc.configs[le-1].Groups)
								// sc.configs[le].Shards[le_shard] = 

								NewCongig := sc.JoinConfig(O.Servers)
								
							} else if O.Operate == "Leave" {

								select {
								case sc.leave <- COMD{index: m.CommandIndex, O: O}:
									DEBUG(dLog, "S%d write putAdd in(%v)\n", sc.me, m.CommandIndex)
								default:
									DEBUG(dLog, "S%d can not write putAdd in(%v)\n", sc.me, m.CommandIndex)
								}

								
							} else if O.Operate == "Query" {

								select {
								case sc.query <- COMD{index: m.CommandIndex, O: O}:
									DEBUG(dLog, "S%d write get in(%v)\n", sc.me, m.CommandIndex)
								default:
									DEBUG(dLog, "S%d can not write get in(%v)\n", sc.me, m.CommandIndex)
								}

							} else if O.Operate == "Move" {
								select {
								case sc.move <- COMD{index: m.CommandIndex, O: O}:
									DEBUG(dLog, "S%d write get in(%v)\n", sc.me, m.CommandIndex)
								default:
									DEBUG(dLog, "S%d can not write get in(%v)\n", sc.me, m.CommandIndex)
								}
							}
						} else if sc.CDM[O.Cli_index] == O.Cmd_index {
							DEBUG(dLog2, "S%d this cmd had done, the log had two update applyindex %v to %v\n", sc.me, sc.applyindex, m.CommandIndex)
							sc.applyindex = m.CommandIndex
						}
					} else if sc.applyindex+1 < m.CommandIndex {
						DEBUG(dWarn, "S%d the applyindex + 1 (%v) < commandindex(%v)\n", sc.me, sc.applyindex, m.CommandIndex)
						// sc.applyindex = m.CommandIndex
					}
					sc.mu.Unlock()
				}

			case <-time.After(TIMEOUT * time.Microsecond):
				O := Op{
					Ser_index: int64(sc.me),
					Cli_index: -1,
					Cmd_index: -1,
					Operate:   "TIMEOUT",
				}
				sc.rf.Start(O)
			}

		}

	}()

	return sc
}

func (sc *ShardCtrler) JoinConfig(servers map[int][]string) Config {
	len_configs := len(sc.configs)
	newNUm := sc.configs[len_configs-1].Num + 1


}

func (sc *ShardCtrler) MoveConfig(){

}

func (sc *ShardCtrler) LeaveConfig(){

}

func (sc *ShardCtrler) QueryConfig(){

}