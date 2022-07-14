package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	// "fmt"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogNode struct {
	LogIndex int
	Logterm  int
	Log      interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int //当前任期
	leaderId    int

	votedFor int
	cond     *sync.Cond

	state int //follower0       candidate1         leader2

	electionRandomTimeout int
	electionElapsed       int

	log []LogNode

	commitIndex int

	lastApplied int

	nextIndex []int

	matchIndex []int

	X int

	snapshot []byte

	lastTerm int

	lastIndex int

	tindex int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	if rf.state == 2 {
		isleader = true
	}
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

type Per struct {
	X        int
	Term     int
	Log      []LogNode
	VotedFor int
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var Usr Per
	rf.mu.Lock()
	Usr.X = rf.X
	Usr.Log = rf.log
	SnapShot := rf.snapshot
	Usr.Term = rf.currentTerm
	Usr.VotedFor = rf.votedFor
	e.Encode(Usr)
	rf.mu.Unlock()
	data := w.Bytes()
	go rf.persister.SaveStateAndSnapshot(data, SnapShot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var Usr Per
	rf.mu.Lock()
	if d.Decode(&Usr) != nil {
		DEBUG(dWarn, "S%d labgob fail\n", rf.me)
	} else {
		DEBUG(dLog, "S%d ??? Term = %d votefor(%d) log= (%v)\n", rf.me, Usr.Term, Usr.VotedFor, Usr.Log)
		rf.currentTerm = Usr.Term
		rf.log = Usr.Log
		rf.X = Usr.X
		rf.snapshot = snapshot
		rf.lastIndex = rf.log[0].LogIndex
		rf.lastTerm = rf.log[0].Logterm
		rf.commitIndex = rf.log[0].LogIndex
		rf.lastApplied = rf.log[0].LogIndex
		DEBUG(dLog, "S%d 恢复log lastindex(%d) lastapplied(%d) commitindex(%d)\n", rf.me, rf.lastIndex, rf.lastApplied, rf.commitIndex)
		rf.votedFor = Usr.VotedFor
		rf.matchIndex[rf.me] = len(rf.log) - 1
	}
	rf.mu.Unlock()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has all info up to and including index.
// this means the service no longer needs the log through (and including) that index.
// Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	le := index - rf.X
	rf.lastIndex = index
	rf.lastTerm = rf.log[le].Logterm
	rf.snapshot = snapshot
	rf.log = rf.log[le:]
	if len(rf.log) == 0 {
		fmt.Println("ERROR in snapshot in leader")
	}
	rf.X = index
	for i := range rf.peers {
		if rf.nextIndex[i]-le <= 0 {
			rf.nextIndex[i] = len(rf.log)
		} else {
			rf.nextIndex[i] = rf.nextIndex[i] - le
		}
		DEBUG(dLog, "S%d update nextindex[%d] to %d\n", rf.me, i, rf.nextIndex[i])
	}
	if rf.commitIndex < index {
		DEBUG(dLeader, "S%d update commitindex(%d) to (%d)\n", rf.me, rf.commitIndex, index)
		rf.commitIndex = index
	}
	rf.lastApplied = index

	rf.matchIndex[rf.me] = rf.matchIndex[rf.me] - le
	DEBUG(dLog, "S%d index(%d) logindex(%d) len(%d)AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n", rf.me, index, rf.log[len(rf.log)-1].LogIndex, len(rf.log))
	rf.mu.Unlock()
	go rf.persist()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//Work string  		//请求类型
	Term        int //候选者的任期
	CandidateId int //候选者的编号

	LastLogIndex int

	LastLogIterm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool //投票结果,同意为true
	Term        int  //当前任期，候选者用于更新自己
}

//心跳包
type AppendEntriesArgs struct {
	Term     int //leader任期
	LeaderId int //用来follower重定向到leader

	PrevLogIndex int
	PrevLogIterm int
	Entries      []LogNode

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int //当前任期，leader用来更新自己
	Success bool

	Logterm        int
	Termfirstindex int
}

type SnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Log               interface{}
	Snapshot          []byte
}

type SnapShotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
// 	被请求投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//待处理收到请求投票信息后是否更新超时时间

	//所有服务器和接收者的处理流程
	rf.mu.Lock()
	if rf.currentTerm > args.Term { //候选者任期低于自己
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		DEBUG(dVote, "S%d  vote <- %d T(%d) < cT(%d) A\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	} else if rf.currentTerm <= args.Term { //候选者任期高于自己

		if rf.currentTerm < args.Term {
			rf.state = 0
			rf.currentTerm = args.Term
			rf.votedFor = -1
			go rf.persist()
		}

		if rf.votedFor == -1 || rf.votedFor == args.CandidateId { //任期相同且未投票或者候选者和上次相同
			//if 日志至少和自己一样新
			logi := len(rf.log) - 1
			if args.LastLogIterm >= rf.log[logi].Logterm {
				if args.LastLogIndex-rf.X >= logi ||
					args.LastLogIterm > rf.log[logi].Logterm {
					rf.state = 0
					reply.Term = args.Term
					rf.electionElapsed = 0
					rand.Seed(time.Now().UnixNano())
					rf.electionRandomTimeout = rand.Intn(200) + 300
					rf.votedFor = args.CandidateId
					rf.leaderId = -1
					DEBUG(dVote, "S%d  vote <- %d T(%d) = LastlogT(%d) logi(%d) lastlogindex(%d)\n", rf.me, args.CandidateId, rf.log[logi].Logterm, args.LastLogIterm, logi, args.LastLogIndex)
					reply.VoteGranted = true

				} else {
					DEBUG(dVote, "S%d  vote <- %d not lastlogIn(%d) < rf.logIn(%d) vf(%d)\n", rf.me, args.CandidateId, args.LastLogIndex, logi, rf.votedFor)

					reply.VoteGranted = false
					reply.Term = args.Term
				}
			} else {
				DEBUG(dVote, "S%d  vote <- %d not logT(%d) < rf.logT(%d) vf(%d)\n", rf.me, args.CandidateId, args.LastLogIterm, rf.log[logi].Logterm, rf.votedFor)

				reply.VoteGranted = false
				reply.Term = args.Term
			}

		} else {

			DEBUG(dVote, "S%d  vote <- %d not T(%d) = cT(%d) vf(%d)\n", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)

			reply.VoteGranted = false
			reply.Term = args.Term
		}
	}
	rf.mu.Unlock()
	go rf.persist()

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	go rf.persist()
	rf.mu.Lock()
	if len(args.Entries) != 0 {
		DEBUG(dLeader, "S%d  app <- %d T(%d) cT(%d)\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	} else {
		DEBUG(dLeader, "S%d  heart <- %d T(%d) cT(%d)\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	}

	if args.Term >= rf.currentTerm { //收到心跳包的任期不低于当前任期

		rf.electionElapsed = 0
		rand.Seed(time.Now().UnixNano())
		rf.electionRandomTimeout = rand.Intn(200) + 300

		if args.Term > rf.currentTerm {
			rf.votedFor = -1
		}

		rf.state = 0
		rf.currentTerm = args.Term
		DEBUG(dLog, "S%d YYYY\n", rf.me)
		if rf.leaderId != args.LeaderId {
			DEBUG(dLog, "S%d be follower\n", rf.me)
		}
		rf.leaderId = args.LeaderId

		logs := args.Entries

		if len(rf.log)-1 >= args.PrevLogIndex-rf.X && args.PrevLogIndex-rf.X >= 0 {
			DEBUG(dLeader, "S%d PreT(%d) LT(%d)\n", rf.me, args.PrevLogIterm, rf.log[args.PrevLogIndex-rf.X].Logterm)
			if args.PrevLogIterm == rf.log[args.PrevLogIndex-rf.X].Logterm {

				index := args.PrevLogIndex + 1 - rf.X

				for i, val := range logs {

					if len(rf.log)-1 >= index {
						DEBUG(dLog, "S%d mat(%d) index(%d) len(%d)\n", rf.me, len(rf.log)-1, index, len(rf.log))
						if rf.log[index].Logterm == val.Logterm {
							index++
						} else {
							rf.log = rf.log[:index]
							DEBUG(dLog, "S%d CCCCCCCCCCCCCCCCC\n", rf.me)
							//rf.matchIndex[rf.me] = index - 1
							rf.log = append(rf.log, logs[i:]...)
							DEBUG(dLog, "S%d A success + log(%v)\n", rf.me, logs[i:])
							//rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex
							rf.matchIndex[rf.me] = len(rf.log) - 1
							index++
							break
						}
					} else {
						rf.log = append(rf.log, logs[i:]...)
						DEBUG(dLog, "S%d B success + log(%v)\n", rf.me, logs[i:])
						//rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].LogIndex
						rf.matchIndex[rf.me] = len(rf.log)
						index++
						break
					}
				}
				reply.Success = true

				if args.LeaderCommit > rf.commitIndex {
					if rf.log[len(rf.log)-1].LogIndex <= args.LeaderCommit {
						rf.commitIndex = rf.log[len(rf.log)-1].LogIndex
					} else {
						rf.commitIndex = args.LeaderCommit
					}
					DEBUG(dCommit, "S%d update commit(%d)\n", rf.me, rf.commitIndex)

				}

			} else {

				reply.Logterm = rf.log[args.PrevLogIndex-rf.X].Logterm //冲突日志任期
				i := args.PrevLogIndex - rf.X
				for rf.log[i].Logterm == reply.Logterm {
					if i <= 1 {
						DEBUG(dWarn, "S%d j = %d\n", rf.me, i)
						break
					}
					i--
				}

				reply.Termfirstindex = rf.log[i].LogIndex + 1 //reply.Logterm任期内的第一条日志
				DEBUG(dLog, "S%d DDDDDDDDDDDDDDDDDDD\n", rf.me)
				rf.log = rf.log[:args.PrevLogIndex-rf.X] //匹配失败，删除该日志条目及其后面的日志
				reply.Success = false
				DEBUG(dLeader, "S%d AAA fail\n", rf.me)
			}
		} else { //不匹配
			if len(rf.log) < 1 {
				reply.Termfirstindex = 0 //reply.Logterm任期内的第一条日志
			} else {
				reply.Logterm = rf.log[len(rf.log)-1].Logterm //最新日志条目的任期
				i := len(rf.log) - 1
				for rf.log[i].Logterm == reply.Logterm {
					if i <= 1 {
						DEBUG(dWarn, "S%d i = %d\n", rf.me, i)
						reply.Termfirstindex = rf.log[i].LogIndex
						break
					}
					i--
					reply.Termfirstindex = rf.log[i].LogIndex + 1 //reply.Logterm任期内的第一条日志
				}

			}
			reply.Success = false
			DEBUG(dLeader, "S%d BBB fail logi(%d) pre(%d) TI(%d)\n", rf.me, len(rf.log)-1, args.PrevLogIndex-rf.X, reply.Termfirstindex)
		}
		reply.Term = args.Term
	} else { //args.term < currentTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		DEBUG(dLeader, "S%d CCC fail\n", rf.me)
		reply.Logterm = 0
	}
	go rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) InstallSnapshot(args *SnapShotArgs, reply *SnapShotReply) {

	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm && args.LeaderId == rf.leaderId && args.LastIncludedIndex > rf.log[0].LogIndex{
		if rf.log[len(rf.log)-1].LogIndex < args.LastIncludedIndex || rf.log[0].LogIndex > args.LastIncludedIndex {
			array := []LogNode{
				{
					LogIndex: args.LastIncludedIndex,
					Logterm:  args.LastIncludedTerm,
					Log:      args.Log,
				},
			}
			rf.log = array
			if len(rf.log) == 0 {
				fmt.Println("ERROR in snapshot in follower log < snap")
			}
			rf.X = args.LastIncludedIndex
			rf.matchIndex[rf.me] = rf.log[0].LogIndex
		} else {
			le := args.LastIncludedIndex - rf.log[0].LogIndex
			rf.log = rf.log[le:]
			if len(rf.log) == 0 {
				fmt.Println("ERROR in snapshot in follower log > snap")
			}
			rf.X = args.LastIncludedIndex
			for i := range rf.peers {
				//rf.matchIndex[i] = rf.matchIndex[i] - le
				if rf.nextIndex[i]-le <= 0 {
					rf.nextIndex[i] = len(rf.log)
				} else {
					rf.nextIndex[i] = rf.nextIndex[i] - le
				}
				DEBUG(dLog, "S%d update next[%d] to %d\n", rf.me, i, rf.nextIndex[i])
			}
			rf.matchIndex[rf.me] = rf.matchIndex[rf.me] - le
		}
		rf.lastTerm = args.LastIncludedTerm
		rf.lastIndex = args.LastIncludedIndex
		if rf.commitIndex < rf.lastIndex {
			DEBUG(dLeader, "S%d update commitindex(%d) to (%d)\n", rf.me, rf.commitIndex, rf.lastIndex)
			rf.commitIndex = rf.lastIndex
		}
		rf.snapshot = args.Snapshot
		// if rf.lastApplied < rf.lastIndex {
		rf.lastApplied = rf.lastIndex
		// }
		DEBUG(dLog2, "S%d aegs.Term(%d) CT(%d)\n", rf.me, args.Term, rf.currentTerm)
		DEBUG(dLog2, "S%d <- snapshot by(%d) index(%d) logindex(%d) len1(%d)AAAAAAAAAAAAAAAAAAAAAA lensnapshot(%d)\n", rf.me, args.LeaderId, args.LastIncludedIndex, rf.log[len(rf.log)-1].LogIndex, len(rf.log), len(args.Snapshot))
	} else {
		DEBUG(dLog2, "S%d <- snapshot but term(%d) < cT(%d) or leaderid(%d) != args.LeaderID(%d)\n", rf.me, args.Term, rf.currentTerm, rf.leaderId, args.LeaderId)
	}
	rf.mu.Unlock()
	go rf.persist()
}

func (rf *Raft) sendInstallSnapshot(server int, args *SnapShotArgs, reply *SnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//发送心跳包
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).

	if rf.killed() == false {

		rf.mu.Lock()
		if rf.state == 2 {
			isLeader = true

			com := LogNode{
				Logterm:  rf.currentTerm,
				Log:      command,
				LogIndex: len(rf.log) + rf.X,
			}
			DEBUG(dLog, "S%d have log\n", rf.me)
			rf.log = append(rf.log, com)
			rf.matchIndex[rf.me]++
			term = rf.currentTerm
			index = com.LogIndex
			go rf.persist()
			DEBUG(dLog, "S%d %v\n", rf.me, com)
			rf.electionElapsed = 0
			go rf.appendentries(rf.currentTerm)
		}
		//else{
		//DEBUG(dLog,"S%d is not leader\n", rf.me)
		//}
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

func (rf *Raft) appendentries(term int) {

	var wg sync.WaitGroup
	rf.mu.Lock()
	index := len(rf.log) - 1
	commit := rf.log[index].LogIndex
	t := rf.log[index].Logterm
	le := len(rf.peers)
	rf.mu.Unlock()
	wg.Add(le - 1)

	//start := time.Now()

	for it := range rf.peers {
		if it != rf.me {
			go func(it int, term int) {
				//for {
				args := AppendEntriesArgs{}
				args.Term = term
				args.LeaderId = rf.me
				rf.mu.Lock()
				if rf.nextIndex[it]-1 > index {
					fmt.Println("AAAAAAAAAAAAAAAAA", rf.nextIndex[it])
				}
				if rf.tindex != rf.log[0].LogIndex || index != len(rf.log)-1{
					rf.tindex = rf.log[0].LogIndex
					DEBUG(dLeader, "S%d appendentries error to exit because tindex or loglen changed\n", rf.me)
					rf.mu.Unlock()
					wg.Done()
					return
				}
				if index == len(rf.log)-1 {
					if commit-rf.X >= 0 && commit-rf.X < len(rf.log)-1 && rf.log[commit-rf.X].LogIndex != commit {
						DEBUG(dLeader, "S%d appendentries error to exit because log changed 2\n", rf.me)
						rf.mu.Unlock()
						wg.Done()
						return
					}
					// fmt.Println("BBBBBBBBBBBBBBBBB")
				}
				if rf.currentTerm != term || rf.state != 2 {
					DEBUG(dLeader, "S%d appendentries error to exit because term changed or not leader CT(%d) T(%d) Status(%d)\n", rf.me, rf.currentTerm, term, rf.state)
					rf.mu.Unlock()
					wg.Done()
					return
				}
				DEBUG(dLeader, "S%d rf.nextindex[%d] = %d\n", rf.me, it, rf.nextIndex[it])
				args.PrevLogIndex = rf.log[rf.nextIndex[it]-1].LogIndex
				DEBUG(dLeader, "S%d index(%d)  Pre(%d) len(%d)\n", rf.me, index, args.PrevLogIndex, len(rf.log)-1)
				DEBUG(dLeader, "S%d app -> %d next(%d) index(%d) neT(%d) cT(%d)\n", rf.me, it, rf.nextIndex[it], index, rf.log[args.PrevLogIndex-rf.X].Logterm, term)

				args.PrevLogIterm = rf.log[rf.nextIndex[it]-1].Logterm

				if len(rf.log)-1 >= rf.nextIndex[it] {
					nums := rf.log[rf.log[rf.nextIndex[it]].LogIndex-rf.X : commit-rf.X+1]
					args.Entries = append(args.Entries, nums...)
				}

				//附加commitIndex，让follower应用日志
				args.LeaderCommit = rf.commitIndex

				iter := it
				rf.mu.Unlock()
				reply := AppendEntriesReply{}

				ok := rf.sendAppendEntries(iter, &args, &reply)

				// start := time.Now()
				if ok {
					rf.mu.Lock()
					if reply.Success {

						successnum := 0

						//统计复制成功的个数，超过半数就提交（修改commitindex）

						rf.matchIndex[it] = index
						if rf.tindex == rf.log[0].LogIndex {
							DEBUG(dLog, "S%d update nextindex[%d](%d) to %d success\n", rf.me, it, rf.nextIndex[it], commit - rf.X + 1)
							rf.nextIndex[it] = commit - rf.X + 1 //index + 1
						}else{
							if commit >= rf.log[0].LogIndex {
								DEBUG(dLog, "S%d update nextindex[%d](%d) to %d success?\n", rf.me, it, rf.nextIndex[it], commit - rf.log[0].LogIndex + 1)
								rf.nextIndex[it] = commit - rf.log[0].LogIndex + 1
							}else{
								DEBUG(dLog, "S%d update nextindex[%d](%d) to %d success 1\n", rf.me, it, rf.nextIndex[it], 1)
								rf.nextIndex[it] = 1
							}
							
						}

						for _, in := range rf.matchIndex {
							if in == index {
								successnum++
							}
						}

						if successnum > le/2 && index > rf.commitIndex-rf.X && rf.currentTerm == t {
							DEBUG(dLog, "S%d sum(%d) ban(%d)\n", rf.me, successnum, le/2)
							DEBUG(dCommit, "S%d new commit(%d) and applied\n", rf.me, index)
							rf.commitIndex = commit
						}

					} else {
						if reply.Term > rf.currentTerm {
							DEBUG(dLeader, "S%d  app be %d's follower T(%d)\n", rf.me, -1, reply.Term)
							rf.state = 0
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.leaderId = -1 //int(Id)
							go rf.persist()
							rf.electionElapsed = 0
							rand.Seed(time.Now().UnixNano())
							rf.electionRandomTimeout = rand.Intn(200) + 300
						} else if rf.state == 2 {
							if reply.Logterm >= 0 {
								DEBUG(dLog, "S%d to %d 匹配失败 tfi(%d)\n", rf.me, it, reply.Termfirstindex)

								//跳过整个冲突任期----可能需要判断该index是否存在
								if reply.Termfirstindex < rf.log[0].LogIndex { //跟随者日志index小于leader的第一条日志index，发快照同步。
									DEBUG(dLog2, "S%d send snapShot to %d\n", rf.me, it)
									go rf.sendsnapshot(rf.currentTerm, it)
								} else if reply.Termfirstindex-rf.X > 1 {
									DEBUG(dLeader, "S%d update nextindex[%d](%d) to X(%d) > 1\n", rf.me, it, rf.nextIndex[it], reply.Termfirstindex - rf.X)
									rf.nextIndex[it] = reply.Termfirstindex - rf.X
								} else {
									DEBUG(dLog, "S%d update nextindex[%d](%d) to %d <= 1\n", rf.me, it, rf.nextIndex[it], 1)
									rf.nextIndex[it] = 1
								}
							} else {
								DEBUG(dLog, "S%d reply.logterm == 0\n", rf.me)
							}
						}
					}
					rf.mu.Unlock()
				} else {
					DEBUG(dLog, "S%d -> %d app fail\n", rf.me, it)
				}

				wg.Done()
			}(it, term)
		}
	}

	wg.Wait()
}

func (rf *Raft) sendsnapshot(term, it int) {

	args := SnapShotArgs{}
	reply := SnapShotReply{}
	rf.mu.Lock()
	args.Term = term
	args.LastIncludedIndex = rf.lastIndex
	args.LastIncludedTerm = rf.lastTerm
	args.Log = rf.log[0].Log
	args.Snapshot = rf.snapshot
	args.LeaderId = rf.me
	DEBUG(dLog, "S%d sendsnap be %d's follower T(%d) lensnapshot(%d)\n", rf.me, -1, reply.Term, len(rf.snapshot))
	rf.mu.Unlock()
	ok := rf.sendInstallSnapshot(it, &args, &reply)

	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.state = 0
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.leaderId = -1 //int(Id)
			go rf.persist()
			rf.electionElapsed = 0
			rand.Seed(time.Now().UnixNano())
			rf.electionRandomTimeout = rand.Intn(200) + 300
		}
		rf.mu.Unlock()
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) requestvotes(term int) {

	rf.mu.Lock()
	truenum := int64(1)
	peers := len(rf.peers)
	rf.votedFor = rf.me
	DEBUG(dVote, "S%d  vote vf(%d) to own\n", rf.me, rf.votedFor)
	var wg sync.WaitGroup

	wg.Add(len(rf.peers) - 1)

	rf.mu.Unlock()

	for it := range rf.peers {
		if it != rf.me {

			go func(it int, term int) {
				args := RequestVoteArgs{}
				reply := RequestVoteReply{}
				args.CandidateId = rf.me
				args.Term = term
				rf.mu.Lock()
				index := len(rf.log) - 1
				args.LastLogIndex = rf.log[index].LogIndex
				args.LastLogIterm = rf.log[index].Logterm

				rf.mu.Unlock()

				DEBUG(dVote, "S%d  vote -> %d cT(%d)\n", rf.me, it, term)
				ok := rf.sendRequestVote(it, &args, &reply) //发起投票

				if ok {
					rf.mu.Lock()
					if term != rf.currentTerm {

						DEBUG(dVote, "S%d  vote tT(%d) != cT(%d)\n", rf.me, term, rf.currentTerm)

					} else if rf.state == 1 {

						//处理收到的票数
						if reply.VoteGranted && reply.Term == term {
							atomic.AddInt64(&truenum, 1)
						}

						if atomic.LoadInt64(&truenum) > int64(peers/2) { //票数过半

							rf.state = 2
							rf.electionElapsed = 0
							rf.electionRandomTimeout = 90

							DEBUG(dVote, "S%d  have %d votes T(%d) cT(%d) %d B\n", rf.me, truenum, term, rf.currentTerm, peers/2)
							rf.matchIndex[rf.me] = len(rf.log) - 1

							for i := 0; i < len(rf.peers); i++ {
								DEBUG(dLog, "S%d update nextindex[%d](%d) to %d (len(log))\n", rf.me, it, rf.nextIndex[it], len(rf.log))
								rf.nextIndex[i] = len(rf.log)
								if i != rf.me {
									rf.matchIndex[i] = 1
								}
							}
							go rf.appendentries(rf.currentTerm)
							DEBUG(dLeader, "S%d  be Leader B\n", rf.me)

						}

						if reply.Term > rf.currentTerm {
							rf.state = 0
							rf.currentTerm = reply.Term
							rf.leaderId = -1
							rf.votedFor = -1
							rf.electionElapsed = 0
							rand.Seed(time.Now().UnixNano())
							rf.electionRandomTimeout = rand.Intn(200) + 300
							DEBUG(dVote, "S%d vote T(%d) > cT(%d) be -1's follower vf(%d)\n", rf.me, term, rf.currentTerm, rf.votedFor)
						}
					}
					rf.mu.Unlock()
				} else {
					DEBUG(dVote, "S%d vote -> %d fail\n", rf.me, it)
				}
				go rf.persist()

				wg.Done()
			}(it, term)

		}
	}

	wg.Wait()

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	//var start time.Time
	//start = time.Now()
	for rf.killed() == false {

		rf.mu.Lock()

		if rf.electionElapsed >= rf.electionRandomTimeout {
			rand.Seed(time.Now().UnixNano())
			rf.electionRandomTimeout = rand.Intn(200) + 300
			rf.electionElapsed = 0
			if rf.state == 2 {
				rf.electionRandomTimeout = 90
				go rf.persist()
				go rf.appendentries(rf.currentTerm)
			} else {
				rf.currentTerm++
				rf.state = 1
				rf.votedFor = -1
				go rf.persist()
				go rf.requestvotes(rf.currentTerm)
			}
		}

		rf.electionElapsed++

		rf.mu.Unlock()
		time.Sleep(time.Millisecond)
		//ti := time.Since(start).Milliseconds()
		//log.Printf("S%d AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d\n", rf.me, ti)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.leaderId = -1
	rf.currentTerm = 0
	rf.electionElapsed = 0
	rand.Seed(time.Now().UnixNano())
	rf.electionRandomTimeout = rand.Intn(200) + 300
	rf.state = 0
	rf.cond = sync.NewCond(&rf.mu)
	rf.log = []LogNode{}
	rf.X = 0

	rf.log = append(rf.log, LogNode{
		Logterm: 0,
	})

	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.tindex = 0

	go func() {
		rf.mu.Lock()
		startindex := rf.log[0].LogIndex
		DEBUG(dLog2, "S%d i = 1 MMMMMMMMMMMMMMMM\n", rf.me)
		rf.mu.Unlock()
		i := 1

		for rf.killed() == false {

			rf.mu.Lock()
			if startindex < rf.log[0].LogIndex {
				node := ApplyMsg{
					CommandValid:  false,
					SnapshotValid: true,
					Snapshot:      rf.snapshot,
					SnapshotTerm:  rf.lastTerm,
					SnapshotIndex: rf.lastIndex,
				}
				DEBUG(dLog2, "S%d snapshot to applymsg lastindex(%d)\n", rf.me, node.SnapshotIndex)
				startindex = rf.log[0].LogIndex
				rf.lastApplied = rf.lastIndex

				rf.mu.Unlock()
				applyCh <- node
			} else {
				rf.mu.Unlock()
			}

			rf.mu.Lock()
			var arry []LogNode
			commit := rf.commitIndex - rf.X
			applied := rf.lastApplied - rf.X
			DEBUG(dCommit, "S%d commit(%d) applied(%d) lenlog(%d) rf.X(%d) in(%d)\n", rf.me, commit, applied, len(rf.log)-1, rf.X, i)
			if commit > applied && applied >= 0 && commit <= len(rf.log)-1 {
				arry = rf.log[applied+1 : commit+1]
			}
			i++
			rf.mu.Unlock()
			if commit > applied {
				for _, it := range arry {

					node := ApplyMsg{
						CommandValid: true,
						CommandIndex: it.LogIndex,
						Command:      it.Log,
					}
					DEBUG(dLog, "S%d lastapp lognode = %v\n", rf.me, node)
					rf.mu.Lock()
					rf.lastApplied++
					DEBUG(dLog, "S%d comm(%d) last(%d)\n", rf.me, commit, rf.lastApplied)
					rf.mu.Unlock()
					applyCh <- node
				}

				go rf.persist()
			}

			time.Sleep(time.Millisecond * 10)
		}
	}()

	// go func() {
	// 	for {
	// 		rf.mu.Lock()

	// 		if rf.commitIndex < rf.log[len(rf.log)-1].LogIndex {
	// 			rf.electionElapsed = 0
	// 			go rf.appendentries(rf.currentTerm)
	// 		}
	// 		rf.mu.Unlock()
	// 		time.Sleep(time.Microsecond * 10)
	// 	}
	// }()

	LOGinit()
	//atomic.StoreInt32(&rf.dead, 0)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot()) //快照

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
