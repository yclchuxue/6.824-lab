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
	// "log"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	Logterm int
	Log     interface{}
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

	state int //follower0       candidate1         leader2

	electionRandomTimeout int
	electionElapsed       int

	log []LogNode

	commitIndex int

	lastApplied int

	nextIndex []int

	matchIndex []int

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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
		//log.Printf("%v %d requestvote from %d but not vote in args.Term(%d) and currentTerm(%d) A",   rf.me, args.CandidateId, args.Term, rf.currentTerm)
	} else if rf.currentTerm < args.Term { //候选者任期高于自己

		if args.LastLogIterm >= rf.log[rf.matchIndex[rf.me]].Logterm {
			if args.LastLogIndex >= rf.matchIndex[rf.me] {
				reply.VoteGranted = true
				reply.Term = args.Term

				rf.votedFor = args.CandidateId
				DEBUG(dVote, "S%d  vote <- %d  T(%d) > cT(%d) vf(%d) A\n", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)

				rf.currentTerm = args.Term
				rf.state = 0
				rf.leaderId = -1
				DEBUG(dVote, "S%d  vote be -1' follower cT(%d) < T(%d)\n", rf.me, rf.currentTerm, args.Term)

				rf.electionElapsed = 0
				rand.Seed(time.Now().UnixNano())
				rf.electionRandomTimeout = rand.Intn(250) + 200
			} else {
				DEBUG(dVote, "S%d  vote <- %d not logIn(%d) < rf.logIn(%d) vf(%d)\n", rf.me, args.CandidateId, args.LastLogIndex, rf.matchIndex[rf.me], rf.votedFor)

				reply.VoteGranted = false
				reply.Term = args.Term
			}
		} else {
			DEBUG(dVote, "S%d  vote <- %d not logT(%d) < rf.logT(%d) vf(%d)\n", rf.me, args.CandidateId, args.LastLogIterm, rf.log[rf.matchIndex[rf.me]].Logterm, rf.votedFor)

			reply.VoteGranted = false
			reply.Term = args.Term
		}

	} else {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId { //任期相同且未投票或者候选者和上次相同
			//if 日志至少和自己一样新
			if args.LastLogIterm >= rf.log[rf.matchIndex[rf.me]].Logterm {
				if args.LastLogIndex >= rf.matchIndex[rf.me] {
					rf.electionElapsed = 0
					rand.Seed(time.Now().UnixNano())
					rf.electionRandomTimeout = rand.Intn(250) + 200
					rf.votedFor = args.CandidateId

					DEBUG(dVote, "S%d  voye <- %d T(%d) = cT(%d) vf(%d)\n", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)

					reply.VoteGranted = true
					reply.Term = args.Term
				} else {
					DEBUG(dVote, "S%d  vote <- %d not logIn(%d) < rf.logIn(%d) vf(%d)\n", rf.me, args.CandidateId, args.LastLogIndex, rf.matchIndex[rf.me], rf.votedFor)

					reply.VoteGranted = false
					reply.Term = args.Term
				}
			} else {
				DEBUG(dVote, "S%d  vote <- %d not logT(%d) < rf.logT(%d) vf(%d)\n", rf.me, args.CandidateId, args.LastLogIterm, rf.log[rf.matchIndex[rf.me]].Logterm, rf.votedFor)

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

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DEBUG(dLeader, "S%d  app <- %d T(%d) cT(%d)\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	//log.Printf("%v %d heart from %d in args.Term(%d) and currentTerm(%d)",   rf.me, args.LeaderId, args.Term, rf.currentTerm)
	if args.Term >= rf.currentTerm { //收到心跳包的任期不低于当前任期

		rf.electionElapsed = 0
		rand.Seed(time.Now().UnixNano())
		rf.electionRandomTimeout = rand.Intn(250) + 200

		if args.Term > rf.currentTerm {
			rf.votedFor = -1
		}

		DEBUG(dLeader, "S%d  app vf(%d)\n", rf.me, rf.votedFor)
		rf.currentTerm = args.Term
		if rf.leaderId != args.LeaderId {

			DEBUG(dLeader, "S%d  app be follower\n", rf.me)
		}
		rf.leaderId = args.LeaderId
		rf.state = 0
		//DEBUG(dLog, "S%d T(%d) index(%d) oT(%d) oin(%d) %d\n",rf.me, args.PrevLogIterm, args.PrevLogIndex, rf.log[rf.matchIndex[rf.me]].Logterm, rf.matchIndex[rf.me] , len(args.Entries))
		if len(args.Entries) != 0 {
			if rf.matchIndex[rf.me] >= args.PrevLogIndex {
				if args.PrevLogIterm == rf.log[args.PrevLogIndex].Logterm {

					for _, value := range args.Entries {
						rf.log = append(rf.log, value)
						rf.matchIndex[rf.me]++
					}
					reply.Success = true
					DEBUG(dLog, "S%d success\n", rf.me)
				} else {
					reply.Success = false
				}
			} else { //不匹配
				reply.Success = false
			}
		} else {
			reply.Success = true
		}

		if args.LeaderCommit > rf.commitIndex {
			if rf.matchIndex[rf.me] < args.LeaderCommit {
				rf.commitIndex = rf.matchIndex[rf.me]
			}else{
				rf.commitIndex = args.LeaderCommit
			}
		}
		reply.Term = args.Term
	} else { //args.term < currentTerm
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	rf.mu.Unlock()
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
	rf.mu.Lock()
	if rf.state == 2 {
		isLeader = true

		com := LogNode{
			Logterm: rf.currentTerm,
			Log:     command,
		}

		rf.log = append(rf.log, com)
		rf.matchIndex[rf.me]++
		term = rf.currentTerm
		index = rf.matchIndex[rf.me]

		go rf.appendentries(rf.currentTerm, index)
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

func (rf *Raft) appendentries(term int, index int) {

	var wg sync.WaitGroup

	successnum := 1

	len := len(rf.peers)

	wg.Add(len - 1)

	for it := range rf.peers {
		if it != rf.me {
			go func(it int, term int) {
				for {
					args := AppendEntriesArgs{}
					args.Term = term
					args.LeaderId = rf.me
					rf.mu.Lock()
					if index != 0 {
						args.PrevLogIndex = rf.nextIndex[it] - 1
						//DEBUG(dLog, "S%d args.PrevIndex = %d, next(%d) index(%d)\n", rf.me, args.PrevLogIndex, rf.nextIndex[it], index)
						args.PrevLogIterm = rf.log[args.PrevLogIndex].Logterm

						args.Entries = rf.log[rf.nextIndex[it] : index+1]
					}

					//附加commitIndex，让follower应用日志
					args.LeaderCommit = rf.commitIndex


					rf.mu.Unlock()
					reply := AppendEntriesReply{}

					ok := rf.sendAppendEntries(it, &args, &reply)

					rf.mu.Lock()
					if ok {
						if reply.Success {
							rf.matchIndex[it] = index //更新follower的最新日志位置
							rf.nextIndex[it] = index+1
							//统计复制成功的个数，超过半数就提交（修改commitindex）
							if successnum != 0 && index == args.PrevLogIndex{
								successnum++
								if successnum > len/2 {
									DEBUG(dCommit,"S%d new commit(%d)\n", rf.me, index)
									rf.commitIndex = index
								}
							}
							rf.mu.Unlock()
							break
						} else {
							if reply.Term > rf.currentTerm {
								DEBUG(dLeader, "S%d  app be %d's follower T(%d)\n", rf.me, -1, reply.Term)
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								rf.leaderId = -1 //int(Id)
								rf.state = 0
								rf.electionElapsed = 0
								rand.Seed(time.Now().UnixNano())
								rf.electionRandomTimeout = rand.Intn(250) + 200
								rf.mu.Unlock()
								break
							} else {
								DEBUG(dLog, "S%d 匹配失败\n", rf.me)
								rf.nextIndex[it]--
							}
						}
					} else {
						DEBUG(dWarn, "S%d -> %d app fail\n", rf.me, it)
						rf.mu.Unlock()
						break
					}
					rf.mu.Unlock()
				}
				wg.Done()
			}(it, term)
		}
	}

	wg.Wait()

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

	rf.mu.Unlock()

	var wg sync.WaitGroup

	wg.Add(len(rf.peers) - 1)

	for it := range rf.peers {
		if it != rf.me {

			go func(it int) {
				args := RequestVoteArgs{}
				reply := RequestVoteReply{}
				args.CandidateId = rf.me
				args.Term = term
				rf.mu.Lock()

				args.LastLogIndex = rf.matchIndex[rf.me]
				args.LastLogIterm = rf.log[rf.matchIndex[rf.me]].Logterm

				rf.mu.Unlock()

				DEBUG(dVote, "S%d  vote -> %d cT(%d)\n", rf.me, it, term)
				ok := rf.sendRequestVote(it, &args, &reply) //发起投票
				rf.mu.Lock()
				if ok {

					if term != rf.currentTerm {

						DEBUG(dVote, "S%d  vote tT(%d) != cT(%d)\n", rf.me, term, rf.currentTerm)

					} else if rf.state == 1 {

						//处理收到的票数
						if reply.VoteGranted && reply.Term == term {
							atomic.AddInt64(&truenum, 1)
						}

						if atomic.LoadInt64(&truenum) > int64(peers/2) { //票数过半
							DEBUG(dVote, "S%d  have %d votes T(%d) cT(%d) %d B\n", rf.me, truenum, term, rf.currentTerm, peers/2)
							//log.Printf("%v %d have %d votes in term(%d) but currentterm(%d)! %d B",   rf.me, truenum, term, rf.currentTerm, peers/2)

							rf.state = 2
							rf.electionElapsed = 0
							rf.electionRandomTimeout = 90
							go rf.appendentries(rf.currentTerm, 0)
							DEBUG(dLeader, "S%d  be Leader B\n", rf.me)

						}

						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.leaderId = -1
							rf.state = 0
							rf.votedFor = -1
							rf.electionElapsed = 0
							rand.Seed(time.Now().UnixNano())
							rf.electionRandomTimeout = rand.Intn(250) + 200
							DEBUG(dVote, "S%d vote T(%d) > cT(%d) be -1's follower vf(%d)\n", rf.me, term, rf.currentTerm, rf.votedFor)
						}
					}
				} else {
					DEBUG(dVote, "S%d vote -> %d fail\n", rf.me, it)
				}
				rf.mu.Unlock()
				wg.Done()
			}(it)

		}
	}

	wg.Wait()

	// rf.mu.Lock()

	// if atomic.LoadInt64(&truenum) > int64(peers/2) && rf.state == 1 { //票数过半

	// 	DEBUG(dVote, "S%d  have %d votes T(%d) cT(%d) %d D\n", rf.me, truenum, term, rf.currentTerm, peers/2)
	// 	//log.Printf("%v %d have %d votes in term(%d) but currentterm(%d)! %d D",   rf.me, truenum, term, rf.currentTerm, peers/2)
	// 	if term == rf.currentTerm {
	// 		rf.electionElapsed = 0
	// 		rf.state = 2
	// 		rf.electionRandomTimeout = 90
	// 		go rf.appendentries(rf.currentTerm, 0)
	// 		DEBUG(dVote, "S%d  be Leader! B\n", rf.me)
	// 	}
	// } else {
	// 	DEBUG(dVote, "S%d  have %d votes T(%d) cT(%d) %d C\n", rf.me, truenum, term, rf.currentTerm, peers/2)
	// }
	// rf.mu.Unlock()

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		//start := time.Now()
		rf.mu.Lock()

		if rf.electionElapsed >= rf.electionRandomTimeout {
			rand.Seed(time.Now().UnixNano())
			rf.electionRandomTimeout = rand.Intn(250) + 200
			rf.electionElapsed = 0
			if rf.state == 2 {
				rf.electionRandomTimeout = 90

				go rf.appendentries(rf.currentTerm, 0)
			} else {
				rf.currentTerm++
				rf.state = 1
				rf.votedFor = -1
				go rf.requestvotes(rf.currentTerm)
			}
		}

		//start := time.Now()
		//ti := time.Since(start).Milliseconds()
		//log.Printf("S%d AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d", rf.me, ti)
		//DEBUG(dLog, "S%d SB time = %d\n", rf.me, rf.electionElapsed)

		rf.electionElapsed++

		rf.mu.Unlock()
		//start := time.Now()
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
	rf.electionRandomTimeout = rand.Intn(250) + 200
	rf.state = 0

	rf.log = []LogNode{}

	rf.log = append(rf.log, LogNode{
		Logterm: 0,
	})

	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	for _, it := range rf.nextIndex {
		fmt.Println(it)
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	LOGinit()
	//atomic.StoreInt32(&rf.dead, 0)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState()) //快照

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// func (rf *Raft) appendentries(term int, index int) {

// 	var wg sync.WaitGroup

// 	wg.Add(len(rf.peers) - 1)

// 	for it := range rf.peers {
// 		if it != rf.me {

// 			go func(it int, term int) {

// 				args := AppendEntriesArgs{}
// 				args.Term = term
// 				args.LeaderId = rf.me
// 				reply := AppendEntriesReply{}

// 				DEBUG(dLeader, "S%d  app -> %d cT(%d)\n", rf.me, it, term)

// 				ok := rf.sendAppendEntries(it, &args, &reply)
// 				rf.mu.Lock()
// 				if ok {
// 					if reply.Term > rf.currentTerm {

// 						DEBUG(dLeader, "S%d  app be %d's follower T(%d)\n", rf.me, -1, reply.Term)
// 						rf.currentTerm = reply.Term
// 						rf.votedFor = -1
// 						rf.leaderId = -1 //int(Id)
// 						rf.state = 0
// 						rf.electionElapsed = 0
// 						rand.Seed(time.Now().UnixNano())
// 						rf.electionRandomTimeout = rand.Intn(250) + 200
// 					}
// 				} else {

// 					DEBUG(dLeader, "S%d  app -> %d fail cT(%d)\n", rf.me, it, term)
// 					//log.Printf("%d send heart call to %d fail int term(%d)", rf.me, it, args.Term)
// 				}
// 				rf.mu.Unlock()
// 				//}

// 				wg.Done()

// 			}(it, term)

// 		}
// 	}

// 	wg.Wait()
// }
