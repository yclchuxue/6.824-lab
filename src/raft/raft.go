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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int  		//当前任期
	leaderId int
	
	votedFor int          

	//state string      //follower       candidate         leader

	electionRandomTimeout int
	electionElapsed 	  int

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

	if string(rf.persister.raftstate) == "leader" {
		isleader = true
	}

	term = rf.currentTerm

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
	Term int     		//候选者的任期
	CandidateId  int	//候选者的编号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool  	//投票结果,同意为true
	Term int 			//当前任期，候选者用于更新自己
}

//心跳包
type AppendEntriesArgs struct {
	Term int  			//leader任期
	LeaderId int 		//用来follower重定向到leader
}

type AppendEntriesReply struct {
	Term int 			//当前任期，leader用来更新自己
}

//
// example RequestVote RPC handler.
// 	被请求投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//待处理收到请求投票信息后是否更新超时时间
	
	//所有服务器和接收者的处理流程

	if rf.currentTerm > args.Term {       //候选者任期低于自己
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
		DEBUG(dVote, "S%d %3v vote <- %d T(%d) < cT(%d) A\n", rf.me, Mi, args.CandidateId, args.Term, rf.currentTerm)
		//log.Printf("%v %d requestvote from %d but not vote in args.Term(%d) and currentTerm(%d) A", Mi, rf.me, args.CandidateId, args.Term, rf.currentTerm)
	}else if rf.currentTerm < args.Term {      //候选者任期高于自己
		reply.VoteGranted = true
		reply.Term = args.Term

		rf.mu.Lock()
		
		Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
		// rf.votedFor = -1
		rf.votedFor = args.CandidateId
		DEBUG(dVote,"S%d %3v vote <- %d  T(%d) > cT(%d) vf(%d) A\n", rf.me, Mi, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
		//log.Printf("%v %d requestvote from %d becouse args.Term(%d) and currentTerm(%d) and votefor(%d)", Mi, rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
		//rf.
		rf.currentTerm = args.Term
		rf.persister.raftstate = []byte("follower")
		rf.leaderId = args.CandidateId
		DEBUG(dVote,"S%d %3v vote be follower cT(%d) < T(%d)\n",rf.me, Mi, rf.currentTerm, args.Term)
		//log.Printf("%d become follower because of currenTerm(%d), < T(%d)", rf.me, rf.currentTerm, args.Term)
		rf.electionElapsed = 0
		rand.Seed(time.Now().UnixNano())
		rf.electionRandomTimeout = rand.Intn(250) + 200
		rf.mu.Unlock()
	}else{
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {  //任期相同且未投票或者候选者和上次相同
			//if 日志至少和自己一样新
			rf.mu.Lock()
			rf.electionElapsed = 0
			rand.Seed(time.Now().UnixNano())
			rf.electionRandomTimeout = rand.Intn(250) + 200
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
			Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
			DEBUG(dVote,"S%d %3v voye <- %d T(%d) = cT(%d) vf(%d)\n", rf.me, Mi, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
			//log.Printf("%v %d requestvote from %d in args.Term(%d) and currentTerm(%d) and votefor(%d)", Mi, rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
			reply.VoteGranted = true
			reply.Term = args.Term
		}else{
			Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
			DEBUG(dVote,"S%d %3v vote <- %d not T(%d) = cT(%d) vf(%d)\n",rf.me, Mi, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
			//log.Printf("%v %d requestvote from %d but not vote in args.Term(%d) and currentTerm(%d) votefor(%d)", Mi, rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
			reply.VoteGranted = false
			reply.Term = args.Term
		}
	}
	// else if commitIndex > lastApplied {

	// }

	//不同身份服务器处理流程
	// switch string(rf.persister.raftstate) {
	// case "follower":
		
	// case "candidate":

	// case "leader":

}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
	DEBUG(dLeader, "S%d %3v app <- %d T(%d) cT(%d)\n",rf.me, Mi, args.LeaderId, args.Term, rf.currentTerm)
	//log.Printf("%v %d heart from %d in args.Term(%d) and currentTerm(%d)", Mi, rf.me, args.LeaderId, args.Term, rf.currentTerm)
	if args.Term >= rf.currentTerm {    //收到心跳包的任期不低于当前任期
		rf.mu.Lock()
		rf.electionElapsed = 0
		// rf.votedFor = -1
		rand.Seed(time.Now().UnixNano())
		rf.electionRandomTimeout = rand.Intn(250) + 200
		if args.Term > rf.currentTerm{
			rf.votedFor = -1
		}
		Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
		DEBUG(dLeader, "S%d %3v app vf(%d)\n", rf.me, Mi, rf.votedFor)
		//log.Printf("%d votefor(%d)",rf.me, rf.votedFor)
		rf.currentTerm = args.Term
		//if string(rf.persister.raftstate) != "follower" {
		if rf.leaderId != args.LeaderId {
			Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
			DEBUG(dLeader, "S%d %3v app be follower\n", rf.me, Mi)
			// log.Printf("%d become %d's follower!", rf.me, args.LeaderId)
		}
		rf.leaderId = args.LeaderId
		rf.persister.raftstate = []byte("follower")
		rf.mu.Unlock()
		reply.Term = args.Term
	}else{         //args.term < currentTerm
		reply.Term = rf.currentTerm
	}
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
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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

func (rf *Raft) appendentries(){

	becomefollower := int64(-1)

	Ti := int64(len(rf.peers))

	go func() {
	
	for it := range rf.peers {
		if it != rf.me {

			go func(it int) {

				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				reply := AppendEntriesReply{}
				Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
				DEBUG(dLeader, "S%d %3v app -> %d cT(%d)\n", rf.me, Mi, it, rf.currentTerm)
				//log.Printf("%v %d send to %d heart in currentTerm(%d)", Mi, rf.me, it, rf.currentTerm)
				ok := rf.sendAppendEntries(int(it), &args, &reply)
				if ok {
					if reply.Term > rf.currentTerm {
						Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
						DEBUG(dLeader, "S%d %3v app be %d's follower T(%d)\n",rf.me, Mi, -1, reply.Term)
						//log.Printf("%d become %d' follower int term(%d)", rf.me, -1, reply.Term)
						atomic.StoreInt64(&becomefollower, int64(reply.Term))
					}
				}else{
					Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
					DEBUG(dLeader, "S%d %3v app -> %d fail cT(%d)\n", rf.me, Mi, it, rf.currentTerm)
					//log.Printf("%d send heart call to %d fail int term(%d)", rf.me, it, args.Term)
				}
				atomic.AddInt64(&Ti, -1)

			}(it)

		}
	}

	atomic.AddInt64(&Ti, -1)

	}()

	for{
		if int(becomefollower) != -1 && int(becomefollower) > rf.currentTerm{
			rf.mu.Lock()
			rf.currentTerm = int(becomefollower)
			rf.votedFor = -1
			Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
			DEBUG(dLeader, "S%d %3v app be %d's follower T(%d)\n", rf.me, Mi, -1, int(becomefollower))
			//log.Printf("%d votefor(%d) and become follower", rf.me, rf.votedFor)
			rf.leaderId = -1 //int(Id)
			rf.persister.raftstate = []byte("follower")
			rf.electionElapsed = 0
			rand.Seed(time.Now().UnixNano())
			rf.electionRandomTimeout = rand.Intn(250) + 200
			rf.mu.Unlock()
			//return
		}

		if Ti == 0 {
			return
		}
	}
}


func (rf *Raft) requestvotes() {
	truenum := int64(1)
	falsenum := int64(-1)
	Ti := int64(len(rf.peers))
	peers := len(rf.peers)
	rf.mu.Lock()
	term := rf.currentTerm
	// Id := int64(0)
	rf.votedFor = rf.me
	Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
	DEBUG(dVote, "S%d %3v vote vf(%d) to own\n", rf.me, Mi, rf.votedFor)
	// log.Printf("%d votefor(%d) for own", rf.me, rf.votedFor)
	rf.mu.Unlock()

	go func() {

	for it := range rf.peers {
		if it != rf.me {

			go func(it int) {

			args  := RequestVoteArgs{}
			reply := RequestVoteReply{}
			args.CandidateId = rf.me
			args.Term = term	

			Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
			DEBUG(dVote, "S%d %3v vote -> %d cT(%d)\n", rf.me, Mi, it, term)
			ok := rf.sendRequestVote(it, &args, &reply)  //发起投票
			if ok {

				rf.mu.Lock()
				if term != rf.currentTerm {
					rf.votedFor = -1
					rf.mu.Unlock()
					Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
					DEBUG(dVote, "S%d %3v vote tT(%d) != cT(%d)\n", rf.me, Mi, term, rf.currentTerm)
					//log.Printf("%v %d term(%d) != currentTerm(%d) votefor(%d) A", Mi, rf.me, term, rf.currentTerm, rf.votedFor)
					return
				}
				
				//处理收到的票数
				if reply.VoteGranted && reply.Term == term {
					atomic.AddInt64(&truenum, 1)
				}

				if reply.Term > rf.currentTerm {
					//falsenum = reply.Term
					DEBUG(dVote, "S%d T(%d) < cT(%d)\n", rf.me, reply.Term, rf.currentTerm)
					atomic.StoreInt64(&falsenum, int64(reply.Term))
					// atomic.StoreInt64(&Id, int64(it))
				}
				rf.mu.Unlock()
			}

			atomic.AddInt64(&Ti, -1)

			}(it)

			// if truenum > peers/2 {    //同意票数过半
			// 	Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
			// 	log.Printf("%v %d have %d votes in term(%d) but currentterm(%d) A! %d", Mi, rf.me, truenum, term, rf.currentTerm, peers/2)
			// 	rf.mu.Lock()
			// 	if term == rf.currentTerm {
			// 		rf.persister.raftstate = []byte("leader")
			// 		rf.votedFor = -1
			// 		Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
			// 		log.Printf("%v %d become leader! A", Mi, rf.me)
			// 		rf.mu.Unlock()
			// 		return
			// 	}else{
			// 		rf.votedFor = -1
			// 		rf.mu.Unlock()
			// 		return
			// 	}
			// }
			// if falsenum > peers/2 {    //反对票数过半

			// 	return
			// }
		}
	}

	atomic.AddInt64(&Ti, -1)

	}()

	for {
		if  falsenum != -1{
			rf.mu.Lock()
			rf.currentTerm = int(falsenum)
			rf.leaderId = -1//int(Id)
			rf.persister.raftstate = []byte("follower")
			rf.votedFor = -1
			rf.electionElapsed = 0
			rand.Seed(time.Now().UnixNano())
			rf.electionRandomTimeout = rand.Intn(250) + 200
			rf.mu.Unlock()
			Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
			DEBUG(dVote, "S%d %3v vote T(%d) > cT(%d) be -1's follower vf(%d)\n", rf.me, Mi, term, rf.currentTerm, rf.votedFor)
			//log.Printf("%v reply.Term > rf.currentTerm and become follower votefor(%d)", Mi, rf.votedFor)
			// atomic.StoreInt64(&Id, int64(-1))
			atomic.StoreInt64(&falsenum, int64(-1))
			//return 
		}
		if int(truenum) > peers/2 {    //票数过半
			Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
			DEBUG(dVote,"S%d %3v have %d votes T(%d) cT(%d) %d B\n", rf.me, Mi, truenum, term, rf.currentTerm, peers/2)
			//log.Printf("%v %d have %d votes in term(%d) but currentterm(%d)! %d B", Mi, rf.me, truenum, term, rf.currentTerm, peers/2)
			rf.mu.Lock()
					if term == rf.currentTerm {
						rf.persister.raftstate = []byte("leader")
						// rf.votedFor = -1
						rf.mu.Unlock()
						Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
						DEBUG(dLeader, "S%d %3v be Leader B\n", rf.me, Mi)
						//log.Printf("%d become leader! B", rf.me)
						return
					}else{
						// rf.votedFor = -1
						rf.mu.Unlock()
						return
					}
		}
		if Ti == 0 {
			if int(truenum) > peers/2 {    //票数过半
				Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
				DEBUG(dVote, "S%d %3v have %d votes T(%d) cT(%d) %d D\n", rf.me, Mi, truenum, term, rf.currentTerm, peers/2)
				//log.Printf("%v %d have %d votes in term(%d) but currentterm(%d)! %d D", Mi, rf.me, truenum, term, rf.currentTerm, peers/2)
				rf.mu.Lock()
						if term == rf.currentTerm {
							rf.persister.raftstate = []byte("leader")
							// rf.votedFor = -1
							rf.mu.Unlock()
							Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
							DEBUG(dVote, "S%d %3v be Leader! B\n",rf.me, Mi)
							// log.Printf("%d become leader! B", rf.me)
						}else{
							// rf.votedFor = -1
							rf.mu.Unlock()
						}
			}else{
				Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
				DEBUG(dVote, "S%d %3v have %d votes T(%d) cT(%d) %d C\n", rf.me, Mi, truenum, term, rf.currentTerm, peers/2)
				//log.Printf("%v %d have %d votes in term(%d) but currentterm(%d)! %d C", Mi, rf.me, truenum, term, rf.currentTerm, peers/2)
			}

			return 
		}

	}


	//Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
	//log.Printf("%v %d have %d votes in term(%d) but currentterm(%d)! %d A", Mi, rf.me, truenum, term, rf.currentTerm, peers/2)
	// if int(truenum) > peers/2 {    //票数过半
	// 	Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
	// 	log.Printf("%v %d have %d votes in term(%d) but currentterm(%d)! %d B", Mi, rf.me, truenum, term, rf.currentTerm, peers/2)
	// 	rf.mu.Lock()
	// 			if term == rf.currentTerm {
	// 				rf.persister.raftstate = []byte("leader")
	// 				rf.votedFor = -1
	// 				rf.mu.Unlock()
	// 				log.Printf("%d become leader! B", rf.me)
	// 			}else{
	// 				rf.votedFor = -1
	// 				rf.mu.Unlock()
	// 			}
	// }else{
	// 	Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
	// 	log.Printf("%v %d have %d votes in term(%d) but currentterm(%d)! %d C", Mi, rf.me, truenum, term, rf.currentTerm, peers/2)
	// }
	// else if truenum == peers/2 {
	// 	rand.Seed(time.Now().UnixNano())
	// 	rf.mu.Lock()
	// 	rf.votedFor = -1
	// 	rf.currentTerm++
	// 	rf.electionElapsed = 0
	// 	rf.electionRandomTimeout = rand.Intn(150) + 200
	// 	rf.mu.Unlock()
	// }	
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//log.Printf("%v", string(rf.persister.raftstate))

		switch string(rf.persister.raftstate) {
		case "follower":
			time.Sleep(time.Millisecond)

			go func() {
				
						if rf.electionElapsed >= rf.electionRandomTimeout {
						rand.Seed(time.Now().UnixNano())
						rf.mu.Lock()
						rf.currentTerm++
						rf.votedFor = -1
						Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
						DEBUG(dTimer, "S%d %3v timeout vf(%d) be candidate\n", rf.me, Mi, rf.votedFor)
						//log.Printf("%d timeout votefor(%d) become candidate", rf.me, rf.votedFor)
						rf.persister.raftstate = []byte("candidate")        //变为候选人并发起投票
						rf.electionRandomTimeout = rand.Intn(250) + 200
						rf.electionElapsed = 0
						Mi = time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
						DEBUG(dVote, "S%d %3v vote start election cT(%d)\n", rf.me, Mi, rf.currentTerm)
						//log.Printf("%v %d start a new election in %d!",  Mi, rf.me, rf.currentTerm)
						go rf.requestvotes()
						rf.mu.Unlock()
						//continue
					}
				
			}()

			rf.mu.Lock()
			rf.electionElapsed++
			rf.mu.Unlock()
			break
		case "candidate":
			// if rf.electionElapsed == 0 {
			// 	Mi := time.Now().UnixNano() / 1e6  - time.Now().Unix()*1000
			// 	log.Printf("%v %d start a new election in %d!",  Mi, rf.me, rf.currentTerm)
			// 	go rf.requestvotes()
			// }
			time.Sleep(time.Millisecond)
			go func() {
				
					if rf.electionElapsed >= rf.electionRandomTimeout {
						rand.Seed(time.Now().UnixNano())
						rf.mu.Lock()
						rf.currentTerm++
						rf.votedFor = -1
						Mi := time.Now().UnixNano() / 1e6  //- time.Now().Unix()*1000
						DEBUG(dTimer, "S%d %06v timeout vf(%d) cT(%d)\n", rf.me, Mi, rf.votedFor, rf.currentTerm)
						//log.Printf("%d timeout votefor(%d) agin event in term(%d)", rf.me, rf.votedFor, rf.currentTerm)
						rf.electionRandomTimeout = rand.Intn(250) + 200
						rf.electionElapsed = 0
						Mi = time.Now().UnixNano() / 1e6  //- time.Now().Unix()*1000 
						DEBUG(dVote, "S%d %06v vote start election agin cT(%d)\n", rf.me, Mi, rf.currentTerm)
						//log.Printf("%v %d start a new election in %d!",  Mi, rf.me, rf.currentTerm)
						go rf.requestvotes()
						rf.mu.Unlock()
						//continue
					}
				
			}()
			rf.mu.Lock()
			rf.electionElapsed++
			rf.mu.Unlock()
			break
		case "leader":
			DEBUG(dError, "S%v HB now: %v\n", rf.me, time.Now().UnixMilli())
		
			go rf.appendentries()
			
			time.Sleep(time.Millisecond*90)
			break
	}
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
	LOGinit()
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
	rf.persister.raftstate = []byte("follower")

	//atomic.StoreInt32(&rf.dead, 0)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())  //快照

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
