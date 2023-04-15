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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	STATE_FOLLOWER = iota
	STATE_LEADER
	STATE_CANDIDATE
)

type LogEntry struct {
	Command interface{}
	Term    int
}

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

type RaftState struct {
	currentTerm       int
	state             int
	LastRecvHeartTime time.Time
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int
	log         []LogEntry
	state       int
	commitIndex int
	lastApplied int
	//! 在选举后重新初始化
	nextIndex  []int //对于所有节点，下一次发送的 log 的 index
	matchIndex []int //要复制给对应节点来覆盖的 log 的 index

	electionAlarm  *time.Timer
	heartBeatTimer *time.Timer
}

func (rf *Raft) isUptoDate(LastLogIndex, LastLogTerm int) bool {
	return true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	isleader = false
	// Your code here (2A).
	if rf.state == STATE_LEADER {
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogterm  int
}

func (rf *Raft) constructRequestVoteArgs() *RequestVoteArgs {
	LastLogIndex := len(rf.log) - 1
	LastLogTerm := -1
	if LastLogIndex >= 0 {
		LastLogTerm = rf.log[LastLogIndex].Term
	}
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: LastLogIndex,
		LastLogterm:  LastLogTerm,
	}
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("In Processing RequestVote, Term : %v,  CandidateId : %v\n", args.Term, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, args, reply)

	if args.Term < rf.currentTerm || (args.CandidateId != rf.voteFor && rf.currentTerm == args.Term && rf.voteFor != -1) || rf.killed() {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		//fmt.Printf("rf id : %v,currTerm : %v is lower than arg id : %v, term : %v\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.currentTerm, rf.voteFor = args.Term, -1
		rf.state = STATE_FOLLOWER
	}
	// if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isUptoDate(args.LastLogIndex, args.LastLogterm) {
	// 	println("Raft id : ", args.CandidateId, "with :", args.Term, "Get vote from ", rf.me, "in the term : ", rf.currentTerm)
	// 	reply.VoteGranted = true
	// 	rf.voteFor = args.CandidateId
	// 	rf.electionAlarm = nextElectionAlarm()
	// }
	if !rf.isUptoDate(args.LastLogIndex, args.LastLogterm) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.voteFor = args.CandidateId
	rf.electionAlarm.Reset(randTime())
	reply.Term, reply.VoteGranted = rf.currentTerm, true

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term        int
	LeaderId    int
	PreLogIndex int
	PreLogTerm  int

	Entries []int

	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	ret := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Unlock()
	return ret
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, args, reply)

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		DPrintf("rf.currentTerm changed in AppendEntries")
		rf.currentTerm, rf.voteFor = args.Term, -1
	}

	rf.state = STATE_FOLLOWER
	rf.electionAlarm.Reset(randTime())

	reply.Term, reply.Success = rf.currentTerm, true
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
//! sendRequestVote 的 args 和  reply 的参数类型必须是给定的 RequestVoteArgs， RequestVoteReply 类型
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//! labrpc 模拟有损网络，网络可能无法到达，并且请求可能丢失， Call 发送一个请求并且等待回复，如果回复未超时则返回true,否则返回 false
//! call() 返回可能延时，一个 false 可能由 server 坏死，好的server但是请求未到达，或者回复丢失
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//! Call()保证返回， 即使server端无法返回，因此无需自己实现超时Call()
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//! 如果你在RCP过程中遇到了麻烦，请检查你已经开头大写了所有的 Struct的数据成员, 并且传入 reply 时使用其引用而不是复制
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
//! 如果该节点最近没有收到心跳，则该节点需要发起选举
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionAlarm.C:
			DPrintf("electionTimer is timeout")
			rf.mu.Lock()
			rf.state = STATE_CANDIDATE
			rf.currentTerm++
			rf.election()
			rf.electionAlarm.Reset(randTime())
			rf.mu.Unlock()
		case <-rf.heartBeatTimer.C:
			DPrintf("heartBeatTimer is timeout")
			rf.mu.Lock()
			if rf.state == STATE_LEADER {
				rf.heartBeat(true)
				rf.heartBeatTimer.Reset(randheartBeatTime())
			}
			rf.mu.Unlock()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = STATE_FOLLOWER
	rf.electionAlarm = time.NewTimer(randTime())
	rf.heartBeatTimer = time.NewTimer(randheartBeatTime())
	rf.voteFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) election() {
	args := rf.constructRequestVoteArgs()
	rf.voteFor = rf.me
	votecnt := 1
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, reply, peer, args, rf.currentTerm)
				if rf.currentTerm == reply.Term && rf.state == STATE_CANDIDATE {
					if reply.VoteGranted {
						votecnt++
						if votecnt > len(rf.peers)/2 {
							rf.state = STATE_LEADER
							rf.heartBeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, reply.Term, rf.currentTerm)
						rf.state = STATE_FOLLOWER
						rf.currentTerm, rf.voteFor = reply.Term, -1

					}
				}
			}
		}(peer)
	}

}

func (rf *Raft) heartBeat(isNil bool) {
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		Entries:  nil,
	}

	for peer := range rf.peers {
		reply := new(AppendEntriesReply)
		isSucess := rf.SendAppendEntries(peer, &args, reply)
		if isSucess && reply.Success {
			rf.electionAlarm.Reset(randTime())
		} else if isSucess && !reply.Success && reply.Term > rf.currentTerm {
			DPrintf("[Node %v] find bigger leader that id : %v, term : %v", rf, peer, reply.Term)
		}
	}
}

func randTime() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration(r.Intn(200)+200)
}

func randheartBeatTime() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration(r.Intn(100)+100)
}
