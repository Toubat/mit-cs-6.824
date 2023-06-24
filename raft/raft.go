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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Toubat/mit-cs-6.824/labrpc"
)

// import "bytes"
// import "labgob"

const MinElectionTimeout = 200 * time.Millisecond
const MaxElectionTimeout = 400 * time.Millisecond

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const Invalid = -1

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term int
}

type LogEntries []LogEntry

// A Go object implementing a single Raft peer.
type Raft struct {
	// Persistent state on all servers
	currentTerm int
	votedFor    int
	logEntries  LogEntries

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Other state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	self            int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	applyCh         chan ApplyMsg       // channel to send committed messages to kvserver
	state           State               // current server state
	electionTimeout time.Time           // next election timeout time
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) isUpToDate(term int, index int) bool {
	l := rf.logEntries

	if index == -1 {
		return len(l) == 0
	}

	if len(l) == 0 {
		return true
	}

	// Check if the last entry of each log is different
	if l[len(l)-1].Term != term {
		return term > l[len(l)-1].Term
	}

	return index+1 >= len(l)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

func (rf *Raft) resetElectionTimeout() {
	randDuration := time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout))) + MinElectionTimeout
	rf.electionTimeout = time.Now().Add(randDuration)
}

func (rf *Raft) resetIndex() {
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logEntries)
		rf.matchIndex[i] = Invalid
	}
}

// restore previously persisted state.
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

func (rf *Raft) run() {
	go rf.startElectionTimer()
	go rf.heartbeat()
}

func (rf *Raft) startElectionTimer() {
	for {
		rf.mu.Lock()
		timeout := time.Now().After(rf.electionTimeout)
		shouldStartElection := rf.state != Leader && timeout
		rf.mu.Unlock()

		if shouldStartElection {
			go rf.startElection()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) heartbeat() {
	// TODO: implement heartbeat
	// ...
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.self
	rf.resetElectionTimeout()

	// prepare RequestVoteArgs for RPC
	lastLogIndex := len(rf.logEntries) - 1
	lastLogTerm := Invalid
	if lastLogIndex >= 0 {
		lastLogTerm = rf.logEntries[lastLogIndex].Term
	}

	self := rf.self
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.self,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	// send RequestVote RPCs to all other servers
	voteCount := 1
	finishedCount := 1
	c := sync.NewCond(&rf.mu)
	for i := range rf.peers {
		if i == self {
			continue
		}

		go func(server int) {
			requestVoteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// handle RequestVote response
			if requestVoteReply.Term > rf.currentTerm {
				rf.currentTerm = requestVoteReply.Term
				rf.state = Follower
			}

			if requestVoteReply.VoteGranted {
				voteCount++
			}
			finishedCount++
			c.Broadcast()
		}(i)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// wait for a majority of votes
	for voteCount < len(rf.peers)/2+1 && finishedCount < len(rf.peers) {
		c.Wait()
	}

	// received majority of votes, become leader
	if voteCount >= len(rf.peers)/2+1 && rf.state == Candidate {
		rf.state = Leader
		rf.resetIndex()
	}
}

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) onRequestVote(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		rf.votedFor = Invalid
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rules for Servers: All Servers (2)
	rf.onRequestVote(args.Term)

	if rf.state != Follower || args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if (rf.votedFor == Invalid || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      LogEntries // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) onAppendEntries(term int, leaderId int) {
	if term >= rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		rf.votedFor = leaderId
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Rules for Servers: All Servers (2)
	rf.onAppendEntries(args.Term, args.LeaderId)

	if rf.state != Follower || args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	if len(args.Entries) == 0 {
		return
	}

	// TODO: implement log replication
	// ...
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, self int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		currentTerm: 0,
		votedFor:    Invalid,
		logEntries:  make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		peers:       peers,
		persister:   persister,
		applyCh:     applyCh,
		self:        self,
		mu:          sync.Mutex{},
		state:       Follower,
		dead:        0,
	}
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
