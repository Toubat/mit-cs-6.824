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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Toubat/mit-cs-6.824/labrpc"
)

// import "bytes"
// import "labgob"

const MinElectionTimeout = 350 * time.Millisecond
const MaxElectionTimeout = 550 * time.Millisecond
const ElectionTickInterval = 10 * time.Millisecond
const HeartbeatInterval = 120 * time.Millisecond
const CommitIndexInterval = 50 * time.Millisecond
const ApplyLogInterval = 50 * time.Millisecond

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
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
	Term    int
	Command interface{}
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
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// Other state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	self            int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	applyCh         chan ApplyMsg       // channel to send committed messages to kvserver
	applyCond       *sync.Cond          // condition variable to notify changes of commitIndex
	state           State               // current server state
	electionTimeout time.Time           // next election timeout time
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) GetLastLogIndex() int {
	return len(rf.logEntries) - 1
}

func (rf *Raft) GetLastLogTerm() int {
	prevLogIndex := rf.GetLastLogIndex()
	if prevLogIndex >= 0 {
		return rf.logEntries[prevLogIndex].Term
	}
	return Invalid
}

func (rf *Raft) GetPrevLogIndex(i int) int {
	return rf.nextIndex[i] - 1
}

func (rf *Raft) GetPrevLogTerm(i int) int {
	prevLogIndex := rf.GetPrevLogIndex(i)
	if prevLogIndex >= 0 {
		return rf.logEntries[prevLogIndex].Term
	}
	return Invalid
}

func (rf *Raft) CheckUpToDate(term int, index int) bool {
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

// return whether the log entry at a given index is consistent with the given term
// the first return value is whether the log entry matches the term
// the second return value is whether a conflict is found
func (rf *Raft) CheckLogMatch(prevLogIndex int, prevLogTerm int) (bool, bool) {
	// leader has empty log
	if prevLogIndex < 0 {
		return true, false
	}

	// leader's log is longer than follower's log
	if prevLogIndex > rf.GetLastLogIndex() {
		return false, false
	}

	matchTerm := rf.logEntries[prevLogIndex].Term == prevLogTerm
	return matchTerm, !matchTerm
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

func (rf *Raft) appendLogEntry(index int, entry LogEntry) {
	if index < len(rf.logEntries) {
		rf.logEntries[index] = entry
		return
	}

	if index != len(rf.logEntries) {
		log.Fatalf("index %d is not the next index", index)
	}
	rf.logEntries = append(rf.logEntries, entry)
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
	go rf.refreshCommitIndex()
	go rf.applyCommittedEntries()
}

func (rf *Raft) startElectionTimer() {
	for {
		rf.mu.Lock()
		timeout := time.Now().After(rf.electionTimeout)
		shouldStartElection := rf.state != Leader && timeout
		rf.mu.Unlock()

		if shouldStartElection {
			DPrintf("[%d] starting a new election...", rf.self)
			go rf.startElection()
		}

		time.Sleep(ElectionTickInterval)
	}
}

func (rf *Raft) applyCommittedEntries() {
	for {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.logEntries[rf.lastApplied]

			// apply the command to the state machine
			DPrintf("[%d] applying log entry %d...", rf.self, rf.lastApplied)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1,
			}
			DPrintf("[%d] log entry %d has been applied", rf.self, rf.lastApplied)
		}
		rf.mu.Unlock()

		time.Sleep(ApplyLogInterval)
	}
}

func (rf *Raft) heartbeat() {
	for {
		rf.mu.Lock()
		isLeader := rf.state == Leader
		rf.mu.Unlock()

		if isLeader {
			go rf.sendHeartbeat()
		}

		time.Sleep(HeartbeatInterval)
	}
}

// periodically check if commitIndex can be advanced, and update commitIndex
func (rf *Raft) refreshCommitIndex() {
	for {
		rf.updateCommitIndex()

		time.Sleep(CommitIndexInterval)
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] [%v] updating commit index...", rf.self, rf.state)
	DPrintf("[%d] [%v] current commit index: %d", rf.self, rf.state, rf.commitIndex)
	DPrintf("[%d] [%v] current last applied: %d", rf.self, rf.state, rf.lastApplied)
	DPrintf("[%d] [%v] current logs: %v", rf.self, rf.state, rf.logEntries)

	// find the largest N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm
	N, lo, hi := rf.commitIndex, rf.commitIndex+1, rf.GetLastLogIndex()
	for lo <= hi {
		mid := lo + (hi-lo)/2

		matchCount := 0
		for _, idx := range rf.matchIndex {
			if idx >= mid {
				matchCount++
			}
		}

		if matchCount < len(rf.peers)/2+1 { // no majority, go left
			hi = mid - 1
		} else if rf.logEntries[mid].Term < rf.currentTerm { // older term, go right
			lo = mid + 1
		} else if rf.logEntries[mid].Term > rf.currentTerm { // newer term, go left
			DPrintf("[%d] [ERROR] found a newer term %d > %d while updating commitIndex", rf.self, rf.logEntries[mid].Term, rf.currentTerm)
			hi = mid - 1
		} else { // found a feasible commitIndex, record and go right
			N = mid
			lo = mid + 1
		}
	}

	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.applyCond.Broadcast()
	}

	if rf.state == Leader {
		// print nextIndex & matchIndex
		DPrintf("[%d] [%v] nextIndex: %v", rf.self, rf.state, rf.nextIndex)
		DPrintf("[%d] [%v] matchIndex: %v", rf.self, rf.state, rf.matchIndex)
	}

	DPrintf("[%d] [%v] updated commit index: %d", rf.self, rf.state, rf.commitIndex)
	DPrintf("[%d] [%v] updated last applied: %d", rf.self, rf.state, rf.lastApplied)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.self
	rf.resetElectionTimeout()

	// prepare RequestVoteArgs for RPC
	self := rf.self
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.self,
		LastLogIndex: rf.GetLastLogIndex(),
		LastLogTerm:  rf.GetLastLogTerm(),
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
			DPrintf("[%d] sending RequestVote to [%d]", self, server)
			ok := rf.sendRequestVote(server, &requestVoteArgs, &requestVoteReply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// handle RequestVote response
			if requestVoteReply.Term > rf.currentTerm {
				DPrintf("[%d] received higher term from [%d], becoming follower", self, server)
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
	DPrintf("[%d] waiting for votes...", rf.self)
	for voteCount < len(rf.peers)/2+1 && finishedCount < len(rf.peers) {
		c.Wait()
	}
	DPrintf("[%d] votes: %d, finished: %d", rf.self, voteCount, finishedCount)

	// received majority of votes, become leader
	if voteCount >= len(rf.peers)/2+1 && rf.state == Candidate {
		DPrintf("[%d] received majority of votes, becoming leader", rf.self)
		rf.state = Leader
		rf.resetIndex()
		rf.matchIndex[rf.self] = rf.GetLastLogIndex()
	}
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	// prepare AppendEntriesArgs for RPC
	self := rf.self
	appendEntriesArgList := make([]AppendEntriesArgs, len(rf.peers))
	for i := range rf.peers {
		if i == self {
			continue
		}

		appendEntriesArgList[i] = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.self,
			PrevLogIndex: rf.GetPrevLogIndex(i),
			PrevLogTerm:  rf.GetPrevLogTerm(i),
			Entries:      rf.logEntries[rf.nextIndex[i]:],
			LeaderCommit: rf.commitIndex,
		}
		DPrintf("[%d] sending heartbeat to [%d]; new enteries: %v", self, i, appendEntriesArgList[i].Entries)
	}
	rf.mu.Unlock()

	// send AppendEntries RPCs to all other servers
	for i := range rf.peers {
		if i == self {
			continue
		}

		go func(server int, appendEntriesArgs *AppendEntriesArgs) {
			appendEntriesReply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, appendEntriesArgs, &appendEntriesReply)
			if !ok {
				DPrintf("[%d] heartbeat to [%d] failed", self, server)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// handle AppendEntries response
			if appendEntriesReply.Term > rf.currentTerm {
				DPrintf("[%d] received higher term from [%d], becoming follower", self, server)
				rf.currentTerm = appendEntriesReply.Term
				rf.state = Follower
				return
			}

			if appendEntriesReply.Success {
				DPrintf("[%d] heartbeat acknowledged by [%d]", self, server)
				// update nextIndex and matchIndex
				rf.matchIndex[server] = appendEntriesArgs.PrevLogIndex + len(appendEntriesArgs.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			} else {
				DPrintf("[%d] heartbeat rejected by [%d]", self, server)
				// optimize decrement nextIndex process to bypass a term per RPC
				conflictTerm := appendEntriesReply.ConflictTerm
				termIndex := appendEntriesReply.TermIndex
				prevLogIndex := appendEntriesArgs.PrevLogIndex
				nextIndex := termIndex

				// server is not in follower state
				if conflictTerm == Invalid && termIndex == Invalid {
					return
				}

				// missing log entries
				if conflictTerm == Invalid {
					rf.nextIndex[server] = termIndex
					return
				}

				// bypass a term if possible
				for nextIndex < prevLogIndex && rf.logEntries[nextIndex].Term == conflictTerm {
					nextIndex++
				}
				rf.nextIndex[server] = nextIndex
			}
		}(i, &appendEntriesArgList[i])
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.logEntries) + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if isLeader {
		DPrintf("[%d] received command %v", rf.self, command)
		rf.logEntries = append(rf.logEntries, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.nextIndex[rf.self] = index
		rf.matchIndex[rf.self] = index - 1
	}

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
		commitIndex: Invalid,
		lastApplied: Invalid,
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
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
