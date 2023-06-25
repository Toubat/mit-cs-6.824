package raft

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

func (rf *Raft) onRequestVote(term int) {
	if term > rf.currentTerm {
		DPrintf("[%d] [%s] received a greater term (%d > %d), stepping down to follower", rf.self, "RequestVote", term, rf.currentTerm)
		rf.currentTerm = term
		rf.state = Follower
		rf.votedFor = Invalid
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] [%s] received vote request from [%d]", rf.self, "RequestVote", args.CandidateId)
	// Rules for Servers: All Servers (2)
	rf.onRequestVote(args.Term)

	if rf.state != Follower || args.Term < rf.currentTerm {
		DPrintf("[%d] [%s] rejected vote request from [%d]; state: %v, currentTerm: %v", rf.self, "RequestVote", args.CandidateId, rf.state, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	upToDate := rf.CheckUpToDate(args.LastLogTerm, args.LastLogIndex)
	if (rf.votedFor == Invalid || rf.votedFor == args.CandidateId) && upToDate {
		DPrintf("[%d] [%s] granted vote for [%d]", rf.self, "RequestVote", args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.resetElectionTimeout()
	} else {
		DPrintf("[%d] [%s] rejected vote request from [%d]; votedFor: %v, isUpToDate: %v", rf.self, "RequestVote", args.CandidateId, rf.votedFor, upToDate)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) onAppendEntries(term int, leaderId int) {
	if term >= rf.currentTerm {
		DPrintf("[%d] [%s] received a greater or equal term (%d >= %d), stepping down to follower", rf.self, "AppendEntries", term, rf.currentTerm)
		rf.currentTerm = term
		rf.state = Follower
		rf.votedFor = leaderId
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] [%s] received heartbeat from [%d] with term %d", rf.self, "AppendEntries", args.LeaderId, args.Term)
	// Rules for Servers: All Servers (2)
	rf.onAppendEntries(args.Term, args.LeaderId)

	if rf.state != Follower || args.Term < rf.currentTerm {
		DPrintf("[%d] [%s] rejected heartbeat from [%d]; state: %v, currentTerm: %v", rf.self, "AppendEntries", args.LeaderId, rf.state, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	DPrintf("[%d] [%s] accepted heartbeat from [%d]", rf.self, "AppendEntries", args.LeaderId)
	rf.resetElectionTimeout()

	match, conflict := rf.CheckLogMatch(args.PrevLogIndex, args.PrevLogTerm)
	if !match && !conflict {
		DPrintf("[%d] [%s] missing log entries starting from index %d", rf.self, "AppendEntries", args.PrevLogIndex)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// delete all entries starting from the conflicting entry
	if conflict {
		DPrintf("[%d] [%s] deleting log entries starting from index %d", rf.self, "AppendEntries", args.PrevLogIndex)
		rf.logEntries = rf.logEntries[:args.PrevLogIndex]
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// found matching log prefix, append new entries
	for i, entry := range args.Entries {
		rf.appendLogEntry(args.PrevLogIndex+i+1, entry)
	}

	// update commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}

	DPrintf("[%d] [%s] appended %d log entries from [%d]; updated commitIndex to %d", rf.self, "AppendEntries", len(args.Entries), args.LeaderId, rf.commitIndex)
	reply.Term = rf.currentTerm
	reply.Success = true
}
