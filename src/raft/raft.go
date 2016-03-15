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
	"labrpc"
	"sync"
	"time"
)

const (
	basetime = 175
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2 ; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.RWMutex
	role        uint64 //atomic
	currentTerm int    //atomic counter

	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	electionTimeout  time.Duration
	heartbeatTimeout time.Duration

	heartBeatChan chan bool

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) isLeader() (isLeader bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	isLeader = rf.role == 2
	return
}

func (rf *Raft) isCandidate() (isCandidate bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	isCandidate = rf.role == 1
	return
}

func (rf *Raft) isFollower() (isFollower bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	isFollower = rf.role == 0
	return
}

func (rf *Raft) makeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = 2
	return
}

func (rf *Raft) makeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = 1
	return
}

func (rf *Raft) makeFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = 0
	return
}

func (rf *Raft) incTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	return
}

func (rf *Raft) decTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm--
	return
}

func (rf *Raft) setTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
	return
}

func (rf *Raft) getTerm() (term int) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = rf.currentTerm
	return
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.getTerm()
	isLeader := rf.isLeader()
	// Your code here.
	return term, isLeader
}

// This is our main loop. It checks the state of the peer and drops down into the applicable loop
func (rf *Raft) run() {
	for {
		switch {
		case rf.isFollower():
			DPrintf("👍🏻Follower state it on %v", rf.me)
			rf.runFollower()
			continue
		case rf.isCandidate():
			DPrintf("✋ Candidate state hit on %v", rf.me)
			rf.runCandidate()
			continue
		case rf.isLeader():
			DPrintf("💪 leader state hit on %v", rf.me)
			rf.runLeader()
			continue
		}
	}
}

func (rf *Raft) runFollower() {
	electionTimeout := randomTimeout(rf.electionTimeout)

	select {
	case <-electionTimeout:
		DPrintf("⏲ follower %v election timed out", rf.me)
		rf.makeCandidate()
		return
	case <-rf.heartBeatChan:
		DPrintf("💖 follower %v received heartbeat", rf.me)
		return
	}
}

func (rf *Raft) runCandidate() {
	electionTimeout := randomTimeout(rf.electionTimeout)
	rf.incTerm()
	votes := rf.sendVotes()

	select {
	case <-rf.heartBeatChan:
		DPrintf("💖 Candidate %v received heartbeat", rf.me)
		rf.makeFollower()
		return
	case <-electionTimeout:
		DPrintf("⏲ Candidate %v election timed out", rf.me)
		return
	case vote := <-votes:
		DPrintf("✏️ Candidate %v recieved vote: %+v ", rf.me, vote)
		rf.makeLeader()
		return
	}
}

func (rf *Raft) sendVotes() (votes chan *RequestVoteReply) {
	others := exclude(rf.peers, rf.me)
	votes = make(chan *RequestVoteReply, len(others))
	request := rf.makeVoteRequest()
	for _, v := range others {
		go func(v int) {
			DPrintf("Requesting vote with: %+v", request)
			reply := &RequestVoteReply{}
			rf.sendRequestVote(v, request, reply)
			votes <- reply
		}(v)
	}
	return votes
}

func (rf *Raft) makeVoteRequest() RequestVoteArgs {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	request := RequestVoteArgs{"Ping"}

	DPrintf("Request constructed: %+v", request)

	return request
}

func (rf *Raft) runLeader() {
	heartbeatTimeout := randomTimeout(rf.heartbeatTimeout)

	select {
	case <-heartbeatTimeout:
		DPrintf("⏲ 💖Leader %v sent heartbeat", rf.me)
		rf.makeFollower()
		return
	case <-rf.heartBeatChan:
		DPrintf("💖 Candidate %v received heartbeat", rf.me)
		rf.makeFollower()
		return
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Test string
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Test string
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	reply.Test = "Pong"
	DPrintf("🎉 Got a vote request %+v. Replied: %+v", args, reply)
	// Your code here.
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should probably
// pass &reply.
//
// returns true if labrpc says the RPC was delivered.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail.
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

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.heartbeatTimeout = basetime * time.Millisecond
	rf.electionTimeout = 2 * basetime * time.Millisecond
	rf.makeFollower()

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
