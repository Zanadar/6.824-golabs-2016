package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, IsLeader)
//   start agreement on a new log entry
// rf.GetState() (term, IsLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const baseTime = 75

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

type LogEntry struct {
	command string
	term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg
	resetCh   chan bool
	killCh    chan bool

	electionTimer *time.Ticker
	heartbeat     *time.Ticker
	// Persist these
	currentTerm int // increases monotonically
	votedFor    int
	log         []LogEntry //maybe create a LogEntry struct?

	//Volatile state on all servers
	commitIndex int // index of Highest entry known to be committed (initialize at zero)
	lastApplied int

	//Volatile state on leaders. Reinitialized after an election
	nextIndex  []int // per server, initialized to leader last log index + 1
	matchIndex []int // per server, index of highest log entry known to be replciated on server

	IsLeader bool
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var IsLeader bool

	rf.mu.Lock()
	term = rf.currentTerm
	IsLeader = rf.IsLeader
	rf.mu.Unlock()
	return term, IsLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)

}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here.
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term         int
	VotedGranted bool
	// Your data here.
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("Vote request %+v", args)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VotedGranted = false
		reply.Term = rf.currentTerm
	} else if rf.votedFor == -1 {
		rf.mu.Lock()
		// rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID // updated who you last voted for
		rf.IsLeader = false
		rf.mu.Unlock()
		reply.VotedGranted = true
	}
	return
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	// DPrintf("Vote requested from peer %d------=============", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) resetElecTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimer.Stop()
	rf.electionTimer = time.NewTicker(getRandDuration(true))
}

type AppendEntriesArgs struct {
	Term         int //leaders term
	LeaderID     int //to redirect requests to the leader
	PrevLogIndex int
	PrevLogTerm  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("AppendEntries request %+v received by %+v", args, rf.me)
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		go func() { rf.resetCh <- true }()
		rf.resetElecTimer()
		rf.mu.Lock()
		rf.IsLeader = false
		rf.currentTerm = args.Term
		reply.Success = true
		rf.mu.Unlock()
	} else {
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) holdVote() {
	replies := make(chan *RequestVoteReply, len(rf.peers)-1)
	DPrintf("Server %d called for a vote		  ✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️", rf.me)
	rf.mu.Lock()
	rf.IsLeader = false
	rf.currentTerm++
	rf.mu.Unlock()
	rf.resetElecTimer()
	votes := make([]*RequestVoteReply, 0)
	tally := 1 // server votes for itself

	args := RequestVoteArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.lastApplied}
	for peer, _ := range rf.peers {
		if peer != rf.me {
			go func(i int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				DPrintf("Vote reply okay:::::::: %+v", reply)
				for !ok {
					ok = rf.sendRequestVote(i, args, reply)
				}
				replies <- reply
			}(peer)
		}
	}
	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-replies
		if reply.VotedGranted == true {
			tally++
		}
		votes = append(votes, reply)
	}

	majority := (len(rf.peers) - 1) / 2
	if len(votes) >= majority {
		rf.mu.Lock()
		rf.IsLeader = true
		rf.mu.Unlock()
		DPrintf("👏👏👏👏👏👏👏👏👏👏👏👏👏👏👏👏		Leader elected its term is %v", rf.currentTerm)
		DPrintf("I'm the boss %+v 👺👺👺👺👺👺👺👺👺👺👺👺👺👺👺👺👺👺👺    ", rf.me)
		rf.startHeartBeats()
	} else {
		rf.mu.Lock()
		rf.IsLeader = false
		rf.currentTerm--
		rf.mu.Lock()
		DPrintf("No leader!😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬")
	}
	<-rf.electionTimer.C
}

func (rf *Raft) handleVoting() {
	for {
		t := <-rf.electionTimer.C
		select {
		case <-rf.killCh:
			DPrintf("KIlled")
			return
		case <-rf.resetCh:
			DPrintf("Reset!!!!!!!!!! 😡😡😡😡😡😡😡😡😡😡😡    on peer %+v", rf.me)
		default:
			DPrintf("time up ⏲  at %v on %v", t, rf.me)
			go rf.holdVote()
		}
	}
}

func (rf *Raft) startHeartBeats() {
	replies := make(chan *AppendEntriesReply, len(rf.peers)-1)
	for {
		rf.resetElecTimer()
		timeout := <-rf.heartbeat.C
		if rf.IsLeader {
			DPrintf("%+v (%+v) Sent a Regular Heartbeat 💖💖💖💖💖💖💖💖💖💖💖💖💖 at %+v", rf.me, rf.IsLeader,
				timeout)
			rf.mu.Lock()
			args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.lastApplied}
			rf.mu.Unlock()
			for peer, _ := range rf.peers {
				if peer != rf.me {
					go func(i int) {
						reply := &AppendEntriesReply{}
						ok := rf.sendAppendEntries(i, args, reply)
						if ok != true {
							DPrintf("okay %+v ----------  😱   sent: %+v, got: %+v", ok, args, reply)
							// ok = rf.sendAppendEntries(i, args, reply)
						}
						replies <- reply
					}(peer)
				}
			}
			for i := 0; i < len(replies); i++ {
				<-replies
			}
		}
	}
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
	IsLeader := true

	return index, term, IsLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	rf.IsLeader = false
	rf.currentTerm = 0
	rf.heartbeat.Stop()
	rf.electionTimer.Stop()
	rf.mu.Unlock()
	DPrintf("Killed 🔫🔫🔫🔫🔫🔫🔫🔫🔫🔫🔫         %+v", rf.me)
	go func() { rf.killCh <- true }()
	return
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
	rf.applyCh = applyCh
	rf.resetCh = make(chan bool)
	rf.votedFor = -1

	rf.electionTimer = time.NewTicker(getRandDuration(true)) //create a random timer here
	rf.heartbeat = time.NewTicker(getRandDuration(false))    //create a random timer here

	// Your initialization code here.
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.handleVoting()

	return rf
}

func getRandDuration(double bool) time.Duration {
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	var randDur time.Duration
	randomInt := rand.Intn(150)
	if double {
		randDur = time.Millisecond * time.Duration(100+2*(baseTime+randomInt)) // save the duration for resets
	} else {
		randDur = time.Millisecond * time.Duration(baseTime+randomInt) // save the duration for resets
	}
	return randDur
}
