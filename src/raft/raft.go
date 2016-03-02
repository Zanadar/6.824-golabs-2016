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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

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

	electionDuration time.Duration
	electionTimer    *time.Timer
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

	isleader bool
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	return term, isleader
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

	DPrintf("Current state is %v, %v, %v", rf.currentTerm, rf.votedFor, rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	term         int
	candidateID  int
	lastLogIndex int
	lastLogTerm  int
	// Your data here.
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	term         int
	votedGranted bool
	// Your data here.
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("Vote request %v", args)
	rf.resetElecTimer()
	reply.term = rf.currentTerm
	if args.term < rf.currentTerm {
		reply.votedGranted = true
		DPrintf("reply was %v", reply)
	} else {
		// rest of voting logic here
	}
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
	DPrintf("Vote requested from peer %d------=============", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) resetElecTimer() {
	rf.electionTimer.Reset(getRandDuration())
}

type AppendEntriesArgs struct {
	Term         int //leaders term
	LeaderID     int //to redirect requests to the leader
	PrevLogIndex int
	PrevLogTerm  int
}

type AppendEntriesReply struct {
}

func (rf *Raft) AppendEntries() {}

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
	rf.applyCh = applyCh

	rf.electionTimer = time.NewTimer(getRandDuration()) //create a random timer here

	// Your initialization code here.
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	replies := make(chan *RequestVoteReply, len(peers)-1)
	go func() {
		<-rf.electionTimer.C
		DPrintf("Server %d called for a vote", rf.me)
		rf.currentTerm++
		rf.resetElecTimer()
		args := RequestVoteArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.lastApplied}
		for peer, _ := range peers {
			if peer != rf.me {
				go func(i int) {
					DPrintf("requesting vote with %v", args)
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(i, args, reply)
					for ok != true {
						DPrintf("Not okay %v", ok)
						ok = rf.sendRequestVote(i, args, reply)
					}
					replies <- reply
				}(peer)
			}
		}
	}()

	select {
	case reply := <-replies:
		DPrintf("Reply is %v", reply)
	}
	return rf
}

func getRandDuration() time.Duration {
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	randDur := time.Millisecond * time.Duration(10+rand.Intn(500)) // save the duration for resets
	return randDur
}
