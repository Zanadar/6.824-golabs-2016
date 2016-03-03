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

const baseTime = 150

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

	term = rf.currentTerm
	isleader = rf.isleader
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
	// DPrintf("my term is %d", rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.VotedGranted = false
		reply.Term = rf.currentTerm
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		rf.votedFor = args.CandidateID // updated who you last voted for
		rf.resetElecTimer()
		rf.isleader = false
		reply.VotedGranted = true
	}
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.resetElecTimer()
	// DPrintf("Vote requested from peer %d------=============", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) resetElecTimer() {
	dur := getRandDuration()
	// DPrintf("Timer reset ⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲⏲  on client %v timer is for %+v", rf.me, dur)
	rf.electionTimer.Reset(dur)
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
	DPrintf("AppendEntries request %+v", args)
	rf.resetElecTimer()
	if args.Term >= rf.currentTerm {
		go func() { rf.resetCh <- true }()
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedFor = -1
		reply.Success = true
	} else {
		reply.Term = rf.currentTerm
	}
	// Your code here.
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) establishAuthority() {
	DPrintf("I'm the boss %+v 👺👺👺👺👺👺👺👺👺👺👺👺👺👺👺👺👺👺👺    ", rf.me)
	go func() {
		replies := make(chan *AppendEntriesReply, len(rf.peers)-1)
		DPrintf("Authority Heartbeat 💖💖💖💖💖💖💖💖💖💖💖💖💖")
		args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.lastApplied}
		for peer, _ := range rf.peers {
			if peer != rf.me {
				go func(i int) {
					reply := &AppendEntriesReply{}
					rf.mu.Lock()
					ok := rf.sendAppendEntries(i, args, reply)
					for ok != true {
						DPrintf("Not okay %+v", ok)
						ok = rf.sendAppendEntries(i, args, reply)
					}
					replies <- reply
					rf.mu.Unlock()
				}(peer)
			}
		}
		for _, i := range rf.peers {
			reply := <-replies
			DPrintf(" %d sent 👀👀👀👀👀👀👀👀      Reply::::: %+v", i, reply)
		}

	}()
}
func (rf *Raft) startHeartBeats() {
	replies := make(chan *AppendEntriesReply, len(rf.peers)-1)
	for rf.isleader {
		timeout := <-rf.electionTimer.C
		DPrintf("Regular Heartbeat 💖💖💖💖💖💖💖💖💖💖💖💖💖 at %+v", timeout)
		args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.lastApplied}
		for peer, _ := range rf.peers {
			if peer != rf.me {
				go func(i int) {
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, args, reply)
					for ok != true {
						DPrintf("Not okay %+v ----------  😱   %+v", ok, args)
						ok = rf.sendAppendEntries(i, args, reply)
					}
					replies <- reply
				}(peer)
			}
		}
		for i := 0; i < len(replies); i++ {
			<-replies
		}
		rf.resetElecTimer()
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
	rf.resetCh = make(chan bool)
	rf.votedFor = -1

	rf.electionTimer = time.NewTimer(getRandDuration()) //create a random timer here

	// Your initialization code here.
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	replies := make(chan *RequestVoteReply, len(peers)-1)
	go func() {
		for {
			<-rf.electionTimer.C
			rf.currentTerm++
			select {
			case <-rf.resetCh:
				rf.currentTerm--
				DPrintf("Reset!!!!!!!!!! %%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
			default:
				DPrintf("Server %d called for a vote		  ✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️", rf.me)
				args := RequestVoteArgs{rf.currentTerm, rf.me, rf.commitIndex, rf.lastApplied}
				for peer, _ := range peers {
					if peer != rf.me {
						go func(i int) {
							// DPrintf("requesting vote with %+v	   ✏️✏️✏️✏️✏️✏️✏️✏️", args)
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
				votes := make([]bool, 0)
				for i := 0; i < len(peers)-1; i++ {
					reply := <-replies
					// DPrintf("Vote		✏️✏️✏️✏️✏️✏️✏️✏️✏️✏️	 reply::::: %+v", reply)
					if reply.VotedGranted == true {
						votes = append(votes, true)
					} else {
						rf.currentTerm = reply.Term
					}
				}
				votes = append(votes, true) // Server votes for itself
				rf.votedFor = rf.me
				majority := (len(peers) - 1) / 2
				if len(votes) >= majority {
					rf.isleader = true
					DPrintf("👏👏👏👏👏👏👏👏👏👏👏👏👏👏👏👏		Leader elected its term is %v", rf.currentTerm)
					rf.establishAuthority()
					rf.startHeartBeats()
				} else {
					DPrintf("No leader!😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬😬")
				}
			}
		}
	}()

	return rf
}

func getRandDuration() time.Duration {
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	randomInt := rand.Intn(150)
	randDur := time.Millisecond * time.Duration(baseTime+randomInt) // save the duration for resets

	return randDur
}
