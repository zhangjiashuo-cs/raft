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
	"../labrpc"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
const (
	HEARTBEAT_INTERVAL     int = 120
	ELECTION_TIMEOUT_BASE  int = 300
	ELECTION_TIMEOUT_RANGE int = 300
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        int //follower:0, candidate:1, leader:2
	currentTerm int //last term server has seen, initialized to 0 on first boot
	voteFor     int // candidateID that received vote in current ture, or -1 if none
	log         []logEntry
	commitIndex int
	lastApplied int
	nextIndex   []int //reinitialized after election
	matchIndex  []int //reinitialized after election
	heartBeatCh chan bool
	applyCh     chan ApplyMsg
}
type logEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.role == 2)

	return term, isleader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() { //unsafe
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveRaftState(rf.encodeState())
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
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []logEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		return
	}
	rf.currentTerm, rf.voteFor, rf.log = currentTerm, votedFor, logs
	rf.lastApplied, rf.commitIndex = rf.log[0].Index, rf.log[0].Index
	//fmt.Println(rf.me, "Restarted! loaded log:", rf.log)
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
	LastLogTerm  int
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
func (rf *Raft) BroadCastRequestVoteUnsafe(args *RequestVoteArgs) {
	peerNumber := len(rf.peers)
	selfNumber := rf.me

	count := 1 //vote for itself

	for i := 0; i < peerNumber; i += 1 {

		if i == selfNumber {
			continue
		} else {
			go func(peer int) {
				reply := RequestVoteReply{
					Term:        0,
					VoteGranted: false,
				}

				ok := rf.sendRequestVote(peer, args, &reply)
				if ok {
					rf.mu.Lock()
					if reply.VoteGranted == false && reply.Term > rf.currentTerm && rf.role == 1 {
						rf.BecomeFollower_unsafe(reply.Term)

						rf.mu.Unlock()
						rf.heartBeatCh <- true
						return
					}
					if rf.currentTerm == reply.Term && rf.role == 1 && reply.VoteGranted {
						count++
						if count >= (peerNumber+1)/2 {
							rf.BecomeLeaderUnsafe()
							rf.MakeHeartBeatOnceUnsafe()
							go rf.MakeHeartBeat()
						}
					}
					rf.mu.Unlock()
				}

			}(i)

		}
	}

	return

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevlogIndex int
	PrevlogTerm  int
	Entries      []logEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term       int
	Success    bool
	MatchIndex int
}

func (rf *Raft) BecomeLeaderUnsafe() {
	//fmt.Println(rf.me, "Become Leader!")
	rf.role = 2
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.getLastLogIndexUnsafe() + 1
	}
}
func (rf *Raft) MakeHeartBeatOnceUnsafe() {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevlogIndex: rf.getLastLogIndexUnsafe(),
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := AppendEntriesReply{
			Success:    false,
			Term:       0,
			MatchIndex: 0,
		}
		go func(peer int) {
			rf.sendAppendEntries(peer, &args, &reply)
		}(i)
	}

	return
}

// AppendEntries RPC Handle
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//defer fmt.Println(rf.me, "Received AppendEntry", args, "current log is:", rf.log, "current term is:", rf.currentTerm, "AppendEntryReply is:", reply)
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.MatchIndex = 0

	if args.Term >= rf.currentTerm && rf.role != 0 {
		rf.BecomeFollower_unsafe(args.Term)
	}
	if args.Term < rf.currentTerm {
		rf.persist()
		rf.mu.Unlock()
		return
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.MatchIndex = 0

	if args.Term == rf.currentTerm {

		if len(args.Entries) == 0 { //heartbeat
			if rf.getLastLogIndexUnsafe() >= args.PrevlogIndex && rf.log[args.PrevlogIndex].Term < args.PrevlogTerm && len(args.Entries) == 0 {
				reply.MatchIndex = rf.commitIndex
				rf.persist()
				rf.mu.Unlock()
				rf.heartBeatCh <- true
				return
			}
			if args.LeaderCommit > rf.commitIndex && args.PrevlogTerm == rf.getLastLogTermUnsafe() && args.PrevlogIndex == rf.getLastLogIndexUnsafe() { //update commit
				if args.LeaderCommit > rf.getLastLogIndexUnsafe() && rf.getLastLogTermUnsafe() == args.PrevlogTerm {
					rf.commitIndex = rf.getLastLogIndexUnsafe()
				} else {
					if rf.log[args.LeaderCommit].Term == args.PrevlogTerm {
						rf.commitIndex = args.LeaderCommit
					}
				}
			}
			rf.persist()
			rf.mu.Unlock()
			rf.heartBeatCh <- true
			reply.Success = true
			reply.Term = args.Term
			reply.MatchIndex = rf.commitIndex
			return
		}

		if rf.getLastLogIndexUnsafe() > args.PrevlogIndex {
			if rf.log[args.PrevlogIndex].Term != args.PrevlogTerm {
				reply.MatchIndex = rf.commitIndex
				rf.persist()
				rf.mu.Unlock()
				rf.heartBeatCh <- true

				return
			}
			rf.log = rf.log[0 : args.PrevlogIndex+1]
			for _, i := range args.Entries {
				rf.log = append(rf.log, i)
			}
			rf.persist()
			reply.Success = true
			reply.Term = args.Term
			reply.MatchIndex = rf.getLastLogIndexUnsafe()
			rf.mu.Unlock()

			rf.heartBeatCh <- true
			return
		}
		if rf.getLastLogIndexUnsafe() < args.PrevlogIndex {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.MatchIndex = rf.commitIndex
			rf.persist()
			rf.mu.Unlock()
			rf.heartBeatCh <- true
			return
		}
		// rf.getLastLogIndexUnsafe() == args.PrevlogIndex
		if rf.log[args.PrevlogIndex].Term != args.PrevlogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.MatchIndex = rf.commitIndex
			rf.persist()
			rf.mu.Unlock()
			rf.heartBeatCh <- true
			return
		}
		//lastlog==prevlog
		rf.log = rf.log[0 : args.PrevlogIndex+1]
		for _, i := range args.Entries {
			rf.log = append(rf.log, i)
		}

		if args.LeaderCommit > rf.commitIndex && args.PrevlogTerm == rf.getLastLogTermUnsafe() { //update commit
			if args.LeaderCommit > rf.getLastLogIndexUnsafe() && rf.getLastLogTermUnsafe() == args.PrevlogTerm {
				rf.commitIndex = rf.getLastLogIndexUnsafe()
			} else {
				if rf.log[args.LeaderCommit].Term == args.PrevlogTerm {
					rf.commitIndex = args.LeaderCommit
				}
			}
		}

		reply.Success = true
		reply.Term = args.Term
		reply.MatchIndex = rf.getLastLogIndexUnsafe()
		rf.persist()
		rf.mu.Unlock()
		rf.heartBeatCh <- true

		return
	}
	rf.persist()
	rf.mu.Unlock()
	return
}

// RequestVote RPC Handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//defer fmt.Println(rf.me, "Received RequestVote from ", args.CandidateId, ",Args is ", args, "My Term: ", rf.currentTerm, "log:", rf.log, "RequestVoteReply: ", reply)

	rf.mu.Lock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.persist()
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.BecomeFollower_unsafe(args.Term)
	}
	if args.Term == rf.currentTerm && rf.role == 1 && ((args.LastLogIndex > rf.getLastLogIndexUnsafe() && args.LastLogTerm == rf.getLastLogTermUnsafe()) || (args.LastLogTerm > rf.getLastLogTermUnsafe())) {
		rf.BecomeFollower_unsafe(args.Term)
	}
	if args.Term >= rf.currentTerm && ((args.LastLogIndex >= rf.getLastLogIndexUnsafe() && args.LastLogTerm == rf.getLastLogTermUnsafe()) || (args.LastLogTerm > rf.getLastLogTermUnsafe())) && (rf.voteFor == args.CandidateId || rf.voteFor == -1) {

		rf.currentTerm = args.Term
		rf.voteFor = args.CandidateId
		rf.role = 0
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		//rf.mu.Unlock()
		rf.persist()
		rf.mu.Unlock()
		rf.heartBeatCh <- true
		return
	}
	reply.Term = rf.currentTerm
	//rf.mu.Unlock()
	rf.persist()
	rf.mu.Unlock()
	rf.heartBeatCh <- true
	return

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//fmt.Println(rf.me, "Try to Append Entries to ", server, "Args:", args)
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

func (rf *Raft) appendNewEntryUnsafe(command interface{}) (int, int) {
	newEntry := logEntry{Term: rf.currentTerm, Index: rf.getLastLogIndexUnsafe() + 1, Command: command}
	rf.log = append(rf.log, newEntry)

	index := rf.getLastLogIndexUnsafe()
	term := rf.currentTerm
	return index, term
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == 2 {
		isLeader = true
		index, term = rf.appendNewEntryUnsafe(command)
		rf.persist()
		//fmt.Println(rf.me, "Received Command: ", command)
	}

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) getLastLogIndexUnsafe() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTermUnsafe() int {
	return rf.log[rf.getLastLogIndexUnsafe()].Term
}

func (rf *Raft) BecomeFollower_unsafe(term int) {
	defer rf.persist()
	rf.voteFor = -1
	rf.currentTerm = term
	rf.role = 0
	//rf.heartBeatCh <- true
	return
}

func (rf *Raft) UpdateCommitIndexUnsafe(index int) {
	if index <= rf.commitIndex {
		return
	}
	count := 1
	for i := 0; i < len(rf.peers); i++ {
		if rf.matchIndex[i] >= index && i != rf.me {
			count++
		}
	}
	if count >= (len(rf.peers)+1)/2 && rf.commitIndex < index {
		rf.commitIndex = index
	}

	return
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
//=

func (rf *Raft) ReplicateOnce() {
	if rf.role != 2 {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			if rf.role != 2 {
				rf.mu.Unlock()
				return
			}
			currentEntryIndex := rf.nextIndex[peer]
			var currentEntryArray []logEntry
			if currentEntryIndex == rf.getLastLogIndexUnsafe()+1 {
				//currentEntryIndex = rf.getLastLogIndexUnsafe() + 1
				currentEntryArray = nil
			} else {
				if currentEntryIndex < rf.getLastLogIndexUnsafe()+1 {
					//currentEntryArray = []logEntry{rf.log[currentEntryIndex]}
					currentEntryArray = make([]logEntry, rf.getLastLogIndexUnsafe()+1-currentEntryIndex)
					copy(currentEntryArray, rf.log[currentEntryIndex:rf.getLastLogIndexUnsafe()+1])
				}

			}

			entryArgs := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevlogIndex: currentEntryIndex - 1, PrevlogTerm: rf.log[currentEntryIndex-1].Term, Entries: currentEntryArray, LeaderCommit: rf.commitIndex}
			Reply := AppendEntriesReply{
				Success:    false,
				Term:       0,
				MatchIndex: 0,
			}
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(peer, &entryArgs, &Reply)
			if ok {
				rf.mu.Lock()
				if Reply.Success && Reply.Term == rf.currentTerm {
					rf.matchIndex[peer] = Reply.MatchIndex
					rf.nextIndex[peer] = Reply.MatchIndex + 1
					if rf.matchIndex[peer] <= rf.getLastLogIndexUnsafe() && rf.log[rf.matchIndex[peer]].Term == rf.currentTerm {
						rf.UpdateCommitIndexUnsafe(rf.matchIndex[peer])
					}
				} else {
					if Reply.Term > rf.currentTerm {
						//Must do nothing
					}
					if Reply.Term == rf.currentTerm {
						rf.nextIndex[peer] = Reply.MatchIndex + 1
						rf.matchIndex[peer] = Reply.MatchIndex
					}
				}
				rf.mu.Unlock()
			}
		}(i)
	}

}
func (rf *Raft) HeartBeatTicker() {
	for !rf.killed() {
		time.Sleep(time.Duration(5) * time.Millisecond)
		select {
		case <-rf.heartBeatCh:
		case <-time.After(time.Duration(ELECTION_TIMEOUT_BASE+rand.Intn(ELECTION_TIMEOUT_RANGE)) * time.Millisecond):
			rf.mu.Lock()
			if rf.role != 2 {
				//fmt.Println(rf.me, " Started to Election due to heartbeat timeout,current Term:", rf.currentTerm+1)

				rf.StartElectionUnsafe()
			}
			rf.mu.Unlock()
		}
	}

}

func (rf *Raft) MakeHeartBeat() {
	for !rf.killed() {
		_ = <-time.After(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
		rf.mu.Lock()
		if rf.role == 2 {
			//rf.MakeHeartBeatOnceUnsafe()
			rf.ReplicateOnce()
		} else {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) StartElectionUnsafe() {
	defer rf.persist()
	if !rf.killed() {
		rf.role = 1 //become candidate
		rf.currentTerm++
		rf.voteFor = rf.me
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.getLastLogIndexUnsafe(),
			LastLogTerm:  rf.getLastLogTermUnsafe(),
		}

		rf.BroadCastRequestVoteUnsafe((&args)) // Broadcast Request Vote

		return
	}

	return
}
func (rf *Raft) StartApplyMsg() {
	for !rf.killed() {
		time.Sleep(time.Duration(10) * time.Millisecond)
		rf.mu.Lock()
		entries := make([]logEntry, rf.commitIndex-rf.lastApplied)
		copy(entries, rf.log[rf.lastApplied+1:rf.commitIndex+1])
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		for _, entry := range entries {

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}

		}
	}
	return
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = 0
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = append(rf.log, logEntry{Term: 0, Index: 0}) //初始时压入一个Entry

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.heartBeatCh = make(chan bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.HeartBeatTicker()
	go rf.MakeHeartBeat()
	go rf.StartApplyMsg()

	return rf
}
