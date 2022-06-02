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
	//	"bytes"
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
type LogEntry struct {
	Term    int
	Command interface{}
}

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
	logs        []*LogEntry

	isLeader    bool
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	heartBeat   chan struct{}

	voteStatus int32
	applyChan  *chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.isLeader
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
	Commit       bool
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.currentTerm > args.Term {
		log.Printf("sever %v current term is %v, leader's term is %v", rf.me, rf.currentTerm, args.Term)
		log.Printf("rf voteStatus is %v", rf.voteStatus)
		return
	} else if rf.voteStatus == 1 {
		if rf.currentTerm <= args.Term {
			rf.voteStatus = 0
		} else {
			return
		}
	}
	//rf.mu.Unlock()
	rf.heartBeat <- struct{}{}
	//rf.mu.Lock()
	rf.currentTerm = args.Term
	rf.voteFor = -1
	rf.isLeader = false
	//log.Printf("args.Prev %v, rf.last %v", args.PrevLogIndex, rf.lastApplied)
	if args.PrevLogIndex <= rf.lastApplied &&
		(rf.lastApplied == -1 || rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm) {
		//log.Printf("rflogs %v len %v", rf.logs[:args.PrevLogTerm+1], len(rf.logs))
		rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
		rf.lastApplied = len(rf.logs) - 1
		if args.Commit {
			lastCommitIndex := rf.commitIndex
			rf.commitIndex = rf.lastApplied
			log.Printf("rf %v commit %v", rf.me, rf.commitIndex)
			for i := lastCommitIndex + 1; i <= rf.commitIndex; i++ {
				command := rf.logs[i].Command
				rf.mu.Unlock()
				*rf.applyChan <- ApplyMsg{
					CommandValid: true,
					Command:      command,
					CommandIndex: i,
				}
				rf.mu.Lock()
			}
			log.Printf("Server %v commit id %v", rf.me, rf.commitIndex)
		}
		reply.Success = true
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	log.Printf("send")
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	log.Printf("send end")
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	log.Printf("args is %#v", args)
	log.Printf("rf is %#v", *rf)
	if rf.voteFor != -1 || rf.currentTerm >= args.Term {
		return
	}
	logTerm := 0
	if rf.lastApplied >= 0 {
		logTerm = rf.logs[rf.lastApplied].Term
	}
	if (args.LastLogTerm > logTerm) ||
		(args.LastLogTerm == logTerm && args.LastLogIndex >= rf.lastApplied) {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	}
	return
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
	term, isLeader = rf.GetState()
	if isLeader {
		log.Printf("rf %v start", rf.me)
		rpcTimeOut := 150
		rf.mu.Lock()
		rf.logs = append(rf.logs, &LogEntry{Term: rf.currentTerm, Command: command})
		rf.lastApplied++
		lastTerm := rf.logs[rf.lastApplied].Term
		rf.mu.Unlock()
		sumSuccess := rf.LeaderOperation(term, true, rpcTimeOut, false) + 1
		log.Printf("sum: %v", sumSuccess)
		if sumSuccess > int32(len(rf.peers)/2) {
			rf.mu.Lock()
			if lastTerm != rf.currentTerm {
				rf.mu.Unlock()
				return index, term, isLeader
			}
			//rf.logs = append(rf.logs, &LogEntry{Term: rf.currentTerm, Command: "Commit"})
			lastCommitIndex := rf.commitIndex
			rf.commitIndex = rf.lastApplied
			//rf.lastApplied++
			rf.mu.Unlock()
			for i := lastCommitIndex + 1; i <= rf.commitIndex; i++ {
				command = rf.logs[i].Command
				*rf.applyChan <- ApplyMsg{
					CommandValid: true,
					Command:      command,
					CommandIndex: i,
				}
			}
			rf.LeaderOperation(term, true, rpcTimeOut, true)
			log.Printf("Server commit id is %v", index)
		}
		index = rf.lastApplied
	} else {
		rf.mu.Lock()
		index = rf.lastApplied
		rf.mu.Unlock()
	}
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

func (rf *Raft) VoteOperation(term int, rpcTimeOut int) {
	if rf.killed() {
		return
	}
	timeLimit := time.After(time.Duration(rpcTimeOut) * time.Millisecond)
	rf.mu.Lock()
	if rf.voteFor != -1 {
		rf.voteFor = -1
		rf.mu.Unlock()
		return
	} else {
		log.Printf("leader dead, server %v request vote......", rf.me)
		rf.voteStatus = 1
		rf.voteFor = rf.me
		//rf.currentTerm += 1
	}
	//term := rf.currentTerm
	lastLogIndex := rf.lastApplied
	var LastLogTerm int
	if lastLogIndex >= 0 {
		LastLogTerm = rf.logs[lastLogIndex].Term
	} else {
		LastLogTerm = 0
	}
	rf.mu.Unlock()
	requestVoteArgs := RequestVoteArgs{
		Term:         term + 1,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  LastLogTerm,
	}
	wg := sync.WaitGroup{}
	var voteNum int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		//rf.mu.Lock()
		if int(voteNum) > len(rf.peers)/2 {
			//rf.mu.Unlock()
			break
		}
		wg.Add(1)
		//rf.mu.Unlock()
		go func(i int) {
			log.Printf("Server %v (address is %p) become to send vote req to server %v ",
				rf.me, rf, i)
			requestVoteReply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &requestVoteArgs, &requestVoteReply); !ok {
				log.Printf("Server %v (address is %p) send vote req to server %v error",
					rf.me, rf, i)
			} else {
				//rf.mu.Lock()
				//defer rf.mu.Unlock()
				//if rf.currentTerm < requestVoteReply.term {
				//	rf.currentTerm = requestVoteReply.term
				//}
				log.Printf("Server %v send vote req to server %v successful, get voteGrand %v",
					rf.me, i, requestVoteReply.VoteGranted)
				if requestVoteReply.VoteGranted {
					//voteNum += 1
					atomic.AddInt32(&voteNum, 1)
				}
			}
			if atomic.LoadInt32(&rf.voteStatus) == 0 {
				return
			}
			wg.Done()
		}(i)
	}
	log.Printf("Server %v waiting for vote req", rf.me)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-timeLimit:
		log.Printf("Server %v send vote req timeout", rf.me)
	case <-done:
		log.Printf("Server %v send vote req finished", rf.me)
	}
	if atomic.LoadInt32(&rf.voteStatus) == 0 {
		return
	}
	if int(voteNum) > len(rf.peers)/2 {
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.lastApplied + 1
			rf.matchIndex[i] = 0
		}
		rf.mu.Lock()
		rf.isLeader = true
		rf.currentTerm += 1
		//rf.logs = append(rf.logs, &LogEntry{Term: rf.currentTerm})
		//rf.lastApplied++
		rf.mu.Unlock()
		log.Printf("sever %v become leader", rf.me)
		time.Sleep(10 * time.Millisecond)
		rf.LeaderOperation(rf.currentTerm, true, rpcTimeOut, false)
	} else {
		log.Printf("sever %v cant become leader", rf.me)
	}
	atomic.StoreInt32(&rf.voteStatus, 0)
}

func (rf *Raft) LeaderOperation(term int, isLeader bool, rpcTimeOut int, commit bool) int32 {
	if rf.killed() {
		return 0
	}
	if isLeader {
		var sumSuccess int32
		timeLimit := time.After(time.Duration(rpcTimeOut) * time.Millisecond)
		log.Printf("Server %v (adress is %p) become appending entries, logs is %v",
			rf.me, rf, len(rf.logs))
		wg := sync.WaitGroup{}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(i int) {
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 {
					prevLogTerm = rf.logs[prevLogIndex].Term
				}
				rf.mu.Lock()
				entries := rf.logs[rf.nextIndex[i] : rf.lastApplied+1]
				commitIndex := rf.commitIndex
				rf.mu.Unlock()
				appendEntriesArgs := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: commitIndex,
					Commit:       commit,
				}
				appendEntriesReply := AppendEntriesReply{}
				log.Printf("Server %v (adress is %p) send entries to server %v, logs length is %v",
					rf.me, rf, i, len(rf.logs))
				if ok := rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply); !ok {
					log.Printf("Server %v (address is %p) send entries to server %v error",
						rf.me, rf, i)
				} else {
					//rf.mu.Lock()
					//defer rf.mu.Unlock()
					//if appendEntriesReply.term > rf.currentTerm {
					//	rf.currentTerm = appendEntriesReply.term
					//}
					if !appendEntriesReply.Success {
						log.Printf("Follower sever %v reject entries", i)
						rf.nextIndex[i]--
						if rf.nextIndex[i] < 1 {
							rf.nextIndex[i] = 1
						}
					} else {
						log.Printf("Follower sever %v accept entries", i)
						rf.nextIndex[i] = rf.lastApplied + 1
						rf.matchIndex[i] = rf.nextIndex[i] - 1
						atomic.AddInt32(&sumSuccess, 1)
					}
				}
				wg.Done()
			}(i)
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-timeLimit:
			log.Printf("Server %v (adress is %p) appending entries timeout", rf.me, rf)
		case <-done:
			log.Printf("Server %v (adress is %p) appending entries finished", rf.me, rf)
		}

		return sumSuccess
	} else {
		return 0
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		followerDur := rand.Intn(200) + 400
		leaderDur := 150
		rpcTimeOut := 150
		timeFollower := time.After(time.Duration(followerDur) * time.Millisecond)
		term, isLeader := rf.GetState()
		timeLeader := time.After(time.Duration(10000) * time.Second)
		if isLeader {
			timeLeader = time.After(time.Duration(leaderDur) * time.Millisecond)
		}
		select {
		case <-rf.heartBeat:
			log.Printf("server %v get heartBeat", rf.me)
			continue
		case <-timeFollower:
			rf.VoteOperation(term, rpcTimeOut)
		case <-timeLeader:
			rf.LeaderOperation(term, isLeader, rpcTimeOut, false)
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
	log.Printf("make server %v, adress is %p", me, rf)

	// Your initialization code here (2A, 2B, 2C).
	rf.voteFor = -1
	rf.heartBeat = make(chan struct{}, 2)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logs = make([]*LogEntry, 1, 10)
	rf.logs[0] = &LogEntry{Term: 0}
	rf.applyChan = &applyCh
	rf.lastApplied = 0
	rf.commitIndex = -1
	log.SetFlags(log.Lmicroseconds)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
