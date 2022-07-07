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
	LogEntries
	isLeader   bool
	nextIndex  []int
	matchIndex []int
	matchStep  []int
	heartBeat  chan struct{}

	isCandidate bool
	applyChan   *chan ApplyMsg

	// For 2D
	SnapshotByte []byte
	NeedSnapShot bool
	SnapShotCond sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.isLeader
	return term, isleader
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}
func (rf *Raft) CommitApplyCh(lastCommitIndex, commitIndex int) {
	for i := lastCommitIndex + 1; i <= commitIndex; i++ {
		//if i%SnapShotInterval == 0 {
		//	time.Sleep(5 * time.Millisecond)
		//}
		//rf.SnapShotCond.L.Lock()
		//for rf.NeedSnapShot {
		//	rf.SnapShotCond.Wait()
		//}
		//rf.SnapShotCond.L.Unlock()
		command := rf.GetLogItem(i).Command
		//log.Printf("ALL command: %v commit %v li %v\n", command, i, commitIndex)
		*rf.applyChan <- ApplyMsg{
			SnapshotValid: false,
			CommandValid:  true,
			Command:       command,
			CommandIndex:  i,
		}
		//log.Printf("ALLFinish command: %v commit %v li %v\n", command, i, commitIndex)
	}
}
func (rf *Raft) Commit() bool {
	if rf.killed() == false {
		log.Printf("start commit judge %v", rf.me)
		rf.mu.Lock()
		//lastTerm := rf.logs[rf.lastApplied].Term
		commitIndex := rf.lastApplied
		log.Printf("Get mu start commit %v %v judge %v, lastAppid %v, lastInclud %v",
			rf.commitIndex, commitIndex, rf.me, rf.lastApplied, rf.GetLogItem(commitIndex))
		//term := rf.currentTerm
		rf.matchIndex[rf.me] = rf.lastApplied
	loop:
		for ; ; commitIndex-- {
			if rf.GetLogItem(commitIndex).Term != rf.currentTerm || commitIndex <= rf.commitIndex {
				log.Printf("no commit rf.logs[commitIndex].Term %v, rf.currentTerm %v, commitIndex %v, rf.commitIndex %v\n",
					rf.GetLogItem(commitIndex).Term, rf.currentTerm, commitIndex, rf.commitIndex)
				rf.mu.Unlock()
				return false
			}
			count := 0
			for i := 0; i < len(rf.peers); i++ {
				if commitIndex <= rf.matchIndex[i] {
					count++
					if count > len(rf.peers)/2 {
						break loop
					}
				}
			}
		}
		log.Printf("mority of server have same logs length %v, become commit %v \n", len(rf.logs), commitIndex)
		//limit := SnapShotInterval*((rf.commitIndex+1)/SnapShotInterval+1) - 1
		//if limit < commitIndex {
		//	commitIndex = limit
		//}
		lastCommitIndex := rf.commitIndex
		//rf.mu.Unlock()
		rf.CommitApplyCh(lastCommitIndex, commitIndex)
		//rf.mu.Lock()
		rf.commitIndex = commitIndex
		rf.mu.Unlock()
		//for i := lastCommitIndex + 1; i <= commitIndex; i++ {
		//	command := rf.GetLogItem(i).Command
		//	log.Printf("ALL command: %v commit %v li %v\n", command, i, commitIndex)
		//	*rf.applyChan <- ApplyMsg{
		//		SnapshotValid: false,
		//		CommandValid:  true,
		//		Command:       command,
		//		CommandIndex:  i,
		//	}
		//	log.Printf("ALLFinish command: %v commit %v li %v\n", command, i, commitIndex)
		//}
		return true
		//log.Printf("Server %v commit id is %v", rf.me, rf.commitIndex)
	}
	return false
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
	//log.Printf("commad all :%v %v %v ", rf.isLeader, rf.me, command)
	rf.mu.Lock()
	term, isLeader = rf.currentTerm, rf.isLeader
	if isLeader {
		log.Printf("rf %v start entry is %v\n", rf.me, command)
		//rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
		rf.SetLogItems(rf.lastApplied+1, []LogEntry{{Term: rf.currentTerm, Command: command}})
		index = rf.lastApplied
		//rf.persist()
		//time.Sleep(10 * time.Millisecond)
		//log.Printf("sum: %v", sumSuccess)
	} else {
		index = rf.lastApplied
	}
	rf.mu.Unlock()
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
func (rf *Raft) ticker() {
	//go rf.Commit(150)
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		followerDur := rand.Intn(300) + 300
		leaderDur := 160
		//commitDur := 100
		//rpcTimeOut := 100
		voteRpcTime := 55
		timeFollower := time.After(time.Duration(followerDur) * time.Millisecond)
		time.Sleep(time.Duration(voteRpcTime) * time.Millisecond)
		_, isLeader := rf.GetState()
		timeLeader := time.After(time.Duration(10000) * time.Second)
		if isLeader {
			log.Printf("Leader %v rf wakeup", rf.me)
			timeLeader = time.After(time.Duration(leaderDur-voteRpcTime) * time.Millisecond)
			timeFollower = time.After(time.Duration(10000) * time.Second)
		}
		rf.persist()
		//rf.PeriodSnapshot(16)
		select {
		case <-rf.heartBeat:
			log.Printf("server %v get heartBeat", rf.me)
			continue
		case <-timeFollower:
			go func() {
				beLeader := rf.VoteOperation(voteRpcTime - 5)
				if beLeader {
					rf.LeaderOperation()
				}
			}()
		case <-timeLeader:
			go func() {
				rf.Commit()
				rf.LeaderOperation()
			}()
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
	//log.Printf("make server %v, adress is %p", me, rf)

	// Your initialization code here (2A, 2B, 2C).
	rf.voteFor = -1
	rf.heartBeat = make(chan struct{}, 10)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchStep = make([]int, len(rf.peers))
	rf.logs = make([]LogEntry, 1, 10)
	rf.logs[0] = LogEntry{Term: 0}
	rf.applyChan = &applyCh
	rf.lastApplied = 0
	rf.commitIndex = -1
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1
	rf.SnapShotCond = sync.Cond{L: &sync.Mutex{}}
	//rf.Snapshot(-1, rf.GetSnapshotByte())
	log.SetFlags(log.Lmicroseconds)
	//log.SetFlags(0)
	//log.SetOutput(ioutil.Discard)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//log.Printf("Server %v", rf.me)
	//for i := 0; i < len(rf.logs); i++ {
	//	log.Printf("rf restart %v ", rf.logs[i])
	//}
	// start ticker goroutine to start elections
	//log.Logger
	//rf.Snapshot(rf.lastIncludedIndex, rf.GetSnapshotByte())
	go rf.ticker()

	return rf
}
