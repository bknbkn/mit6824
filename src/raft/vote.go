package raft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	//log.Printf("args is %#v", args)
	//log.Printf("rf is %#v", *rf)
	if (rf.voteFor != -1 && rf.voteFor != args.CandidateId) || rf.currentTerm >= args.Term {
		log.Printf("Server from %v to rf.me %v rf.RequestVote False, rf.voteFor %v, rf.currentTerm %v args.Term %v",
			args.CandidateId, rf.me, rf.voteFor, rf.currentTerm, args.Term)
		return
	}
	logTerm := rf.GetLogItem(rf.lastApplied).Term
	if (args.LastLogTerm > logTerm) ||
		(args.LastLogTerm == logTerm && args.LastLogIndex >= rf.lastApplied) {
		rf.voteFor = args.CandidateId
		rf.heartBeat <- struct{}{}
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

func (rf *Raft) VoteOperation(rpcTimeOut int) bool {
	if rf.killed() {
		return false
	}
	timeLimit := time.After(time.Duration(rpcTimeOut) * time.Millisecond)
	rf.mu.Lock()
	term := rf.currentTerm
	log.Printf("leader dead, server %v request vote......", rf.me)
	rf.voteStatus = 1
	rf.voteFor = rf.me
	lastLogIndex := rf.lastApplied
	var LastLogTerm int

	if lastLogIndex >= 0 {
		LastLogTerm = rf.GetLogItem(lastLogIndex).Term
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
		if int(atomic.LoadInt32(&voteNum)) > len(rf.peers)/2 {
			break
		}
		wg.Add(1)
		//rf.mu.Unlock()
		go func(i int) {
			defer wg.Done()
			//log.Printf("Server %v (address is %p) become to send vote req to server %v ",
			//	rf.me, rf, i)
			requestVoteReply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &requestVoteArgs, &requestVoteReply); !ok {
				//log.Printf("Server %v (address is %p) send vote req to server %v error",
				//	rf.me, rf, i)
			} else {
				log.Printf("Server %v send vote req to server %v successful, get voteGrand %v",
					rf.me, i, requestVoteReply.VoteGranted)
				if requestVoteReply.VoteGranted {
					atomic.AddInt32(&voteNum, 1)
				}
			}
			//if atomic.LoadInt32(&rf.voteStatus) == 0 {
			//	return
			//}
		}(i)
	}
	//log.Printf("Server %v waiting for vote req", rf.me)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-timeLimit:
		//log.Printf("Server %v send vote req timeout", rf.me)
	case <-done:
		//log.Printf("Server %v send vote req finished", rf.me)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		rf.voteFor = -1
		rf.voteStatus = 0
	}()
	if rf.voteStatus == 0 {
		return false
	}
	if int(atomic.LoadInt32(&voteNum)) > len(rf.peers)/2 {
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.lastApplied + 1
			rf.matchIndex[i] = 0
		}
		rf.isLeader = true
		rf.currentTerm += 1
		log.Printf("sever %v become leader, term is %v", rf.me, rf.currentTerm)
		return true
		//time.Sleep(5 * time.Millisecond)
	} else {
		//log.Printf("sever %v cant become leader", rf.me)
		return false
	}
}
