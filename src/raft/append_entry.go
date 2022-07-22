package raft

import (
	"log"
	"sync"
	"time"
)

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
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Printf("follower get append entry from %v to %v", args.LeaderId, rf.me)
	reply.Term = rf.currentTerm
	reply.Success = false
	if rf.currentTerm > args.Term {
		log.Printf("sever %v current term is %v, leader's term is %v", rf.me, rf.currentTerm, args.Term)
		return
	} else if rf.voteStatus == 1 {
		//log.Printf("rf voteStatus is %v", rf.voteStatus)
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
	//log.Printf("sever %v current term is %v, leader's term is %v", rf.me, rf.currentTerm, args.Term)
	rf.voteFor = -1
	//rf.persist()
	rf.isLeader = false
	log.Printf("args.Prev %v, rf.last %v from %v to rf.me %v %v, append entry %v", args.PrevLogIndex, rf.lastApplied,
		args.LeaderId, rf.me, args.LeaderCommit, len(args.Entries))
	if rf.MatchLogs(args.PrevLogIndex, args.PrevLogTerm) {
		rf.SetLogItems(args.PrevLogIndex+1, args.Entries)
		log.Printf("rf.me %v, logs %v, commit :%v\n", rf.me, rf.logs, rf.commitIndex)
		//limit := SnapShotInterval*((rf.commitIndex+1)/SnapShotInterval+1) - 1
		commitIndex := args.LeaderCommit
		//if limit < commitIndex {
		//	commitIndex = limit
		//}
		if commitIndex > rf.commitIndex {
			lastCommitIndex := rf.commitIndex
			rf.commitIndex = commitIndex
			rf.mu.Unlock()
			log.Printf(" rf commit success %v %v\n", rf.me, rf.commitIndex)
			rf.CommitApplyCh(lastCommitIndex, commitIndex)
			//for i := lastCommitIndex + 1; i <= commitIndex; i++ {
			//	command := rf.GetLogItem(i).Command
			//	//log.Printf("server %v command : %v", rf.me, command)
			//	*rf.applyChan <- ApplyMsg{
			//		CommandValid: true,
			//		Command:      command,
			//		CommandIndex: i,
			//	}
			//}
			rf.mu.Lock()
			log.Printf("Server %v commit id %v", rf.me, rf.commitIndex)
		}
		reply.Success = true
	} else {
		if rf.lastApplied >= args.PrevLogIndex {
			//log.Printf("COMMIT FAIL: args: %v, rf.logs[args.PrevLogIndex].Term %v", args, rf.logs[args.PrevLogIndex].Term)
		} else {
			//log.Printf("LASTAPP: %v %v", rf.me, rf.lastApplied)
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) LeaderOperation(rpcTimeOut int) {
	log.Printf("Server %v (adress is %p) become appending entries, logs is %v",
		rf.me, rf, len(rf.logs))
	if rf.killed() == false {
		timeLimit := time.After(time.Duration(rpcTimeOut) * time.Millisecond)
		//rf.mu.Lock()
		//if !rf.isLeader {
		//	rf.mu.Unlock()
		//	return
		//}
		//lastApplied := rf.lastApplied
		//lastIncludedIndex := rf.lastIncludedIndex
		//lastIncludedTerm := rf.lastIncludedTerm
		//commitIndex := rf.commitIndex
		//term := rf.currentTerm
		//data := rf.GetSnapshotByte(&rf.SnapShotEntry)
		//rf.mu.Unlock()
		wg := sync.WaitGroup{}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rf.mu.Lock()
				lastApplied := rf.lastApplied
				lastIncludedIndex := rf.lastIncludedIndex
				lastIncludedTerm := rf.lastIncludedTerm
				commitIndex := rf.commitIndex
				term := rf.currentTerm
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 {
					prevLogTerm = rf.GetLogItem(prevLogIndex).Term
				}
				//entries := rf.logs[rf.nextIndex[i]-lastIncludedIndex : lastApplied-lastIncludedIndex+1]
				log.Printf("rf.nextIndex %v is : %v\n", i, rf.nextIndex[i])
				var data []byte
				var entries []LogEntry
				if prevLogIndex <= lastIncludedIndex {
					data = rf.GetSnapshotByte()
					log.Println("data", data)
					entries = rf.GetLogItems(lastIncludedIndex+1, lastApplied+1)
				} else {
					entries = rf.GetLogItems(rf.nextIndex[i], lastApplied+1)
				}
				rf.mu.Unlock()
				if prevLogIndex <= lastIncludedIndex {
					snapshotArgs := SnapshotArgs{
						Term:              term,
						LeaderId:          rf.me,
						LastIncludedIndex: lastIncludedIndex,
						LastIncludedTerm:  lastIncludedTerm,
						Offset:            0,
						Data:              data,
						Done:              true,
					}

					reply := SnapshotReply{}
					if ok := rf.sendInstallSnapshot(i, &snapshotArgs, &reply); ok {
						rf.mu.Lock()
						if rf.currentTerm >= reply.Term {
							log.Printf("Server %d send Snapshot to %d success", rf.me, i)
							rf.nextIndex[i] = lastIncludedIndex + 1
							rf.matchIndex[i] = lastIncludedIndex
							prevLogTerm = lastIncludedTerm
							prevLogIndex = lastIncludedIndex
						}
						rf.mu.Unlock()
					} else {
						log.Printf("Server %d send Snapshot to %d failed", rf.me, i)
						return
					}
					//return
				}

				appendEntriesArgs := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: commitIndex,
				}
				appendEntriesReply := AppendEntriesReply{}
				//log.Printf("Server %v (adress is %p) send entries to server %v, logs length is %v",
				//	rf.me, rf, i, len(rf.logs))
				//time.Sleep(20 * time.Millisecond)
				if ok := rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply); !ok {
					log.Printf("Server %v (address is %p) send entries to server %v error",
						rf.me, rf, i)
				} else {
					//rf.mu.Lock()
					//defer rf.mu.Unlock()
					//if appendEntriesReply.term > rf.currentTerm {
					//	rf.currentTerm = appendEntriesReply.term
					//}
					rf.mu.Lock()
					if !appendEntriesReply.Success {
						//log.Printf("Follower sever %v reject entries", i)
						lowerBound := rf.matchIndex[i] + 1
						if rf.lastIncludedIndex+1 > lowerBound {
							lowerBound = rf.lastIncludedIndex + 1
						}
						rf.nextIndex[i] = (rf.nextIndex[i] + lowerBound) / 2
						//rf.nextIndex[i]--
						//if rf.nextIndex[i] < 1 {
						//	rf.nextIndex[i] = 1
						//}
						//log.Printf("leader %v rejust nextIndex %v %v %v", rf.me, i, rf.nextIndex[i], rf.matchIndex[i])
					} else {
						rf.nextIndex[i] = lastApplied + 1
						rf.matchIndex[i] = lastApplied
						log.Printf("Follower sever %v accept entries, rf.matchIndex[i] %v value : %v commit :%v\n",
							i, rf.matchIndex[i], rf.logs, rf.commitIndex)
					}
					rf.mu.Unlock()
				}
			}(i)
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-timeLimit:
			//log.Printf("Server %v (adress is %p) appending entries timeout", rf.me, rf)
		case <-done:
			//log.Printf("Server %v (adress is %p) appending entries finished", rf.me, rf)
		}
	}
}
