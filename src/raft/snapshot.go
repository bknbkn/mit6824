package raft

import (
	"6.824/labgob"
	"bytes"
	"log"
)

type SnapShotEntry struct {
	lastIncludedIndex int
	lastIncludedTerm  int
	stateMachineState []interface{}
}

func (snapShotEntry *SnapShotEntry) GetSnapshotByte() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapShotEntry.lastIncludedIndex)
	e.Encode(snapShotEntry.stateMachineState)
	//e.Encode(snapShotEntry.lastIncludedTerm)
	return w.Bytes()
}

func (snapShotEntry *SnapShotEntry) GetSnapshotEntry(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	//snapShotEntry.stateMachineState = make([]interface{}, 0)
	var lastIncludedIndex int
	var entries []interface{}
	//log.Printf("data %v %v", data, d.Decode(&currentTerm))
	if d.Decode(&lastIncludedIndex) != nil {
		log.Printf("lastIncludeIndex lost")
	} else if d.Decode(&entries) != nil {
		log.Printf("entries lost")
	} else {
		snapShotEntry.lastIncludedIndex, snapShotEntry.stateMachineState =
			lastIncludedIndex, entries
		//log.Fatal("SnapShot Parse Error")
	}
}

func (rf *Raft) PeriodSnapshot(period int) {
	if (rf.commitIndex != rf.lastIncludedIndex) &&
		(rf.commitIndex+1)%period == 0 {
		rf.mu.Lock()
		lastIncludedIndex, lastIncludedTerm := rf.commitIndex, rf.GetLogItem(rf.commitIndex).Term
		rf.SnapShotEntry = SnapShotEntry{
			lastIncludedIndex: lastIncludedIndex,
			lastIncludedTerm:  lastIncludedTerm,
			stateMachineState: []interface{}{rf.GetLogItem(rf.commitIndex).Command},
		}
		log.Printf("rf.logs snap : %v %v", rf.commitIndex+1, rf.lastApplied+1)
		rf.SetLogItems(rf.commitIndex+1, rf.GetLogItems(rf.commitIndex+1, rf.lastApplied+1))
		log.Printf("rf.logs snap : %v", rf.logs)
		snapShotByte := rf.GetSnapshotByte()
		rf.mu.Unlock()
		applyMsg := ApplyMsg{
			CommandValid:  false,
			Command:       nil,
			CommandIndex:  0,
			SnapshotValid: true,
			Snapshot:      snapShotByte,
			SnapshotTerm:  lastIncludedTerm,
			SnapshotIndex: lastIncludedIndex,
		}
		*rf.applyChan <- applyMsg
		//rf.Snapshot(rf.commitIndex, rf.GetSnapshotByte())
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	entries := rf.GetLogItems(index+1, rf.lastApplied+1)
	term := rf.GetLogItem(index).Term
	rf.GetSnapshotEntry(snapshot)
	rf.SetLogItems(rf.lastApplied+1, entries)
	rf.mu.Unlock()
	applyMsg := ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  term,
		SnapshotIndex: index,
	}
	*rf.applyChan <- applyMsg
}

func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		return
	}
	rf.heartBeat <- struct{}{}
	if args.Offset == 0 {
		rf.SnapshotByte = args.Data
	} else {
		maxLength := args.Offset + len(args.Data)
		if maxLength > len(rf.SnapshotByte) {
			SnapshotByte := rf.SnapshotByte
			rf.SnapshotByte = make([]byte, maxLength)
			copy(rf.SnapshotByte, SnapshotByte)
		} else {
			for i, v := range args.Data {
				rf.SnapshotByte[i+args.Offset] = v
			}
		}
	}
	if args.Done {
		if args.LastIncludedIndex > rf.lastIncludedIndex && rf.GetLogItem(args.LastIncludedIndex).Term == args.Term {
			log.Printf("Server %v has same index and term in snap", rf.me)
			rf.Snapshot(args.LastIncludedIndex, rf.SnapshotByte)
		} else {
			log.Printf("Server %v hasn't same index and term in snap", rf.me)
			rf.Snapshot(rf.lastApplied, rf.SnapshotByte)
		}
	}
}
func (rf *Raft) sendInstallSnapshot(server int, args *SnapshotArgs, reply *SnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
