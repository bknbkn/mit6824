package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type MapReplay struct {
	Filename     string
	MapId        int
	NReduce      int
	WorkId       int
	TimeOutLimit int
}

type ReduceReplay struct {
	ReduceId     int
	NMap         int
	WorkId       int
	TimeOutLimit int
}

type MapMessage struct {
	Result bool
	MapId  int
	WorkId int
}

type ReduceMessage struct {
	Result   bool
	ReduceId int
	WorkId   int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
