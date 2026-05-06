package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

type Args struct {
	TaskType  string
	TaskIndex int
	//workerID  string
}
type Reply struct {
	//taskType: map, reduce, wait ,done
	TaskType       string
	TaskIndex      int
	FileName       string
	NumberMapTasks int
	NReduce        int
}

// Add your RPC definitions here.
type RPC struct {
	args  Args
	reply Reply
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
