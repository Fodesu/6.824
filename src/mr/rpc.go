package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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

type taskStatus int

const (
	Working taskStatus = iota
	Waiting
	Done
)

type taskType int

const (
	MapTask taskType = iota
	ReduceTask
	DoneTask
)

// Add your RPC definitions here.
type Task struct {
	FileName   string
	Id         int
	FileNum    int
	Workerid   int
	LastTime   time.Time
	Status     taskStatus
	TypefoTask taskType
	NReduce    int
}
type MapTaskJoinArgs struct {
	FileId   int
	WorkerId int
}

type MapTaskJoinReply struct {
	Accept bool
}

type ReduceTaskJoinArgs struct {
	FileId   int
	WorkerId int
}

type ReduceTaskJoinReply struct {
	Accept bool
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
