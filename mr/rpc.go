package mr

import "time"

//
// RPC definitions.
//

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

// Type definitions
type TaskState string

const (
	Idle       TaskState = "Idle"
	InProgress TaskState = "InProgress"
	Completed  TaskState = "Completed"
)

type TaskType string

const (
	Map    TaskType = "Map"
	Reduce TaskType = "Reduce"
	Empty  TaskType = ""
)

type MapReduceTask struct {
	TaskType
	MapTask
	ReduceTask
}

type MapTask struct {
	Id            int
	File          string
	NReduce       int
	State         TaskState
	LastScheduled time.Time
}

type ReduceTask struct {
	Id            int
	NMap          int
	State         TaskState
	LastScheduled time.Time
}

// RPC definitions
type GetTaskArgs struct {
}

type GetTaskReply struct {
	Task MapReduceTask
}

type CompleteTaskArgs struct {
	Id int
	TaskType
}

type CompleteTaskReply struct {
}
