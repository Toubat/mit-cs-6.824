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
type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type MapReduceTask interface {
	GetType() TaskType
}

type MapTask struct {
	Id            int
	File          string
	NReduce       int
	State         TaskState
	LastScheduled time.Time
	MapReduceTask
}

func (t *MapTask) GetType() TaskType {
	return Map
}

type ReduceTask struct {
	Id            int
	NMap          int
	State         TaskState
	LastScheduled time.Time
	MapReduceTask
}

func (t *ReduceTask) GetType() TaskType {
	return Reduce
}

// RPC definitions
type GetTaskArgs struct {
}

type GetTaskReply struct {
	Task *MapReduceTask
}

type CompleteTaskArgs struct {
	Id int
	TaskType
}

type CompleteTaskReply struct {
}
