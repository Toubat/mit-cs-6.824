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

type TaskType string

const (
	Map    TaskType = "Map"
	Reduce TaskType = "Reduce"
)

type MapReduceTask interface {
	GetType() TaskType
	GetState() TaskState
	SetState(state TaskState)
	Schedule()
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

func (t *MapTask) GetState() TaskState {
	return t.State
}

func (t *MapTask) SetState(state TaskState) {
	t.State = state
}

func (t *MapTask) Schedule() {
	t.LastScheduled = time.Now()
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

func (t *ReduceTask) GetState() TaskState {
	return t.State
}

func (t *ReduceTask) SetState(state TaskState) {
	t.State = state
}

func (t *ReduceTask) Schedule() {
	t.LastScheduled = time.Now()
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
