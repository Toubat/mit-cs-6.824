package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	CrashTimeout     = 10 * time.Second
	MasterCronPeriod = 1 * time.Second
)

type Master struct {
	MapTasks    []*MapTask
	ReduceTasks []*ReduceTask
	NReduce     int
	TaskStage   TaskType
	sync.Mutex
}

// RPC handlers for the worker to call.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.Lock()
	defer m.Unlock()

	// m.DebugMaster()
	reply.Task.TaskType = Empty

	switch m.TaskStage {
	case Map:
		for _, task := range m.MapTasks {
			if task.State != Idle {
				continue
			}

			task.State = InProgress
			task.LastScheduled = time.Now()
			reply.Task.TaskType = Map
			reply.Task.MapTask = *task

			fmt.Printf("Assigned Map Task %v to worker %v\n", task.Id, task.State == InProgress)
			break
		}

	case Reduce:
		for _, task := range m.ReduceTasks {
			if task.State != Idle {
				continue
			}

			task.State = InProgress
			task.LastScheduled = time.Now()
			reply.Task.TaskType = Reduce
			reply.Task.ReduceTask = *task

			fmt.Printf("Assigned Reduce Task %v to worker\n", task.Id)
			break
		}
	}

	return nil
}

func (m *Master) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	fmt.Println("CompleteTask")
	m.Lock()
	defer m.Unlock()

	switch args.TaskType {
	case Map:
		task := m.MapTasks[args.Id]
		task.State = Completed
		fmt.Printf("Completed Map Task %v\n", task.Id)

		if m.TaskStage == Map && m.AllMapTasksCompleted() {
			m.TaskStage = Reduce
			fmt.Printf("All Map tasks are completed, switching to Reduce stage\n")
		}
	case Reduce:
		task := m.ReduceTasks[args.Id]
		task.State = Completed
		fmt.Printf("Completed Reduce Task %v\n", task.Id)

		if m.TaskStage == Reduce && m.AllReduceTasksCompleted() {
			fmt.Printf("All Reduce tasks are completed, exiting\n")
			os.Exit(0)
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) Serve() {
	rpc.Register(m)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")

	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
	go m.Cron()
}

func (m *Master) Cron() {
	ticker := time.NewTicker(MasterCronPeriod)

	// periodically check if any worker task has crashed
	for range ticker.C {
		m.Lock()

		for _, task := range m.MapTasks {
			if task.State == InProgress && time.Since(task.LastScheduled) > CrashTimeout {
				task.State = Idle
				fmt.Printf("Map Task %v has crashed, reset to Idle\n", task.Id)
			}
		}

		for _, task := range m.ReduceTasks {
			if task.State == InProgress && time.Since(task.LastScheduled) > CrashTimeout {
				task.State = Idle
				fmt.Printf("Reduce Task %v has crashed, reset to Idle\n", task.Id)
			}
		}

		m.Unlock()
	}
}

func (m *Master) AllMapTasksCompleted() bool {
	for _, task := range m.MapTasks {
		if task.State != Completed {
			return false
		}
	}

	return true
}

func (m *Master) AllReduceTasksCompleted() bool {
	for _, task := range m.ReduceTasks {
		if task.State != Completed {
			return false
		}
	}

	return true
}

func (m *Master) DebugMaster() {
	fmt.Printf("Task Stage: %v\n", m.TaskStage)

	for _, task := range m.MapTasks {
		fmt.Printf("Map Task %v: %v\n", task.Id, task.State)
	}

	for _, task := range m.ReduceTasks {
		fmt.Printf("Reduce Task %v: %v\n", task.Id, task.State)
	}

	fmt.Println()
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Master.
func MakeMaster(files []string, nReduce int) *Master {
	mapTasks := make([]*MapTask, len(files))
	for i, file := range files {
		mapTasks[i] = &MapTask{
			Id:      i,
			File:    file,
			NReduce: nReduce,
			State:   Idle,
		}
	}

	reduceTasks := make([]*ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = &ReduceTask{
			Id:    i,
			NMap:  len(files),
			State: Idle,
		}
	}

	m := Master{
		MapTasks:    mapTasks,
		ReduceTasks: reduceTasks,
		NReduce:     nReduce,
		TaskStage:   Map,
	}
	m.Serve()

	return &m
}
