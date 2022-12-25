package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	CrashTimeout = 10 * time.Second
	CronPeriod   = 1 * time.Second
)

type Master struct {
	MapTasks    []MapTask
	ReduceTasks []ReduceTask
	NReduce     int
	TaskStage   TaskType
}

// RPC handlers for the worker to call.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
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
	ticker := time.NewTicker(CronPeriod)

	// periodically check if any worker has crashed
	for range ticker.C {
		for _, task := range m.MapTasks {
			if task.State == InProgress && time.Since(task.LastScheduled) > CrashTimeout {
				task.State = Idle
			}
		}

		for _, task := range m.ReduceTasks {
			if task.State == InProgress && time.Since(task.LastScheduled) > CrashTimeout {
				task.State = Idle
			}
		}
	}
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
	mapTasks := make([]MapTask, len(files))
	for i, file := range files {
		mapTasks[i] = MapTask{
			Id:      i,
			File:    file,
			NReduce: nReduce,
			State:   Idle,
		}
	}

	reduceTasks := make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = ReduceTask{
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
