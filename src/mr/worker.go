package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Worker struct {
	MapF    func(string, string) []KeyValue
	ReduceF func(string, []string) string
}

func (w *Worker) Cron() {
	for {
		task, err := w.GetTask()
		if err != nil {
			log.Fatal(err)
		}

		if task == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		switch task.GetType() {
		case Map:
			mapTask := task.(*MapTask)
			if mapTask == nil {
				log.Fatal("Invalid MapTask")
			}

			file, err := os.Open(mapTask.File)
			if err != nil {
				log.Fatalf("cannot open %v", mapTask.File)
			}

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", mapTask.File)
			}

			kva := w.MapF(mapTask.File, string(content))
			for reduceTaskId := 0; reduceTaskId < mapTask.NReduce; reduceTaskId++ {
				filename := CreateIntermediateFilename(mapTask.Id, reduceTaskId)
				file, err := os.Create(filename)
				if err != nil {
					log.Fatal(err)
				}

				enc := json.NewEncoder(file)
				for _, kv := range kva {
					if ihash(kv.Key)%mapTask.NReduce == reduceTaskId {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatal(err)
						}
					}
				}
			}

			file.Close()
			w.CompleteTask(mapTask.Id, Map)

		case Reduce:
			reduceTask := task.(*ReduceTask)
			if reduceTask == nil {
				log.Fatal("Invalid ReduceTask")
			}

			intermediate := make([]KeyValue, 0)
			for mapTaskId := 0; mapTaskId < reduceTask.NMap; mapTaskId++ {
				filename := CreateIntermediateFilename(mapTaskId, reduceTask.Id)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatal(err)
				}

				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}

					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			sort.Sort(ByKey(intermediate))

			tempFile, err := ioutil.TempFile("", "mr-out-tmp-*")
			if err != nil {
				log.Fatal(err)
			}

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := w.ReduceF(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			err = os.Rename(tempFile.Name(), CreateOutputFilename(reduceTask.Id))
			if err != nil {
				log.Fatal(err)
			}

			tempFile.Close()
			w.CompleteTask(reduceTask.Id, Reduce)
		}
	}
}

func MakeWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	worker := Worker{
		MapF:    mapf,
		ReduceF: reducef,
	}

	go worker.Cron()
}

func (w *Worker) GetTask() (MapReduceTask, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	err := w.Call("Master.GetTask", &args, &reply)
	return reply.Task, err
}

func (w *Worker) CompleteTask(taskId int, taskType TaskType) error {
	args := CompleteTaskArgs{}
	reply := CompleteTaskReply{}

	args.Id = taskId
	args.TaskType = taskType

	err := w.Call("Master.CompleteTask", &args, &reply)
	return err
}

// Send an RPC request to the master, wait for the response.
// usually returns nil. returns err if something goes wrong.
func (w *Worker) Call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func CreateIntermediateFilename(mapTaskId int, reduceTaskId int) string {
	return fmt.Sprintf("mr-%v-%v", mapTaskId, reduceTaskId)
}

func CreateOutputFilename(reduceTaskId int) string {
	return fmt.Sprintf("mr-out-%v", reduceTaskId)
}
