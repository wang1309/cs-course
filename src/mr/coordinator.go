package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	lock sync.Mutex

	mapNumber    int
	reduceNumber int

	stage string
	tasks map[string]Task

	availableTasks chan Task
}

const (
	Map    = "map"
	Reduce = "reduce"
)

type Task struct {
	Index        int
	Type         string
	MapInputFile string
	WorkerID     string
	Deadline     time.Time
}

type ApplyForTaskArgs struct {
	LastTaskType  string
	LastTaskIndex int
	WorkerID      string
}

type ApplyForTaskReply struct {
	TaskType     string
	TaskIndex    int
	MapInputFile string
	MapNum       int
	ReduceNum    int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	if err := rpc.Register(c); err != nil {
		log.Fatal("register error:", err)
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapNumber:      len(files),
		reduceNumber:   nReduce,
		tasks:          make(map[string]Task),
		availableTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
		stage:          Map,
	}

	// Your code here.
	for k, v := range files {
		task := Task{
			Index:        k,
			Type:         Map,
			MapInputFile: v,
		}

		key := GenTaskID(task.Type, task.Index)
		c.tasks[key] = task

		c.availableTasks <- task
	}
	log.Printf("Coordinator start\n")
	c.server()

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			c.lock.Lock()

			for _, task := range c.tasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					task.WorkerID = ""
					c.availableTasks <- task
				}
			}

			c.lock.Unlock()
		}
	}()

	return &c
}

func GenTaskID(t string, index int) string {
	return fmt.Sprintf("%s-%d", t, index)
}

func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	if args.LastTaskType != "" {
		// ?????? Worker ???????????? Task ??????????????????
		c.lock.Lock()
		lastTaskID := GenTaskID(args.LastTaskType, args.LastTaskIndex)
		if task, exists := c.tasks[lastTaskID]; exists && task.WorkerID == args.WorkerID {
			log.Printf(
				"Mark %s task %d as finished on worker %s\n",
				task.Type, task.Index, args.WorkerID)
			// ?????? Worker ????????????????????????????????????????????????
			if args.LastTaskType == Map {
				for ri := 0; ri < c.reduceNumber; ri++ {
					err := os.Rename(
						tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri),
						finalMapOutFile(args.LastTaskIndex, ri))
					if err != nil {
						log.Fatalf(
							"Failed to mark map output file `%s` as final: %e",
							tmpMapOutFile(args.WorkerID, args.LastTaskIndex, ri), err)
					}
				}
			} else if args.LastTaskType == Reduce {
				err := os.Rename(
					tmpReduceOutFile(args.WorkerID, args.LastTaskIndex),
					finalReduceOutFile(args.LastTaskIndex))
				if err != nil {
					log.Fatalf(
						"Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutFile(args.WorkerID, args.LastTaskIndex), err)
				}
			}

			// ?????????????????? Task ??????????????????????????????
			delete(c.tasks, lastTaskID)
			if len(c.tasks) == 0 {
				c.transit()
			}
		}
		c.lock.Unlock()
	}

	// ?????????????????? Task ?????????
	task, ok := <-c.availableTasks
	if !ok { // Channel ????????????????????? MR ???????????????????????? Worker ??????
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("Assign %s task %d to worker %s\n", task.Type, task.Index, args.WorkerID)
	task.WorkerID = args.WorkerID
	task.Deadline = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task

	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = c.mapNumber
	reply.ReduceNum = c.reduceNumber

	return nil
}

func (c *Coordinator) transit() {
	if c.stage == Map {
		// MAP Task ???????????????????????? REDUCE ??????
		log.Printf("All MAP tasks finished. Transit to REDUCE stage\n")
		c.stage = Reduce

		// ?????? Reduce Task
		for i := 0; i < c.reduceNumber; i++ {
			task := Task{
				Type:  Reduce,
				Index: i,
			}
			c.tasks[GenTaskID(task.Type, task.Index)] = task
			c.availableTasks <- task
		}
	} else if c.stage == Reduce {
		// REDUCE Task ??????????????????MR ??????????????????????????????
		log.Printf("All REDUCE tasks finished. Prepare to exit\n")
		close(c.availableTasks) // ?????? Channel???????????????????????????????????? RPC ??????
		c.stage = ""            // ????????????????????????????????????
	}
}
