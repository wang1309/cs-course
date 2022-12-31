package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (b ByKey) Len() int {
	return len(b)
}

func (b ByKey) Less(i, j int) bool {
	return b[i].Key < b[j].Key
}

func (b ByKey) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// 单机运行，直接使用 PID 作为 Worker ID，方便 debug
	id := strconv.Itoa(os.Getpid())
	// 进入循环，向 Coordinator 申请 Task
	var lastTaskType string
	var lastTaskIndex int

	for {
		args := ApplyForTaskArgs{
			WorkerID:      id,
			LastTaskIndex: lastTaskIndex,
			LastTaskType:  lastTaskType,
		}

		reply := ApplyForTaskReply{}

		call("Coordinator.ApplyForTask", &args, &reply)

		if reply.TaskType == "" {
			// MR 作业已完成，退出
			log.Printf("Received job finish signal from coordinator")
			break
		}

		if reply.TaskType == Map {
			file, err := os.Open(reply.MapInputFile)
			if err != nil {
				log.Fatalf("Failed to open map input file %s: %e", reply.MapInputFile, err)
			}

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Failed to read map input file %s: %e", reply.MapInputFile, err)
			}

			kva := mapf(reply.MapInputFile, string(content))
			hashedKva := make(map[int][]KeyValue)
			for _, kv := range kva {
				hashedKva[ihash(kv.Key)%reply.ReduceNum] = append(hashedKva[ihash(kv.Key)], kv)
			}

			for i := 0; i < reply.ReduceNum; i++ {
				ofile, _ := os.Create(tmpMapOutFile(id, reply.TaskIndex, i))
				for _, kv := range hashedKva[i] {
					fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
				}
				ofile.Close()
			}

		} else if reply.TaskType == Reduce {
			var lines []string
			for i := 0; i < reply.MapNum; i++ {
				inputFile := finalMapOutFile(i, reply.TaskIndex)
				file, err := os.Open(inputFile)
				if err != nil {
					log.Fatalf("Failed to open map output file %s: %e", inputFile, err)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Failed to read map output file %s: %e", inputFile, err)
				}
				lines = append(lines, strings.Split(string(content), "\n")...)
			}

			var kva []KeyValue
			for _, line := range lines {
				if strings.TrimSpace(line) == "" {
					continue
				}
				parts := strings.Split(line, "\t")
				kva = append(kva, KeyValue{
					Key:   parts[0],
					Value: parts[1],
				})
			}

			// 按 Key 对输入数据进行排序
			sort.Sort(ByKey(kva))

			ofile, _ := os.Create(tmpReduceOutFile(id, reply.TaskIndex))

			// 按 Key 对中间结果的 Value 进行归并，传递至 Reduce 函数
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// 写出至结果文件
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			ofile.Close()
		}

		// 记录已完成 Task 的信息，在下次 RPC 调用时捎带给 Coordinator
		lastTaskType = reply.TaskType
		lastTaskIndex = reply.TaskIndex
		log.Printf("Finished %s task %d", reply.TaskType, reply.TaskIndex)
	}

	log.Printf("Worker %s exit\n", id)
}

func tmpMapOutFile(workId string, taskIndex int, fileIndex int) string {
	return fmt.Sprintf("temp-map-%s-%d-%d", workId, taskIndex, fileIndex)
}

func finalMapOutFile(taskIndex int, fileIndex int) string {
	return fmt.Sprintf("final-map-%d-%d", taskIndex, fileIndex)
}

func tmpReduceOutFile(workId string, taskIndex int) string {
	return fmt.Sprintf("temp-reduce-%s-%d", workId, taskIndex)
}

func finalReduceOutFile(lastIndex int) string {
	return fmt.Sprintf("final-reduce-%d", lastIndex)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
