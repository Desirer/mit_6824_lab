package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		// 1、不断重复请求获取任务（通过RPC）
		task := CallForTask()
		// 2、根据任务类型进行处理
		switch task.taskType {
		case EXIT_TASK:
			fmt.Printf("all tasks complete.")
			return
		case WAIT_TASK:
			time.Sleep(time.Duration(time.Second * 5))
			break
		case MAP_TASK:
			worker_map(mapf, task)
		case REDUCE_TASK:
			worker_reduce(reducef, task)
		}
	}
}
func worker_map(mapf func(string, string) []KeyValue, task Task) {
	// 1、generate intermediate pairs
	intermediate := []KeyValue{}
	for _, filename := range task.files { //pg*.txt
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...) //kva...：这是一个可变参数（variadic parameter）的写法，表示将 kva 切片的所有元素逐个传递给 append() 函数。
	}
	// 2、split intermediate pairs into R region, and save to file
	// 2.1 prepare files first
	nReduce := task.nReduce
	outFiles := make([]*os.File, nReduce)
	fileEncs := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		outFiles[i], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
		fileEncs[i] = json.NewEncoder(outFiles[i])
	}
	// 2.2 put the kv into right temp file
	for _, kv := range intermediate {
		word := kv.Key
		outIndex := ihash(word) % task.nReduce
		enc := fileEncs[outIndex]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("Task %v Key %v Value %v Error: %v\n", kv.Key, kv.Value, err)
			panic("Json encode failed")
		}
	}
	// 3、 echo done
	// 3.1 replace temp file
	var outprefix = "mr-" + strconv.Itoa(task.taskId)
	for index, file := range outFiles {
		outname := outprefix + "-" + strconv.Itoa(index)
		oldpath := filepath.Join(file.Name())
		//fmt.Printf("temp file oldpath %v\n", oldpath)
		os.Rename(oldpath, outname)
		file.Close()
	}
	CallTaskDone()
}

func worker_reduce(reducef func(string, []string) string, task Task) {
	// 1、read all intermediate files
	innameprefix := "mr-"
	innamepostfix := "-" + strconv.Itoa(task.taskId)
	// read in all files as a kv array
	intermediate := []KeyValue{}
	for i := 0; i < task.nReduce; i++ {
		inname := innameprefix + strconv.Itoa(i) + innamepostfix
		file, err := os.Open(inname)
		if err != nil {
			fmt.Printf("Open intermediate file %v failed: %v\n", inname, err)
			panic("Open file error")
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

	// 2、sort intermediate key value_list
	sort.Sort(ByKey(intermediate))

	// 3、count each key and save to file
	var outname = "mr-out-" + strconv.Itoa(task.taskId)
	ofile, _ := ioutil.TempFile("mr-tmp", "mr-*")
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
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	// 4、 echo done
	os.Rename(filepath.Join(ofile.Name()), outname)
	ofile.Close()
	CallTaskDone()
}
func CallTaskDone() {
	args := DoneArgs{}
	reply := DoneReply{}
	call("Coordinator.ackTask", &args, &reply)
}

func CallForTask() Task {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.assignTask", &args, &reply)
	if ok {
		fmt.Printf("Task Type %d, Task Id %d\n", reply.task.taskType, reply.task.taskId)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.task
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
