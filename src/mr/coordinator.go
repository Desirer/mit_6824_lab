package mr

import (
	"container/list"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type Coordinator struct {
	mu            sync.Mutex
	taskList      list.List
	nReduce       int
	mapDoneNum    int
	reduceDoneNum int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) assignTask(args *TaskArgs, reply *TaskReply) error {
	// 1、检查map任务是否都完成
	if c.mapDoneNum < c.nReduce {
		c.mu.Lock()
		reply.task = c.taskList.Front().Value.(Task) // 类型断言
		c.taskList.Remove(c.taskList.Front())        //出队
		c.mu.Unlock()
		return nil
	}
	// 2、检查reduce任务是否都完成
	if c.reduceDoneNum < c.nReduce {
		c.mu.Lock()
		reply.task = c.taskList.Front().Value.(Task) // 类型断言
		c.taskList.Remove(c.taskList.Front())        //出队
		c.mu.Unlock()
		return nil
	}
	// 3、都完成任务后发送退出请求
	ret_task := Task{taskType: EXIT_TASK}
	reply.task = ret_task
	return nil
}
func (c *Coordinator) ackTask(args *DoneArgs, reply *DoneReply) error {
	switch args.taskType {
	case REDUCE_TASK:
		c.reduceDoneNum++
	case MAP_TASK:
		c.mapDoneNum++
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.mapDoneNum == c.nReduce && c.reduceDoneNum == c.nReduce {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:       nReduce,
		taskList:      list.List{},
		mapDoneNum:    0,
		reduceDoneNum: 0,
	}
	mapTasks := MakeMapTasks(files, nReduce)
	for _, task := range mapTasks {
		c.taskList.PushBack(task)
	}
	reduceTasks := MakeReduceTasks(nReduce)
	for _, task := range reduceTasks {
		c.taskList.PushBack(task)
	}
	c.server()
	return &c
}
func MakeMapTasks(files []string, n int) []Task {
	chunkSize := (len(files) + n - 1) / n
	chunks := make([]Task, n)
	for i := 0; i < n; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end >= len(files) {
			end = len(files) - 1
		}
		chunks[i].taskType = MAP_TASK
		chunks[i].taskId = i
		chunks[i].files = files[start:end]
		chunks[i].nReduce = n
	}
	return chunks
}
func MakeReduceTasks(n int) []Task {
	chunks := make([]Task, n)
	for i := 0; i < n; i++ {
		chunks[i].taskType = REDUCE_TASK
		chunks[i].taskId = i
		chunks[i].nReduce = n
	}
	return chunks
}
