package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// type Sym interface{}

type Coordinator struct {
	// Your definitions here.
	Infilenames []string
	Index int
	// ma Symbol
	// re Symbol
}


func (c *Coordinator) Mapf(args *Args, reply *Reply) error {
	//reply.Index = c.Index
	fmt.Println("In Mapf", args.Index, "\t", c.Infilenames)
	if args.Index <= len(c.Infilenames) {
		fmt.Println("read file")
		reply.Filename = c.Infilenames[0]
		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		
		content, err := ioutil.ReadAll(file)
		if err != nil{
			log.Fatalf("cannot open %v", reply.Filename)
		}

		file.Close()

		reply.Content = content
		reply.Index = args.Index + 1
		fmt.Print(content)
	}
	//c.Index++
	fmt.Println("end of Mapf")
	return nil
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
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":9999")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
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

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.

	//c.ma, c.re = loadPlugin(files[0])
	c.Infilenames = files
	c.Index = 1

	c.server()
	return &c
}
