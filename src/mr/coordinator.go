package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// type Sym interface{}

type Coordinator struct {
	// Your definitions here.
	Infilenames []string
	Index int
	mux sync.Mutex

	fileopen bool

	cont []byte

	start int

	len   int
}

func (c *Coordinator) Reducef(args *Args, reply *Reply) error {
	//reply.Filename
	
	for i:=0; i < 8 ; i++ {
		reply.Filenames = append(reply.Filenames, fmt.Sprintf("mr-%d%d.txt", i, args.Index))
	}

	return nil
}


func (c *Coordinator) Mapf(args *Args, reply *Reply) error {
	c.mux.Lock()
	reply.Index = c.Index
	// c.Index++
	
	//fmt.Println("In Mapf", args.Index, "\t", len(c.Infilenames))
	if reply.Index < len(c.Infilenames) {//&& !c.fileopen {
	//	fmt.Println("read file", c.Infilenames[reply.Index])
		reply.Filename = c.Infilenames[reply.Index]
		file, err := os.Open(reply.Filename)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Filename)
		}
		
		content, err := ioutil.ReadAll(file)
		if err != nil{
			log.Fatalf("cannot open %v", reply.Filename)
		}

		file.Close()
		c.cont = content
		//c.fileopen = true
		c.len = len(content)
		reply.Content = c.cont
		//fmt.Print(content)
	}
	//if c.fileopen {
		// if c.len - c.start > 1024 {
		// 	reply.Content = c.cont[c.start : c.start+1024]
		// 	c.start = c.start+1024
		// }else{
		// 	reply.Content = c.cont[c.start : c.len]
		// 	c.start = c.len
		// }
		// if c.start >= c.len {
		// 	c.fileopen = false
		// 	c.start = 0
		// 	c.Index++
		// }
	// 	reply.Content = c.cont
	// 	c.cont = c.cont[:0]
	// 	c.fileopen = false
	// }
	c.Index++
	//fmt.Println("end of Mapf")
	c.mux.Unlock()
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
	//l, e := net.Listen("tcp", ":9999")
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
	c.Index = 0
	c.start = 0
	c.fileopen = false

	c.server()
	return &c
}
