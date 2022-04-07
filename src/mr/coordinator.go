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
	filished map[int]int
	mux sync.Mutex
	cond *sync.Cond
	conditioon bool
	mapover bool
	reduceover bool
	fileopen bool
	cont []byte
	start int
	time  int
}

func (c *Coordinator) Task(args *Args, reply *Reply) error {
	if args.work == "map" {
		c.Mapf(args, reply)
	}else{
		c.Reducef(args, reply)
	}

	return nil
}

func (c *Coordinator) Reducef(args *Args, reply *Reply) error {
	//reply.Filename
	c.cond.L.Lock()
	if args.Done {
		c.filished[args.File] = -1       //filished
		fmt.Println(args.Index)
	}else{
		for i:=0; i < 8 ; i++ {
			reply.Index = i
			reply.Filenames = append(reply.Filenames, fmt.Sprintf("mr-%d%d.txt", i, args.Index))
		}
	}
	c.cond.L.Unlock()
	return nil
}


func (c *Coordinator) Mapf(args *Args, reply *Reply) error {
	c.cond.L.Lock()
	//c.mux.Lock()
	//c.conditioon = true
	if args.Done {
		c.filished[args.File] = -1       //filished
		fmt.Println(args.Index)
	}else{
		reply.Index = c.Index
		// c.Index++
		c.filished[c.Index] = args.Index
		
		fmt.Println("In Mapf", args.Index, "\t", len(c.Infilenames))
		if reply.Index < len(c.Infilenames) {
			reply.Filename = c.Infilenames[reply.Index]
			c.Index++
		}//else{  //多余的mapf等待任务
		// 	for !c.conditioon {
				// c.cond.Wait()
				// reply.Index = c.Index
				// reply.Filename = c.Infilenames[reply.Index]
		// 	}
		//  }
		
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
		reply.Content = c.cont
		
		//fmt.Println("end of Mapf")
	}
	//c.conditioon = false
	c.cond.L.Unlock()
	//c.mux.Unlock()
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
	ret := true

	// Your code here.
	c.time--
	if !c.mapover {
		if c.time < 0 {
			for i,_ := range c.Infilenames {
				if c.filished[i] != -1 {
					c.cond.L.Lock()
					c.conditioon = true
					c.Index = i          //file i failed
					c.cond.Signal()
					c.cond.L.Unlock()
				}
			}
		}
		c.cond.L.Lock()
		for i,_ := range c.Infilenames {
			if c.filished[i] != -1 {
				ret = false
			}
		}
		if ret {
			c.mapover = true
			ret = false
			for i,_ := range c.Infilenames {
				c.filished[i] = -2
			}
		}
		c.cond.L.Unlock()
	}
	if !c.reduceover {
		// if c.time < 0 {
		// 	for i,_ := range c.Infilenames {
		// 		if c.filished[i] != -1 {
		// 			c.cond.L.Lock()
		// 			c.conditioon = true
		// 			c.Index = i          //file i failed
		// 			c.cond.Signal()
		// 			c.cond.L.Unlock()
		// 		}
		// 	}
		// }
		ret = true
		c.cond.L.Lock()
		for i,_ := range c.Infilenames {
			if c.filished[i] != -1 {
				ret = false
			}
		}
		if ret {
			c.reduceover = true
		}
		c.cond.L.Unlock()
	}

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

	c.cond = sync.NewCond(&c.mux)
	c.Infilenames = files
	c.Index = 0
	c.start = 0
	c.fileopen = false
	c.time = 100
	c.filished = make(map[int]int)
	for i,_ := range files{
		c.filished[i] = -2
	}

	c.server()
	return &c
}
