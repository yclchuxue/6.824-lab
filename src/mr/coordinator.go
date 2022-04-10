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
	// "time"
)

// type Sym interface{}

type Coordinator struct {
	// Your definitions here.
	Infilenames []string
	Indexfiles 	[]int
	Index int
	mfilished map[int]int
	rfilished map[int]int
	mux sync.Mutex
	cond *sync.Cond
	//conditioon bool

	Intermediate_index []int

	mapwork map[string]int
	redwork map[int]int
	failindex []int

	fileopen bool
	cont []byte
	renum int
	manum int
	time  int

	mapfilish bool
	reducefilish bool
}

func (c *Coordinator) Task(args *Args, reply *Reply) error {
	if !c.mapfilish {
		reply.Work = "other"
		reply.Index = 0
		c.Mapf(args, reply)
	}else if !c.reducefilish {
		c.Reducef(args, reply)
	}else{
		work := "break"
		reply.Work = work
	}

	return nil
}

func (c *Coordinator) Reducef(args *Args, reply *Reply) error {
	//reply.Filename
	c.cond.L.Lock()
	if args.Done {
		ice := true
		i := -1
		for it,num := range c.failindex {
			if num == args.Index {
				ice = false
				i = it
				break
			}
		}
		if ice {
			c.rfilished[args.File] = 0       //filished
			//reply.Work = "ok"
		}else{
			c.failindex = append(c.failindex[:i], c.failindex[i+1:]...)
		}
		//fmt.Println(args.Index)

	}else{
		reply.Index = c.renum
		//fmt.Println(len(c.Intermediate_index))
		if c.rfilished[reply.Index] == -1{
			for _,i := range c.Intermediate_index {
				name := fmt.Sprintf("mr-%d%d.txt", i, c.renum)
				reply.Filenames = append(reply.Filenames, name)
				//fmt.Println("name = ", name)
			}
			c.renum++
			c.rfilished[reply.Index] = 1
			c.redwork[reply.Index] = reply.Index
			work := "reduce"
			reply.Work = work
		}else{
			c.cond.Wait()
			if c.renum == -1 {
				work := "other"
				reply.Work = work
			}
			if c.rfilished[c.renum] == -1 {
				
				for it,_ := range c.redwork {
					if c.redwork[it] == -1 && it == c.Index{
						reply.Index = it
					}
					break
				}
				for i := range c.Intermediate_index {
					reply.Filenames = append(reply.Filenames, fmt.Sprintf("mr-%d%d.txt", i, reply.Index))
				}
				c.redwork[reply.Index] = reply.Index
				c.rfilished[reply.Index] = 1
				work := "reduce"
				reply.Work = work
			}
		}
	}
	
	c.cond.L.Unlock()
	return nil
}


func (c *Coordinator) Mapf(args *Args, reply *Reply) error {
	
	c.cond.L.Lock()
	//c.conditioon = true
	if args.Done {
		//fmt.Println("aaa = ",  args.File)
		ice := true
		i := -1
		for it,num := range c.failindex {
			if num == args.Index {
				ice = false
				i = it
				break
			}
		}
		if ice {
			c.mfilished[args.File] = 0       //filished

			c.Intermediate_index = append(c.Intermediate_index, args.Index)
		}else{
			c.failindex = append(c.failindex[:i], c.failindex[i+1:]...)
		}
		//fmt.Println("args.index = ", args.Index)
	}else{
		reply.MapIndex = c.manum
		c.manum++
		ice := 0
		for it,_ := range c.Infilenames {
			//fmt.Println("num = ", c.Indexfiles[it], "index = ", it)
			if c.Indexfiles[it] == 1 {
				reply.Index = it
				//fmt.Println("re.index = ", reply.Index, "\t", it)
				reply.Filename = c.Infilenames[it]
				c.Indexfiles[it] = 0
				c.mfilished[it] = 1
				c.mapwork[c.Infilenames[it]] = reply.MapIndex
				//fmt.Println("num1 = ", c.Infilenames[it])
				ice = 1
				break
			}
		}
		if ice == 1 {
			reply.Work = "map"
		}else{  //多余的mapf等待任务
		// 	for !c.conditioon {
		 		c.cond.Wait()
				//fmt.Println("被唤醒")
				ice = 0
				for it,_ := range c.Infilenames {
					if c.Indexfiles[it] == 1 && it == c.Index{
						reply.Index = it
						reply.Filename = c.Infilenames[it]
						c.mapwork[c.Infilenames[it]] = reply.MapIndex
						c.Indexfiles[it] = 0
						c.mfilished[it] = 1
						ice = 1
						break
					}
				}
				if ice == 1{
					reply.Work = "map"
				}else{
					work := "other"
					reply.Work = work
					
					c.cond.L.Unlock()
					return nil
				}
				//fmt.Println(reply.Work)
		// 	}
		}
		
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
		// time.Sleep(time.Second * 4)
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
	if !c.mapfilish {
		if c.time < 0 {
			//fmt.Println("time < 0")
			for it,_ := range c.Infilenames {
				if c.mfilished[it] != 0 {
					c.cond.L.Lock()
					//c.conditioon = true
					c.mfilished[it] = -1
					c.Index = it          //file i failed
					c.Indexfiles[it] = 1
					if c.mapwork[c.Infilenames[it]] != -1 {
						c.failindex = append(c.failindex, c.mapwork[c.Infilenames[it]])
						c.mapwork[c.Infilenames[it]] = -1
					}
					c.time = 30
					c.cond.Signal()
					c.cond.L.Unlock()
				}
			}
		}

		c.cond.L.Lock()
		index := 0
		for it,_ := range c.Infilenames {
			if c.mfilished[it] != 0{
				ret = false
			}
			//c.Infilenames[it] = num
			//fmt.Println( it, "\tindex = ", index)
			index++

		}
		if ret {
			//fmt.Println("AAAAA")
			c.mapfilish = true
			ret = false
			c.Index = 0
			c.cond.Broadcast()
			c.failindex = []int{} //c.failindex[:1]
			// for it,_ := range c.Infilenames {
			// 	c.Infilenames[it] = 1
			// }
		}
		c.cond.L.Unlock()
	}
	if !c.reducefilish {
		if c.time < 0 {
			for i:= 0; i< 10; i++ {
				if c.rfilished[i] != 0 {
					c.cond.L.Lock()
					//c.conditioon = true
					c.Index = i          //file i failed
					c.rfilished[i] = -1
					c.time = 30
					if c.redwork[i] != -1 {
						c.failindex = append(c.failindex, c.redwork[i])
						c.redwork[i] = -1
					}
					c.cond.Signal()
					c.cond.L.Unlock()
				}
			}
		}
		ret = true
		c.cond.L.Lock()
		for i:= 0; i < 10; i++ {
			if c.rfilished[i] != 0 {
				ret = false
			}
		}
		if ret {
			c.reducefilish = true
			c.renum = -1
			c.cond.Broadcast()
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
	c.Index = 0
	c.renum = 0
	c.manum = 0
	c.fileopen = false
	c.time = 30
	c.mfilished = make(map[int]int)
	c.rfilished = make(map[int]int)
	c.mapwork = make(map[string]int)
	c.redwork = make(map[int]int)
	c.mapfilish = false
	c.reducefilish = false

	c.Infilenames = files

	for i,_ :=range c.Infilenames {
		c.Indexfiles = append(c.Indexfiles, 1)
		c.Indexfiles[i] = 1
		c.mapwork[c.Infilenames[i]] = -1
	}

	for i,_ := range files{
		c.mfilished[i] = -1    //未被分配
	}

	for j:=0; j<10 ;j++ {
		c.rfilished[j] = -1
		c.redwork[j] = -1
	} 
	//fmt.Println("VVV\t", c.filished[5])
	c.server()
	return &c
}
