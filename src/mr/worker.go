package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	// "os"
)

// import "strconv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

func mapcall(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string){

	args := Args{}

	args.Index = 0

	reply := Reply{}

	fmt.Println("AAAAAAAAA")
			
 	ok := call("Coordinator.Mapf", &args, &reply)
	if ok {
		// reply.Y should be 100.
		kv := mapf(reply.Filename, string(reply.Content))
	
		// name := fmt.Sprintf("test%d.txt", reply.Index)
		
		// newfile,_ := os.Create(name)
				
		// if err != nil {
		// 	log.Fatalf("cannot creat file %v", name)
		// }
		fmt.Println(len(kv))
		for i:=0 ; i < len(kv); i++ {
			// fmt.Fprintf(newfile, "%v %v\n", kv[i].Key, kv[i].Value)
			fmt.Println(kv[i].Key, "\n" , kv[i].Value)
		}
	} else {
		fmt.Printf("call failed!\n")
	}
	// if err != nil {
	// 	log.Fatal("dialing:", err)
	// }
	fmt.Println("BBBBBBBBB")
	
	
	// newfile.Close()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	goroutinenum := 10

	for i := 1; i < goroutinenum; i++{
		go mapcall(mapf, reducef)
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":9999")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
