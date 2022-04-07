package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	//"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
)

// import "strconv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var wg sync.WaitGroup

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func taskcall(mapf func(string, string) []KeyValue,
reducef func(string, []string) string, index int) {
	args := Args{}

	args.Index = index

	args.Done = false

	reply := Reply{}

	ok := call("Coordinator.Task", &args, &reply)
	if ok {
		
	}else{
		log.Fatalf("call failed!")
	}
}

func mapcall(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, index int) {

	args := Args{}

	args.Index = index

	args.Done = false

	reply := Reply{}

	ok := call("Coordinator.Mapf", &args, &reply)
	if ok {
		fmt.Println(index, "\t", reply.Index)

		args.File = reply.Index

		kv := mapf(reply.Filename, string(reply.Content))

		//fmt.Println("len(kv) = ", len(kv))
		for i := 0; i < len(kv); i++ {
			x := ihash(kv[i].Key) % 10
			name := fmt.Sprintf("mr-%d%d.txt",index, x)

			newfile, err := os.OpenFile(name, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				log.Fatal(err)
			}
			enc := json.NewEncoder(newfile)

			err = enc.Encode(&kv[i])
			if err != nil {
				log.Fatal(err)
			}

			newfile.Close()
			//fmt.Fprintf(opfiles[x], "%v %v\n", kv[i].Key, kv[i].Value)
			//fmt.Println(kv[i].Key, "\n" , kv[i].Value)
		}
		args.Done = true
		ok = call("Coordinator.Mapf", &args, &reply)
		if !ok {
			fmt.Printf("call done failed\n")
		}

	} else {
		fmt.Printf("call failed!\n")
	}
	wg.Done()
}

func reducecall(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, index int) {
	args := Args{}

	args.Index = index

	args.Done = false

	reply := Reply{}

	ok := call("Coordinator.Reducef", &args, &reply)
	if ok {
		args.File = reply.Index
		mediate := []KeyValue{}
		for _,name := range reply.Filenames {
			file, err := os.OpenFile(name, os.O_RDWR, 0666)
			if err != nil {
				fmt.Println("open file failed!")
			}
			x := 0
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				x++
				mediate = append(mediate, kv)
			}
			fmt.Println(x)
			x = 0
			file.Close()
		}

		sort.Sort(ByKey(mediate))

		
		filename := fmt.Sprintf("mr-out-%d.txt", index)

		newfile,err := os.OpenFile(filename, os.O_CREATE | os.O_RDWR, 0666)
		if err != nil {
			fmt.Println("open file failed!")
		}

		i := 0
		for i < len(mediate) {
			j := i + 1
			for j < len(mediate) && mediate[j].Key == mediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, mediate[k].Value)
			}
			output := reducef(mediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(newfile, "%v %v\n", mediate[i].Key, output)

			i = j
		}
		newfile.Close()
		args.Done = true
		ok = call("Coordinator.Reducef", &args, &reply)
		if !ok {
			fmt.Printf("call done failed\n")
		}
	}else{
		fmt.Printf("call failed!\n")
	}
	wg.Done()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	wg.Add(8)

	goroutinenum := 8

	for i := 0; i < goroutinenum; i++ {
		go mapcall(mapf, reducef, i)
	}
	wg.Wait()



	wg.Add(10)

	goroutinenum = 10

	for i := 0; i < goroutinenum; i++ {
		go reducecall(mapf, reducef, i)
	}

	wg.Wait()


		//reducef()
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
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":9999")
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
