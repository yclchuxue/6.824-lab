package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type Symbol interface{}

type Reply struct{
	Filename string		//被分配文件名
	Content []byte		//文件内容
	Filenames []string	
	MapIndex int
	Index int			//文件序号
	Work string			//工作类型
}

type Args struct{
	Index int       //map序号
	File int	   	//被分配文件序号
	Done bool		//是否完成
	Work string		//工作类型
}
//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
