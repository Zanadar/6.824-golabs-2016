package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
	// make sure a worker is dispatched for ever ntasks

	dispatched := 0
	for i := 0; i < ntasks; i++ {
		args := DoTaskArgs{mr.jobName, mr.files[i], phase, i, nios}
		select {
		case worker := <-mr.registerChannel:
			go call(worker, "Worker.DoTask", args, new(struct{}))
			dispatched++
		}

	}

	// type DoTaskArgs struct {
	// JobName    string
	// File       string   // the file to process
	// Phase      jobPhase // are we in mapPhase or reducePhase?
	// TaskNumber int      // this task's index in the current phase

	// // NumOtherPhase is the total number of tasks in other phase; mappers
	// // need this to compute the number of output bins, and reducers needs
	// // this to know how many input files to collect.
	// NumOtherPhase int
	// }

	// func (mr *Master) killWorkers() []int {
	// mr.Lock()
	// defer mr.Unlock()
	// ntasks := make([]int, 0, len(mr.workers))
	// for _, w := range mr.workers {
	// debug("Master: shutdown worker %s\n", w)
	// var reply ShutdownReply
	// ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
	// if ok == false {
	// fmt.Printf("Master: RPC %s shutdown error\n", w)
	// } else {
	// ntasks = append(ntasks, reply.Ntasks)
	// }
	// }
	// return ntasks
	// }
	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
