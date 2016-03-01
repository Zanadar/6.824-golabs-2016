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

	var done = make(chan bool)

	for i := 0; i < ntasks; i++ {
		go func(i int) {
			worker := <-mr.registerChannel
			args := DoTaskArgs{mr.jobName, mr.files[i], phase, i, nios}
			ok := call(worker, "Worker.DoTask", args, new(struct{}))
			for ok != true {
				worker = <-mr.registerChannel
				ok = call(worker, "Worker.DoTask", args, new(struct{}))
			}
			done <- true
			mr.registerChannel <- worker
		}(i)
	}

	for i := 0; i < ntasks; i++ {
		<-done
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
