package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	//count := 0
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++{
		taskarg := DoTaskArgs{JobName:jobName, File:mapFiles[i], Phase:phase, TaskNumber:i, NumOtherPhase:n_other}
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker := <-registerChan
			for {
				success := call(worker, "Worker.DoTask", taskarg, nil)
				if success{
					break
				}else{
					tmp := <-registerChan
					go func() {
						registerChan <- worker
					}()
					worker = tmp
				}
			}
			go func() {
				registerChan <- worker//不断填在通道上，当不需要worker如果不调出来会被堵塞
			}()
		}()
	}
	wg.Wait()


	fmt.Printf("Schedule: %v done\n", phase)
}
