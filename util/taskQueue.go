package util

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Task interface {
	Execute()
}

type TaskQueue struct {
	ctx   context.Context
	workerPool chan *worker
	workerStore []*worker
	workerLock sync.Mutex
}

func NewTaskQueue(ctx context.Context, workerNumber int) *TaskQueue {
	queue:= &TaskQueue{
		ctx:		 ctx,				// used to cancel workers
		workerPool:  make(chan *worker),// has no buffer
		workerStore: make([]*worker, workerNumber, workerNumber),	// used to reset worker number
	}
	for i:= 0; i<workerNumber; i++ {
		w := newWorker(ctx, queue.workerPool)
		queue.workerStore[i] = w
		go w.start()
	}
	return queue
}

// Add a task to task queue
// AddTask() may be blocked when all workers are busy.
// In that case, the task sender may not be willing to wait.
// And it can use a ctx with timeout to do this.
func (q *TaskQueue) AddTask(ctx context.Context, t Task) error {
	defer func() {
		if err := recover(); err != nil {
			// Handle the really rare case when worker is already close.
			// In that case, a panic with "send to a closed channel" would occur.
			// And the task will lose
			// forever.
			fmt.Println(err)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case w, ok := <-q.workerPool:
			if !ok {
				return errors.New("task queue is dead")
			}
			if w.isAlive {
				w.taskCh <- t
				return nil
			}
		}
	}
}

// Change the number of workers
func (q *TaskQueue) SetWorkerNumber(newNumber int) error {
	q.workerLock.Lock()
	defer q.workerLock.Unlock()

	if newNumber <= 0 {
		return errors.New("fuck u")
	}

	if newNumber == len(q.workerStore) {
		return nil
	} else if newNumber > len(q.workerStore) {
		// Add some new workers
		w := newWorker(q.ctx, q.workerPool)
		q.workerStore = append(q.workerStore, w)
		go w.start()
		return nil
	} else {
		// Cancel some workers
		for _, w:= range q.workerStore[newNumber:len(q.workerStore)] {
			w.Cancel()
		}
		q.workerStore = q.workerStore[:newNumber]
		return nil
	}
}

type worker struct {
	ctx context.Context
	taskCh chan Task
	ready chan<- *worker
	isAlive bool

	Cancel context.CancelFunc
}

func newWorker(ctx context.Context, readyCh chan *worker) *worker {
	newCtx, cancelFunc := context.WithCancel(ctx)
	return &worker{
		ctx:    newCtx,
		Cancel: cancelFunc,
		taskCh: make(chan Task),	// taskCh has no buffer
		ready: readyCh,				// send self to ready channel when worker is ready
		isAlive: false,				// just for safe
	}
}

func (w *worker) start() {
	w.isAlive = true
	w.ready <- w
	defer func() {
		fmt.Println("task worker end")
		w.isAlive = false
		close(w.taskCh)
	}()
	for {
		select {
		case <-w.ctx.Done():
			return
		case task, ok := <-w.taskCh:
			if !ok {
				fmt.Println("taskCh closed")
				return
			}
			task.Execute()
			w.ready <- w
		}
	}
}



