package util

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

type testTask struct {
	value int
}

func newTask(v int) *testTask {
	return &testTask{
		value: v,
	}
}

func (t *testTask) Execute() {
	fmt.Printf("Task start %d\n", t.value)
	d, _ := time.ParseDuration(fmt.Sprintf("%dms", rand.Intn(500)))
	time.Sleep(d)
	t.value ++
	fmt.Printf("Task value %d\n", t.value)
}

func TestTaskQueue(t *testing.T) {
	q := NewTaskQueue(context.Background(), 3)
	task := newTask(0)
	for i:=0; i<10; i++ {
		if i==3 {
			t.Log("Change worker number to 1")
			err := q.SetWorkerNumber(1)
			if err != nil {
				t.Error(err)
			}
		}
		err := q.AddTask(context.Background(), task)
		if err != nil {
			t.Error(err)
		}
	}

	t.Log("Change worker number to 5")
	err := q.SetWorkerNumber(5)
	if err != nil {
		t.Error(err)
	}
	//time.Sleep(2000)
}
