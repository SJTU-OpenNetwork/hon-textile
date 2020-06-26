package util

import "sync"

// SyncMap is a thread safe map
type SyncMap struct {
	data map[string]interface{}
	lock sync.Mutex
}

func NewSyncMap() *SyncMap{
	return &SyncMap{
		data: make(map[string]interface{}),
		lock: sync.Mutex{},
	}
}

func (s *SyncMap) Push(key string, x interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.data[key] = x
}

func (s *SyncMap) Pop(key string) interface{} {
	s.lock.Lock()
	defer s.lock.Unlock()
	res, ok := s.data[key]
	if !ok {
		return nil
	}
	delete(s.data, key)
	return res
}

func (s *SyncMap) Get(key string) interface{} {
	s.lock.Lock()
	defer s.lock.Unlock()
	res, ok := s.data[key]
	if !ok {
		return nil
	}
	return res
}