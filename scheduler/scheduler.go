// Copyright 2021-present PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package scheduler

type task struct {
	taskFunc func() error
	done     chan error
}

type BatchTasks struct {
	tasks []*task
}

func NewBatchTasks() *BatchTasks {
	return &BatchTasks{}
}

func (b *BatchTasks) AppendTask(f func() error) {
	b.tasks = append(b.tasks, &task{
		taskFunc: f,
	})
}

type Scheduler struct {
	tasks   chan *task
	workers chan struct{}
}

func NewScheduler(numWorkers int) *Scheduler {
	return &Scheduler{
		tasks:   make(chan *task),
		workers: make(chan struct{}, numWorkers),
	}
}

func (s *Scheduler) BatchSchedule(b *BatchTasks) error {
	done := make(chan error, len(b.tasks))
	count := 0
	for i := range b.tasks {
		t := b.tasks[i]
		t.done = done
		if err := s.scheduleBatchTask(t, &count); err != nil {
			return err
		}
	}
	for count < len(b.tasks) {
		err := <-done
		count++
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) scheduleBatchTask(t *task, count *int) error {
	for {
		select {
		case err := <-t.done:
			*count++
			if err != nil {
				return err
			}
		case s.tasks <- t:
			return nil
		case s.workers <- struct{}{}:
			go s.worker(t)
			return nil
		}
	}
}

func (s *Scheduler) Schedule(f func()) {
	t := &task{
		taskFunc: func() error {
			f()
			return nil
		},
	}
	select {
	case s.tasks <- t:
	case s.workers <- struct{}{}:
		go s.worker(t)
	}
}

func (s *Scheduler) worker(t *task) {
	for {
		err := t.taskFunc()
		if t.done != nil {
			t.done <- err
		}
		select {
		case t = <-s.tasks:
		default:
			<-s.workers
			return
		}
	}
}

type PermanentScheduler struct {
	tasks   chan func()
	closeCh chan struct{}
}

func NewPermanentScheduler(numWorkers, capacity int) *PermanentScheduler {
	s := &PermanentScheduler{
		tasks:   make(chan func(), capacity),
		closeCh: make(chan struct{}),
	}
	if numWorkers < 1 {
		numWorkers = 1
	}
	for i := 0; i < numWorkers; i++ {
		go s.worker()
	}
	return s
}

func (s *PermanentScheduler) Close() {
	close(s.closeCh)
}

func (s *PermanentScheduler) Schedule(f func()) {
	s.tasks <- f
}

func (s *PermanentScheduler) worker() {
	for {
		select {
		case f := <-s.tasks:
			if f != nil {
				f()
			}
		case <-s.closeCh:
			return
		}
	}
}
