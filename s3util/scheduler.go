package s3util

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

type scheduler struct {
	tasks   chan *task
	workers chan struct{}
}

func newScheduler(numWorkers int) *scheduler {
	return &scheduler{
		tasks:   make(chan *task),
		workers: make(chan struct{}, numWorkers),
	}
}

func (s *scheduler) BatchSchedule(b *BatchTasks) error {
	done := make(chan error, len(b.tasks))
	count := 0
	for i := range b.tasks {
		t := b.tasks[i]
		t.done = done
		if err := s.schedule(t, &count); err != nil {
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

func (s *scheduler) schedule(t *task, count *int) error {
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

func (s *scheduler) worker(t *task) {
	for {
		err := t.taskFunc()
		t.done <- err
		select {
		case t = <-s.tasks:
		default:
			<-s.workers
			return
		}
	}
}
