package kit

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var schedule *Schedule

func TestXxx(t *testing.T) {
	schedule = NewSchedule()
	var context = context.Background()
	schedule.Run(context)
	var job3 Job
	job3.TableId = 10000
	schedule.Add(&job3, time.Second*time.Duration(5), false)
	for i := 0; i < 10; i++ {
		var job Job
		job.TableId = i
		schedule.Add(&job, time.Second*time.Duration(i+1), false)

		var job2 Job
		job2.TableId = i + 10
		schedule.Add(&job2, time.Second*time.Duration(i), false)
	}
	time.Sleep(time.Second * 11)
}

type Job struct {
	TableId int
}

func (j *Job) OnTimer() {
	fmt.Println(j.TableId)
}

type blockingJob struct {
	started chan struct{}
	release chan struct{}
	active  *int32
	max     *int32
}

func (j *blockingJob) OnTimer() {
	active := atomic.AddInt32(j.active, 1)
	for {
		max := atomic.LoadInt32(j.max)
		if active <= max || atomic.CompareAndSwapInt32(j.max, max, active) {
			break
		}
	}
	j.started <- struct{}{}
	<-j.release
	atomic.AddInt32(j.active, -1)
}

func TestScheduleRejectsTooShortRecurringDuration(t *testing.T) {
	s := NewSchedule()
	job := &Job{}

	if id := s.Add(job, time.Nanosecond, true); id != 0 {
		t.Fatalf("expected too-short recurring task to be rejected, got id=%d", id)
	}
	if id := s.Add(job, time.Nanosecond, false); id == 0 {
		t.Fatal("expected one-time short task to be accepted")
	}
}

func TestScheduleLimitsConcurrentHandlers(t *testing.T) {
	s := NewSchedule()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run(ctx)

	limit := cap(s.handlerLimit)
	started := make(chan struct{}, limit+3)
	release := make(chan struct{})
	var active int32
	var max int32

	for i := 0; i < limit+3; i++ {
		job := &blockingJob{
			started: started,
			release: release,
			active:  &active,
			max:     &max,
		}
		if id := s.Add(job, 0, false); id == 0 {
			t.Fatal("expected immediate task to be accepted")
		}
	}

	for i := 0; i < limit; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for handler %d to start", i+1)
		}
	}

	select {
	case <-started:
		t.Fatalf("expected at most %d concurrent handlers, got extra start", limit)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for queued handler %d to start", i+1)
		}
	}

	if got := atomic.LoadInt32(&max); got > int32(limit) {
		t.Fatalf("expected max concurrency <= %d, got %d", limit, got)
	}
}
