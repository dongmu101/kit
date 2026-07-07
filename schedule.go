package kit

import (
	"container/heap"
	"context"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

const minRecurringDuration = time.Millisecond

type (
	event struct {
		id           uint64
		endTime      time.Time
		duration     time.Duration
		recurring    bool
		nextTime     func(time.Time) time.Time
		delayHandler DelayHandler
		index        int
	}
	eventHeap []*event
	// DelayHandler 调度程序 需要传递参数通过 struct 传递
	DelayHandler interface {
		OnTimer()
	}
	// Schedule 时间表
	// 命名也只是不想与标准库Timer重名而已
	// 定时时间精确到秒，不精确管理goroutine的退出
	// 主要逻辑只不过是对标准库Timer、Ticker封装管理而已
	Schedule struct {
		idGen        uint64
		mutex        sync.Mutex
		events       map[uint64]*event
		heap         eventHeap
		intervalTime time.Duration
		wakeup       chan struct{}
		handlerLimit chan struct{}
	}
)

func NewSchedule() *Schedule {
	s := &Schedule{
		events:       make(map[uint64]*event),
		intervalTime: time.Second,
		wakeup:       make(chan struct{}, 1),
		handlerLimit: make(chan struct{}, defaultMaxConcurrentHandlers()),
	}
	heap.Init(&s.heap)
	return s
}

func (s *Schedule) Run(ctx context.Context) {
	go s.Start(ctx)
}

// Start 开始
func (s *Schedule) Start(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		wait := s.nextWait()
		if wait < 0 {
			select {
			case <-s.wakeup:
			case <-ctx.Done():
				return
			}
			continue
		}

		resetTimer(timer, wait)
		select {
		case <-timer.C:
			s.fireDue()
		case <-s.wakeup:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
	}
}

// Add 添加
func (s *Schedule) Add(delayHandler DelayHandler, duration time.Duration, persistence bool) (TID uint64) {
	if isNilDelayHandler(delayHandler) {
		return 0
	}
	if duration < 0 {
		duration = 0
	}
	if duration == 0 {
		persistence = false
	}
	if persistence && duration < minRecurringDuration {
		return 0
	}

	s.mutex.Lock()
	s.idGen++
	TID = s.idGen
	ev := &event{
		id:           TID,
		endTime:      time.Now().Add(duration),
		duration:     duration,
		recurring:    persistence,
		delayHandler: delayHandler,
		index:        -1,
	}
	s.pushEventLocked(ev)
	s.mutex.Unlock()
	s.notify()
	return
}

// AddDate 按指定日期时间添加一次性任务
func (s *Schedule) AddDate(delayHandler DelayHandler, date time.Time) (TID uint64) {
	duration := time.Until(date)
	if duration < 0 {
		duration = 0
	}
	return s.Add(delayHandler, duration, false)
}

// AddDaily 每天在指定时刻执行
func (s *Schedule) AddDaily(delayHandler DelayHandler, hour, minute, second int, persistence bool) (TID uint64) {
	now := time.Now()
	first := nextDailyTime(now, hour, minute, second)
	return s.addCalendarDate(delayHandler, first, persistence, func(last time.Time) time.Time {
		return nextDailyTime(last, hour, minute, second)
	})
}

// AddWeekly 每周在指定星期和时刻执行
func (s *Schedule) AddWeekly(delayHandler DelayHandler, weekday time.Weekday, hour, minute, second int, persistence bool) (TID uint64) {
	now := time.Now()
	first := nextWeeklyTime(now, weekday, hour, minute, second)
	return s.addCalendarDate(delayHandler, first, persistence, func(last time.Time) time.Time {
		return nextWeeklyTime(last, weekday, hour, minute, second)
	})
}

// AddMonthly 每月在指定日期和时刻执行
// 当目标月份不存在该日期时，自动使用该月最后一天。
func (s *Schedule) AddMonthly(delayHandler DelayHandler, day, hour, minute, second int, persistence bool) (TID uint64) {
	now := time.Now()
	first := nextMonthlyTime(now, day, hour, minute, second)
	return s.addCalendarDate(delayHandler, first, persistence, func(last time.Time) time.Time {
		return nextMonthlyTime(last, day, hour, minute, second)
	})
}

func (s *Schedule) addCalendarDate(delayHandler DelayHandler, first time.Time, persistence bool, next func(time.Time) time.Time) (TID uint64) {
	if isNilDelayHandler(delayHandler) {
		return 0
	}
	if first.Before(time.Now()) {
		first = time.Now()
	}

	s.mutex.Lock()
	s.idGen++
	TID = s.idGen
	ev := &event{
		id:           TID,
		endTime:      first,
		recurring:    persistence,
		nextTime:     next,
		delayHandler: delayHandler,
		index:        -1,
	}
	s.pushEventLocked(ev)
	s.mutex.Unlock()
	s.notify()
	return
}

func nextDailyTime(from time.Time, hour, minute, second int) time.Time {
	next := time.Date(from.Year(), from.Month(), from.Day(), hour, minute, second, 0, from.Location())
	if !next.After(from) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}

func nextWeeklyTime(from time.Time, weekday time.Weekday, hour, minute, second int) time.Time {
	next := time.Date(from.Year(), from.Month(), from.Day(), hour, minute, second, 0, from.Location())
	diff := (int(weekday) - int(from.Weekday()) + 7) % 7
	next = next.AddDate(0, 0, diff)
	if !next.After(from) {
		next = next.AddDate(0, 0, 7)
	}
	return next
}

func nextMonthlyTime(from time.Time, day, hour, minute, second int) time.Time {
	year, month := from.Year(), from.Month()
	next := monthlyDate(year, month, day, hour, minute, second, from.Location())
	if !next.After(from) {
		year, month = nextMonth(year, month)
		next = monthlyDate(year, month, day, hour, minute, second, from.Location())
	}
	return next
}

func monthlyDate(year int, month time.Month, day, hour, minute, second int, loc *time.Location) time.Time {
	lastDay := daysInMonth(year, month)
	if day < 1 {
		day = 1
	}
	if day > lastDay {
		day = lastDay
	}
	return time.Date(year, month, day, hour, minute, second, 0, loc)
}

func nextMonth(year int, month time.Month) (int, time.Month) {
	if month == time.December {
		return year + 1, time.January
	}
	return year, month + 1
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// Remove 移除  备注：只保证取消未触发，不保证取消已经到期的触发
func (s *Schedule) Remove(id uint64) {
	s.mutex.Lock()
	if ev, ok := s.events[id]; ok {
		if ev.index >= 0 {
			heap.Remove(&s.heap, ev.index)
		}
		delete(s.events, id)
	}
	s.mutex.Unlock()
	s.notify()
}

// Surplus 剩余
func (s *Schedule) Surplus(id uint64) (duration time.Duration) {
	duration, _ = s.SurplusOK(id)
	return
}

// SurplusOK 返回任务剩余时间，并通过 ok 区分任务是否存在。
func (s *Schedule) SurplusOK(id uint64) (duration time.Duration, ok bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	ev, ok := s.events[id]
	if !ok {
		return 0, false
	}
	duration = ev.endTime.Sub(time.Now())
	if duration < 0 {
		duration = 0
	}
	return duration, true
}

func (s *Schedule) nextWait() time.Duration {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if len(s.heap) == 0 {
		return -1
	}
	wait := time.Until(s.heap[0].endTime)
	if wait < 0 {
		return 0
	}
	return wait
}

func (s *Schedule) fireDue() {
	var handlers []DelayHandler
	s.mutex.Lock()
	now := time.Now()
	for len(s.heap) > 0 {
		ev := s.heap[0]
		if ev.endTime.After(now) {
			break
		}

		heap.Pop(&s.heap)
		handlers = append(handlers, ev.delayHandler)

		if !ev.recurring {
			delete(s.events, ev.id)
			continue
		}

		s.scheduleNextLocked(ev, now)
		heap.Push(&s.heap, ev)
	}
	s.mutex.Unlock()

	for _, handler := range handlers {
		s.callHandler(handler)
	}
}

func (s *Schedule) callHandler(handler DelayHandler) {
	s.handlerLimit <- struct{}{}
	go func() {
		defer func() {
			<-s.handlerLimit
		}()
		safeCall(handler)
	}()
}

func (s *Schedule) scheduleNextLocked(ev *event, now time.Time) {
	switch {
	case ev.nextTime != nil:
		next := ev.nextTime(ev.endTime)
		for !next.After(now) {
			next = ev.nextTime(next)
		}
		ev.endTime = next
	case ev.duration > 0:
		ev.endTime = nextIntervalTime(ev.endTime, now, ev.duration)
	default:
		ev.endTime = now
	}
}

func nextIntervalTime(last, now time.Time, interval time.Duration) time.Time {
	if interval <= 0 {
		return now
	}
	if next := last.Add(interval); next.After(now) {
		return next
	}

	elapsed := now.Sub(last)
	const maxDuration = time.Duration(1<<63 - 1)
	if elapsed > maxDuration-interval {
		return now.Add(interval)
	}

	steps := elapsed/interval + 1
	return last.Add(steps * interval)
}

func (s *Schedule) pushEventLocked(ev *event) {
	s.events[ev.id] = ev
	heap.Push(&s.heap, ev)
}

func (s *Schedule) notify() {
	select {
	case s.wakeup <- struct{}{}:
	default:
	}
}

func safeCall(d DelayHandler) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintf(os.Stderr, "schedule: handler panic: %v\n%s\n", err, debug.Stack())
		}
	}()
	d.OnTimer()
}

func isNilDelayHandler(delayHandler DelayHandler) bool {
	if delayHandler == nil {
		return true
	}
	v := reflect.ValueOf(delayHandler)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func defaultMaxConcurrentHandlers() int {
	n := runtime.GOMAXPROCS(0) * 1000
	if n < 4 {
		return 4
	}
	return n
}

func resetTimer(timer *time.Timer, wait time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(wait)
}

func (h eventHeap) Len() int {
	return len(h)
}

func (h eventHeap) Less(i, j int) bool {
	return h[i].endTime.Before(h[j].endTime)
}

func (h eventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *eventHeap) Push(x interface{}) {
	ev := x.(*event)
	ev.index = len(*h)
	*h = append(*h, ev)
}

func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	ev := old[n-1]
	ev.index = -1
	*h = old[:n-1]
	return ev
}
