package timewheel

import (
	"container/list"
	"sync"
	"time"

	"github.com/google/uuid"
)

// TimeWheel TimeWheel
type TimeWheel struct {
	slots    []*list.List
	interval time.Duration
	total    time.Duration
	slot     int64
	pos      int64
	child    *TimeWheel
	parent   *TimeWheel
	level    int64
	doneCh   chan struct{}
	id2Pos   map[string]int64
	wg       *sync.WaitGroup
	executor Executor
	sync.RWMutex
	stopOnce  sync.Once
	startOnce sync.Once
}

type task struct {
	delay      time.Duration
	id         string
	cmd        func()
	circle     int64
	cycle      bool
	remainder  time.Duration
	reAdd      bool
	generation int
}

func (t *task) reset() {
	t.circle = 0
	t.remainder = 0
	t.reAdd = false
}

// Option Option
type Option func(*TimeWheel)

// WithExecutor WithExecutor
func WithExecutor(executor Executor) Option {
	return func(tw *TimeWheel) {
		tw.executor = executor
	}
}

// New New
func New(slot int64, interval time.Duration, opts ...Option) *TimeWheel {
	return NewLeveled(1, slot, interval, opts...)
}

// NewLeveled NewLeveled
func NewLeveled(level, slot int64, interval time.Duration, opts ...Option) *TimeWheel {
	var prev *TimeWheel
	var tw *TimeWheel
	wg := &sync.WaitGroup{}
	for curLevel := int64(0); curLevel < level; curLevel++ {
		var cur *TimeWheel
		if prev == nil {
			cur = newTimeWheel(slot, interval, opts...)
		} else {
			cur = newTimeWheel(slot, prev.total, opts...)
		}
		cur.parent = prev
		cur.level = curLevel
		cur.wg = wg
		if prev != nil {
			prev.child = cur
		}
		if curLevel == 0 {
			tw = cur
		}
		prev = cur
	}

	return tw
}

func newTimeWheel(slot int64, interval time.Duration, opts ...Option) *TimeWheel {
	ltw := &TimeWheel{
		slots:    make([]*list.List, slot),
		interval: interval,
		slot:     slot,
		doneCh:   make(chan struct{}),
		total:    time.Duration(slot) * interval,
		id2Pos:   make(map[string]int64),
	}
	for _, opt := range opts {
		opt(ltw)
	}
	if ltw.executor == nil {
		ltw.executor = NewPooledExecutor(100)
	}

	for i := int64(0); i < slot; i++ {
		ltw.slots[i] = list.New()
	}
	return ltw
}

// Start Start
func (tw *TimeWheel) Start() {
	tw.startOnce.Do(func() {
		child := tw.child
		if child != nil {
			child.Start()
		}

		go tw.start()
	})
}

func (tw *TimeWheel) start() {
	tw.wg.Add(1)
	ticker := time.NewTicker(tw.interval)

	defer func() {
		tw.wg.Done()
		ticker.Stop()
	}()
	for {
		select {
		case <-ticker.C:
			tw.tickerHandler()

		case <-tw.doneCh:
			return
		}
	}
}
func (tw *TimeWheel) tickerHandler() {
	if tw.pos == tw.slot-1 {
		tw.pos = 0
	} else {
		tw.pos++
	}

	tw.Lock()
	defer tw.Unlock()
	slot := tw.slots[tw.pos]

	for e := slot.Front(); e != nil; e = e.Next() {
		task := e.Value.(*task)

		if task.circle > 0 {
			task.circle--
			continue
		}

		slot.Remove(e)
		delete(tw.id2Pos, task.id)
		if task.remainder != 0 && tw.parent != nil {

			task.reAdd = true
			tw.parent.add(task)
			continue
		}

		tw.executor.Exec(func() {
			task.cmd()
			if task.cycle {
				task.reset()
				task.generation++
				tw.addInLock(task)
			}
		})

	}
}

func (tw *TimeWheel) getPostAndCircleAndRemainder(delay time.Duration) (int64, int64, time.Duration) {
	delaySeconds := int64(delay.Seconds())
	intervalSeconds := int64(tw.interval.Seconds())
	circle := delaySeconds / (intervalSeconds * tw.slot)
	if circle >= 1 {
		circle = circle - 1
	}

	remainderAll := delaySeconds - tw.slot*circle*intervalSeconds
	pos := remainderAll / intervalSeconds
	remainder := remainderAll % intervalSeconds
	return (tw.pos + pos) % tw.slot, circle, time.Duration(remainder) * time.Second
}

func (tw *TimeWheel) add(task *task) {
	tw.Lock()
	defer tw.Unlock()
	tw.addInLock2(task)
}

func (tw *TimeWheel) After(d time.Duration, cmd func()) string {
	id := uuid.New().String()
	task := &task{
		id:    id,
		cmd:   cmd,
		delay: d,
	}
	tw.add(task)
	return id
}

func (tw *TimeWheel) Every(d time.Duration, cmd func()) string {
	id := uuid.New().String()
	task := &task{
		id:    id,
		cmd:   cmd,
		delay: d,
		cycle: true,
	}
	tw.add(task)
	return id
}

func (tw *TimeWheel) addInLock2(task *task) {

	delay := task.delay
	if task.reAdd {
		delay = task.remainder
	}

	if delay < tw.interval && tw.parent != nil {
		tw.parent.add(task)
		return
	}

	if delay > tw.total && tw.child != nil {
		tw.child.add(task)
		return
	}

	pos, circle, remainder := tw.getPostAndCircleAndRemainder(delay)

	task.circle = circle
	task.remainder = remainder
	tw.slots[pos].PushBack(task)
	tw.id2Pos[task.id] = pos
}

func (tw *TimeWheel) addInLock(task *task) {

	delay := task.delay
	if task.reAdd {
		delay = task.remainder
	}

	if delay < tw.interval && tw.parent != nil {
		tw.parent.add(task)
		return
	}

	if delay > tw.total && tw.child != nil {
		tw.child.add(task)
		return
	}

	pos, circle, remainder := tw.getPostAndCircleAndRemainder(delay)

	task.circle = circle
	task.remainder = remainder
	tw.slots[pos].PushBack(task)
	tw.id2Pos[task.id] = pos
}

type delMode int

const (
	delModeFromRaw delMode = iota
	delModeFromChild
	delModeFromParent
)

func (tw *TimeWheel) del(id string, delMode delMode) {
	if delMode != delModeFromParent && tw.parent != nil {
		tw.parent.del(id, delModeFromChild)
	}
	if delMode != delModeFromChild && tw.child != nil {
		tw.child.del(id, delModeFromParent)
	}
	tw.Lock()
	defer tw.Unlock()
	pos, exist := tw.id2Pos[id]
	if !exist {
		return
	}
	slot := tw.slots[pos]
	for e := slot.Front(); e != nil; e = e.Next() {
		task := e.Value.(*task)
		if task.id == id {
			slot.Remove(e)
		}
	}
	delete(tw.id2Pos, id)
}

func (tw *TimeWheel) Del(id string) {
	tw.del(id, delModeFromRaw)
}

func (tw *TimeWheel) Stop() {
	tw.stopOnce.Do(func() {
		child := tw.child
		if child != nil {
			child.Stop()
		}
		tw.executor.Close()
		close(tw.doneCh)
	})
}

func (tw *TimeWheel) GracefulStop() {
	tw.Stop()
	tw.wg.Wait()
}
