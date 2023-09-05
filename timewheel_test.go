package timewheel

import (
	"testing"
	"time"
)

func TestLeveled(t *testing.T) {
	tw := NewLeveled(2, 2, time.Second)

	ch := make(chan string, 1)
	expected := "hello"
	tw.Start()

	tw.After(2*time.Second, func() {
		ch <- expected
	})

	select {
	case <-time.After(time.Second * 3):
		t.Errorf("got no data")
	case got := <-ch:
		if got != expected {
			t.Errorf("expected:%s,got:%s", expected, got)
		}
	}
}
