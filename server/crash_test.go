package server

import (
	"sync"
	"testing"
	"time"
)

// 停止意图是「手动停机不该被算成崩溃」这条链路的唯一依据，
// 语义必须是一次停止只对应一次退出，且过期后不再盖住后面的真崩溃。
func TestCrashHandler_ExpectedStopIsConsumedOnce(t *testing.T) {
	h := &CrashHandler{}

	if h.TakeExpectedStop() {
		t.Fatal("没有任何停止动作时不该判定为用户停止")
	}

	h.MarkExpectedStop()

	if !h.TakeExpectedStop() {
		t.Fatal("刚标记过停止，第一次退出应当被识别为用户停止")
	}

	if h.TakeExpectedStop() {
		t.Fatal("一次停止只允许消费一次，后面的崩溃不能被它盖住")
	}
}

func TestCrashHandler_ExpectedStopExpiresOutsideWindow(t *testing.T) {
	h := &CrashHandler{}

	h.mu.Lock()
	h.expectedStop = time.Now().Add(-expectedStopWindow - time.Minute)
	h.mu.Unlock()

	if h.TakeExpectedStop() {
		t.Fatalf("超出 %s 有效窗口的停止意图不该仍然生效", expectedStopWindow)
	}
}

func TestCrashHandler_MarkOverridesStaleIntent(t *testing.T) {
	h := &CrashHandler{}

	h.mu.Lock()
	h.expectedStop = time.Now().Add(-expectedStopWindow - time.Minute)
	h.mu.Unlock()

	h.MarkExpectedStop()

	if !h.TakeExpectedStop() {
		t.Fatal("新的一次停止应当覆盖掉过期的记录")
	}
}

func TestCrashHandler_LastCrashRoundTrip(t *testing.T) {
	h := &CrashHandler{}

	if !h.LastCrashTime().IsZero() {
		t.Fatal("初始状态不应有崩溃时间")
	}

	ts := time.Now().Add(-time.Hour).Truncate(time.Second)
	h.SetLastCrash(ts)

	if got := h.LastCrashTime(); !got.Equal(ts) {
		t.Fatalf("回读崩溃时间不一致: got=%s want=%s", got, ts)
	}
}

// 崩溃处理与电源操作在不同 goroutine 里跑，这里配合 -race 验证锁的用法。
func TestCrashHandler_ConcurrentMarkAndTake(t *testing.T) {
	h := &CrashHandler{}
	var wg sync.WaitGroup

	for i := 0; i < 32; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			h.MarkExpectedStop()
		}()
		go func() {
			defer wg.Done()
			h.TakeExpectedStop()
		}()
	}

	wg.Wait()
}
