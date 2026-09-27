package server

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"emperror.dev/errors"

	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/environment"
	"github.com/pterodactyl/wings/remote"
)

type CrashHandler struct {
	mu sync.RWMutex

	// Tracks the time of the last server crash event.
	lastCrash time.Time

	// 用户主动停止、重启或强杀时记下时间，用来把「正常停机」和
	// 「进程崩了」区分开，否则每次手动停止都会被算进崩溃计数。
	expectedStop time.Time
}

// expectedStopWindow 是停止意图的有效时长。停止到进程真正退出
// 之间可能有几十秒（世界保存），留足余量但不至于盖住下一次真崩溃。
const expectedStopWindow = 3 * time.Minute

// MarkExpectedStop 记录一次由人发起的停止动作。
func (cd *CrashHandler) MarkExpectedStop() {
	cd.mu.Lock()
	cd.expectedStop = time.Now()
	cd.mu.Unlock()
}

// TakeExpectedStop 消费掉一次停止意图：一次停止只对应一次退出事件，
// 取完就清零，避免后面的真崩溃被误判成用户停止。
func (cd *CrashHandler) TakeExpectedStop() bool {
	cd.mu.Lock()
	defer cd.mu.Unlock()

	if cd.expectedStop.IsZero() || time.Since(cd.expectedStop) > expectedStopWindow {
		return false
	}

	cd.expectedStop = time.Time{}

	return true
}

// Returns the time of the last crash for this server instance.
func (cd *CrashHandler) LastCrashTime() time.Time {
	cd.mu.RLock()
	defer cd.mu.RUnlock()

	return cd.lastCrash
}

// Sets the last crash time for a server.
func (cd *CrashHandler) SetLastCrash(t time.Time) {
	cd.mu.Lock()
	cd.lastCrash = t
	cd.mu.Unlock()
}

// Looks at the environment exit state to determine if the process exited cleanly or
// if it was the result of an event that we should try to recover from.
//
// This function assumes it is called under circumstances where a crash is suspected
// of occurring. It will not do anything to determine if it was actually a crash, just
// look at the exit state and check if it meets the criteria of being called a crash
// by Wings.
//
// If the server is determined to have crashed, the process will be restarted and the
// counter for the server will be incremented.
func (s *Server) handleServerCrash() error {
	// 进程不在线时没什么可判断的，先拿到退出状态再决定要不要报给面板。
	if s.Environment.State() != environment.ProcessOfflineState {
		return nil
	}

	exitCode, oomKilled, err := s.Environment.ExitState()
	if err != nil {
		return errors.Wrap(err, "无法获取服务器进程的退出状态")
	}

	// 一次停止意图只对应一次退出，取走之后剩下的才可能是真崩溃。
	expectedStop := s.crasher.TakeExpectedStop()
	isCrash := oomKilled || exitCode != 0 || config.Get().System.CrashDetection.DetectCleanExitAsCrash

	decision, derr := s.reportCrashToPanel(exitCode, oomKilled, expectedStop)
	if derr != nil {
		// 面板够不着时不该把重启能力一起丢掉：
		// 退回 Wings 原本的时间窗判断。
		s.Log().WithField("error", derr).Warn("无法向面板上报进程退出事件，改用本地崩溃检测策略")
		if !isCrash {
			return nil
		}

		return s.handleLocalCrash(exitCode, oomKilled)
	}

	if !isCrash {
		s.Log().Debug("服务器退出并成功退出代码，已按正常退出记录")

		return nil
	}

	if !s.Config().CrashDetectionEnabled {
		s.Log().Debug("服务器触发了崩溃检测，但处理程序已禁用服务器进程")
		s.PublishConsoleOutputFromDaemon("中止自动重启，此实例禁用崩溃检测。")

		return nil
	}

	s.publishCrashNotice(exitCode, oomKilled)

	// 重启与否由面板决定：崩溃计数、退出码归因和用户的重启策略都在那边。
	if !decision.AllowRestart {
		reason := decision.Reason
		if reason == "" {
			reason = "面板的自动重启策略阻止了本次重启。"
		}

		s.PublishConsoleOutputFromDaemon(reason)
		s.Log().WithField("reason", reason).WithField("crash_count", decision.Count).
			Info("面板判定本次不自动重启")

		return nil
	}

	s.crasher.SetLastCrash(time.Now())

	return errors.Wrap(s.HandlePowerAction(PowerActionStart), "检测到崩溃后无法启动服务器")
}

// publishCrashNotice 把崩溃现场打到控制台，让玩家至少知道进程是怎么没的。
func (s *Server) publishCrashNotice(exitCode uint32, oomKilled bool) {
	s.PublishConsoleOutputFromDaemon("---------- 检测到服务器进程处于崩溃状态！ ----------")
	s.PublishConsoleOutputFromDaemon(fmt.Sprintf("退出代码: %d", exitCode))
	s.PublishConsoleOutputFromDaemon(fmt.Sprintf("内存不足: %t", oomKilled))
}

// reportCrashToPanel 上报一次进程退出并取回面板的重启决定。
func (s *Server) reportCrashToPanel(exitCode uint32, oomKilled, expectedStop bool) (remote.CrashDecision, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	return s.client.SendCrashReport(ctx, s.ID(), remote.CrashReport{
		ExitCode:     exitCode,
		OomKilled:    oomKilled,
		ExpectedStop: expectedStop,
		CrashedAt:    time.Now().Unix(),
	})
}

// handleLocalCrash 是面板不可达时的兜底，沿用 Wings 原本的时间窗逻辑。
func (s *Server) handleLocalCrash(exitCode uint32, oomKilled bool) error {
	if !s.Config().CrashDetectionEnabled {
		s.PublishConsoleOutputFromDaemon("中止自动重启，此实例禁用崩溃检测。")

		return nil
	}

	s.publishCrashNotice(exitCode, oomKilled)

	c := s.crasher.LastCrashTime()
	timeout := config.Get().System.CrashDetection.Timeout

	// 上一次崩溃发生在时间窗之内就不再重启，避免无限重启把节点拖死。
	if timeout != 0 && !c.IsZero() && c.Add(time.Second*time.Duration(timeout)).After(time.Now()) {
		s.PublishConsoleOutputFromDaemon("正在中止自动重启，上次崩溃发生在 " + strconv.Itoa(timeout) + " 秒内。")

		return &crashTooFrequent{}
	}

	s.crasher.SetLastCrash(time.Now())

	return errors.Wrap(s.HandlePowerAction(PowerActionStart), "检测到崩溃后无法启动服务器")
}
