package remote

import (
	"context"
	"fmt"
)

// CrashReport 是 Wings 在进程退出时上报给面板的一条事件。面板据此做
// 退出码归因与崩溃计数，并答复这次是否允许自动重启。
type CrashReport struct {
	ExitCode     uint32 `json:"exit_code"`
	OomKilled    bool   `json:"oom_killed"`
	ExpectedStop bool   `json:"expected_stop"`
	CrashedAt    int64  `json:"crashed_at"`
}

// CrashDecision 是面板对这条崩溃的答复。
type CrashDecision struct {
	AllowRestart bool   `json:"allow_restart"`
	Reason       string `json:"reason"`
	// Count 是面板统计的窗口内崩溃次数，方便节点日志与面板对得上。
	Count int `json:"count"`
}

// SendCrashReport posts one exit event and returns the panel's decision.
func (c *client) SendCrashReport(ctx context.Context, server string, data CrashReport) (CrashDecision, error) {
	var decision CrashDecision

	res, err := c.Post(ctx, fmt.Sprintf("/servers/%s/crash", server), data)
	if err != nil {
		return decision, err
	}
	defer res.Body.Close()

	if err := res.BindJSON(&decision); err != nil {
		return decision, err
	}

	return decision, nil
}
