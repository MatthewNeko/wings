package remote

import (
	"context"
	"fmt"
)

// ExternalBackupStorage holds the credentials for one remote storage target.
// Nothing from this struct may ever be logged: the panel decrypts it purely for
// the duration of a single run and Wings keeps it in memory only.
type ExternalBackupStorage struct {
	Type        string `json:"type"`
	Host        string `json:"host"`
	Port        int    `json:"port"`
	Endpoint    string `json:"endpoint"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	Token       string `json:"token"`
	PrivateKey  string `json:"private_key"`
	Fingerprint string `json:"fingerprint"`
	RemotePath  string `json:"remote_path"`
	TLS         bool   `json:"tls"`
	Passive     bool   `json:"passive"`
}

// ExternalBackupInit is the task description the panel hands back before Wings
// starts archiving and uploading.
type ExternalBackupInit struct {
	Uuid           string                `json:"uuid"`
	Status         string                `json:"status"`
	RemotePath     string                `json:"remote_path"`
	IncludePaths   []string              `json:"include_paths"`
	ExcludePaths   []string              `json:"exclude_paths"`
	SpeedLimitKbps int64                 `json:"speed_limit_kbps"`
	KeepCount      int                   `json:"keep_count"`
	TimeoutMinutes int                   `json:"timeout_minutes"`
	Storage        ExternalBackupStorage `json:"storage"`
}

// ExternalBackupStatus is one progress sample sent back to the panel.
type ExternalBackupStatus struct {
	Status        string `json:"status"`
	Progress      int    `json:"progress"`
	ArchiveBytes  int64  `json:"archive_bytes,omitempty"`
	UploadedBytes int64  `json:"uploaded_bytes,omitempty"`
	SpeedKbps     int64  `json:"speed_kbps,omitempty"`
	Error         string `json:"error,omitempty"`
}

// GetExternalBackupInit fetches the full definition of a queued external backup
// run, including the remote credentials needed to reach the storage.
func (c *client) GetExternalBackupInit(ctx context.Context, server, run string) (ExternalBackupInit, error) {
	var data ExternalBackupInit
	res, err := c.Get(ctx, fmt.Sprintf("/servers/%s/external-backups/%s/init", server, run), nil)
	if err != nil {
		return data, err
	}
	defer res.Body.Close()

	if err := res.BindJSON(&data); err != nil {
		return data, err
	}

	return data, nil
}

// GetExternalBackupConfigInit fetches just the storage block for a saved
// configuration, which is all a connection test needs.
func (c *client) GetExternalBackupConfigInit(ctx context.Context, server, config string) (ExternalBackupInit, error) {
	var data ExternalBackupInit
	res, err := c.Get(ctx, fmt.Sprintf("/servers/%s/external-backups/configs/%s/init", server, config), nil)
	if err != nil {
		return data, err
	}
	defer res.Body.Close()

	if err := res.BindJSON(&data); err != nil {
		return data, err
	}

	return data, nil
}

// SendExternalBackupStatus reports progress or the final result of a run.
func (c *client) SendExternalBackupStatus(ctx context.Context, server, run string, data ExternalBackupStatus) error {
	res, err := c.Post(ctx, fmt.Sprintf("/servers/%s/external-backups/%s/status", server, run), data)
	if err != nil {
		return err
	}
	_ = res.Body.Close()

	return nil
}
