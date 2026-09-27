package router

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/pterodactyl/wings/router/externalbackup"
	"github.com/pterodactyl/wings/router/middleware"
)

// postServerExternalBackup starts an archive-and-upload run against the storage
// the user configured. Credentials are never part of this request: the daemon
// fetches them from the panel once it is actually time to use them.
func postServerExternalBackup(c *gin.Context) {
	s := middleware.ExtractServer(c)
	client := middleware.ExtractApiClient(c)

	var data struct {
		RemotePath     string `json:"remote_path"`
		TimeoutMinutes int    `json:"timeout_minutes"`
	}
	if err := c.BindJSON(&data); err != nil {
		return
	}

	timeout := time.Duration(data.TimeoutMinutes) * time.Minute
	if err := externalbackup.Start(s, client, c.Param("run"), timeout); err != nil {
		// The panel owns the queue, so a collision means another run for this
		// server is still going and the request should be retried later rather
		// than forced through.
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": err.Error()})

		return
	}

	c.Status(http.StatusAccepted)
}

// deleteServerExternalBackup cancels a run that this daemon is working on.
func deleteServerExternalBackup(c *gin.Context) {
	if !externalbackup.Cancel(c.Param("run")) {
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": "That external backup is not running on this daemon.",
		})

		return
	}

	c.Status(http.StatusNoContent)
}

// postServerExternalBackupTest checks saved credentials without producing a
// backup. It answers inline because the panel is showing the result to a user
// who is deciding whether the settings are right.
func postServerExternalBackupTest(c *gin.Context) {
	s := middleware.ExtractServer(c)
	client := middleware.ExtractApiClient(c)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 45*time.Second)
	defer cancel()

	if err := externalbackup.Test(ctx, s, client, c.Param("config")); err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "error": err.Error()})

		return
	}

	c.JSON(http.StatusOK, gin.H{"ok": true})
}
