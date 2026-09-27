package externalbackup

import (
	"context"
	"strings"

	"emperror.dev/errors"

	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/server"
	"github.com/pterodactyl/wings/server/remotestore"
)

// storageFrom copies the credential block the panel sent into the shape the
// protocol adapters take. It is explicit field by field on purpose: a new option
// has to be wired here deliberately rather than silently stop being passed down.
func storageFrom(s remote.ExternalBackupStorage) *remotestore.Storage {
	return &remotestore.Storage{
		Type:        s.Type,
		Host:        s.Host,
		Port:        s.Port,
		Endpoint:    s.Endpoint,
		Username:    s.Username,
		Password:    s.Password,
		Token:       s.Token,
		PrivateKey:  s.PrivateKey,
		Fingerprint: s.Fingerprint,
		RemotePath:  s.RemotePath,
		TLS:         s.TLS,
		Passive:     s.Passive,
	}
}

// Test dials the storage saved on a configuration and closes it again. It runs
// inline because the panel is waiting on the answer to show the user whether
// their credentials work.
//
// The reachability check has a tighter deadline than a real run: a user testing
// a wrong host should get an answer in seconds, not after the connection
// timeout ladder runs out.
func Test(ctx context.Context, s *server.Server, client remote.Client, configID string) error {
	init, err := client.GetExternalBackupConfigInit(ctx, s.ID(), configID)
	if err != nil {
		return errors.WrapIf(err, "externalbackup: unable to load the storage configuration from the panel")
	}

	uploader, err := remotestore.New(ctx, storageFrom(init.Storage))
	if err != nil {
		return errors.WrapIf(err, "externalbackup: unable to reach the remote storage")
	}
	defer func() {
		_ = uploader.Close()
	}()

	// Listing proves the target directory is usable too, which is what a user
	// wants to know before scheduling a backup. An empty directory means the
	// user's home or share root, where sftp cannot list by name, so the
	// successful dial above is already the answer.
	if dir := strings.Trim(init.Storage.RemotePath, "/"); dir != "" {
		if _, err := uploader.List(ctx, dir); err != nil {
			return errors.WrapIf(err, "externalbackup: the remote directory could not be listed")
		}
	}

	return nil
}
