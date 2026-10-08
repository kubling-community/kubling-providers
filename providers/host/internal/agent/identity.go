package agent

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const identityFilename = "agent-id"

// LoadOrCreateIdentity returns the durable opaque identifier stored in the
// agent state directory, creating it once with owner-only permissions.
func LoadOrCreateIdentity(stateDirectory string) (string, error) {
	stateDirectory = strings.TrimSpace(stateDirectory)
	if stateDirectory == "" {
		return "", errors.New("host-agent state directory is required")
	}
	if err := os.MkdirAll(stateDirectory, 0o700); err != nil {
		return "", fmt.Errorf("create host-agent state directory: %w", err)
	}
	path := filepath.Join(stateDirectory, identityFilename)
	identity, err := readIdentity(path)
	if err == nil {
		return identity, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	identity, err = generateIdentity()
	if err != nil {
		return "", err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return readIdentity(path)
	}
	if err != nil {
		return "", fmt.Errorf("create host-agent identity: %w", err)
	}
	writeErr := error(nil)
	if _, err := file.WriteString(identity + "\n"); err != nil {
		writeErr = fmt.Errorf("write host-agent identity: %w", err)
	} else if err := file.Sync(); err != nil {
		writeErr = fmt.Errorf("sync host-agent identity: %w", err)
	}
	closeErr := file.Close()
	if writeErr != nil {
		return "", errors.Join(writeErr, closeErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close host-agent identity: %w", closeErr)
	}
	return identity, nil
}

func readIdentity(path string) (string, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", os.ErrNotExist
		}
		return "", fmt.Errorf("read host-agent identity: %w", err)
	}
	identity := strings.TrimSpace(string(contents))
	if err := validateIdentity(identity); err != nil {
		return "", fmt.Errorf("invalid host-agent identity file: %w", err)
	}
	return identity, nil
}

func generateIdentity() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate host-agent identity: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func validateIdentity(identity string) error {
	if len(identity) != 32 {
		return errors.New("identity must contain 32 lowercase hexadecimal characters")
	}
	decoded, err := hex.DecodeString(identity)
	if err != nil || hex.EncodeToString(decoded) != identity {
		return errors.New("identity must contain 32 lowercase hexadecimal characters")
	}
	return nil
}
