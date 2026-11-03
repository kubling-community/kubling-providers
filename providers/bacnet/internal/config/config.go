// Package config loads and validates the BACnet provider process configuration.
package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultListenAddress       = ":50051"
	defaultShutdownGracePeriod = 10 * time.Second
	maximumShutdownGracePeriod = 5 * time.Minute
)

// Config is an immutable validated process configuration.
type Config struct {
	listenAddress       string
	shutdownGracePeriod time.Duration
	logLevel            slog.Level
	profileFiles        []string
	attachments         []Attachment
	scopes              []Scope
}

// ListenAddress returns the provider gRPC listen address.
func (c Config) ListenAddress() string {
	return c.listenAddress
}

// ShutdownGracePeriod bounds graceful gRPC shutdown.
func (c Config) ShutdownGracePeriod() time.Duration {
	return c.shutdownGracePeriod
}

// LogLevel returns the configured structured-log threshold.
func (c Config) LogLevel() slog.Level {
	return c.logLevel
}

// ProfileFiles returns detached absolute paths to injected profile documents.
func (c Config) ProfileFiles() []string {
	return append([]string(nil), c.profileFiles...)
}

// Attachments returns detached BACnet/IP socket configurations.
func (c Config) Attachments() []Attachment {
	return cloneAttachments(c.attachments)
}

// Scopes returns detached logical BACnet discovery scopes.
func (c Config) Scopes() []Scope {
	return cloneScopes(c.scopes)
}

// String exposes only non-sensitive process settings.
func (c Config) String() string {
	return fmt.Sprintf(
		"BACnet provider config (listen=%q shutdownGracePeriod=%s logLevel=%s profiles=%d attachments=%d scopes=%d)",
		c.listenAddress,
		c.shutdownGracePeriod,
		c.logLevel,
		len(c.profileFiles),
		len(c.attachments),
		len(c.scopes),
	)
}

// GoString exposes only non-sensitive process settings.
func (c Config) GoString() string {
	return c.String()
}

type fileConfig struct {
	Server      fileServer       `yaml:"server"`
	Logging     fileLogging      `yaml:"logging"`
	Profiles    fileProfiles     `yaml:"profiles"`
	Attachments []fileAttachment `yaml:"attachments"`
	Scopes      []fileScope      `yaml:"scopes"`
}

type fileServer struct {
	Listen              string `yaml:"listen"`
	ShutdownGracePeriod string `yaml:"shutdownGracePeriod"`
}

type fileLogging struct {
	Level string `yaml:"level"`
}

type fileProfiles struct {
	Files []string `yaml:"files"`
}

// Load reads and strictly validates one YAML document without opening sockets.
func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open BACnet provider config: %w", err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	var serialized fileConfig
	if err := decoder.Decode(&serialized); err != nil {
		return Config{}, fmt.Errorf("decode BACnet provider config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("decode BACnet provider config: multiple YAML documents are not supported")
		}
		return Config{}, fmt.Errorf("decode BACnet provider config: %w", err)
	}

	listenAddress, err := normalizeListenAddress(serialized.Server.Listen)
	if err != nil {
		return Config{}, err
	}
	shutdownGracePeriod, err := normalizeShutdownGracePeriod(serialized.Server.ShutdownGracePeriod)
	if err != nil {
		return Config{}, err
	}
	logLevel, err := normalizeLogLevel(serialized.Logging.Level)
	if err != nil {
		return Config{}, err
	}
	profileFiles, err := normalizeProfileFiles(path, serialized.Profiles.Files)
	if err != nil {
		return Config{}, err
	}
	attachments, scopes, err := normalizeTopology(serialized.Attachments, serialized.Scopes)
	if err != nil {
		return Config{}, err
	}
	return Config{
		listenAddress:       listenAddress,
		shutdownGracePeriod: shutdownGracePeriod,
		logLevel:            logLevel,
		profileFiles:        profileFiles,
		attachments:         attachments,
		scopes:              scopes,
	}, nil
}

func normalizeListenAddress(raw string) (string, error) {
	address := strings.TrimSpace(raw)
	if address == "" {
		address = defaultListenAddress
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("invalid listen address %q: %w", address, err)
	}
	if strings.ContainsAny(host, " \t\r\n") {
		return "", fmt.Errorf("invalid listen address %q: host contains whitespace", address)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", fmt.Errorf("invalid listen address %q: port must be between 1 and 65535", address)
	}
	return address, nil
}

func normalizeShutdownGracePeriod(raw string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return defaultShutdownGracePeriod, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid server.shutdownGracePeriod %q: %w", raw, err)
	}
	if duration <= 0 || duration > maximumShutdownGracePeriod {
		return 0, fmt.Errorf(
			"server.shutdownGracePeriod must be greater than zero and at most %s",
			maximumShutdownGracePeriod,
		)
	}
	return duration, nil
}

func normalizeLogLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid logging.level %q: expected debug, info, warn, or error", raw)
	}
}

func normalizeProfileFiles(configPath string, raw []string) ([]string, error) {
	absoluteConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return nil, fmt.Errorf("resolve BACnet provider config path: %w", err)
	}
	baseDirectory := filepath.Dir(absoluteConfigPath)
	result := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for index, value := range raw {
		path := strings.TrimSpace(value)
		if path == "" {
			return nil, fmt.Errorf("profiles.files[%d] must not be empty", index)
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(baseDirectory, path)
		}
		path = filepath.Clean(path)
		if _, exists := seen[path]; exists {
			return nil, fmt.Errorf("duplicate BACnet profile file %q", path)
		}
		seen[path] = struct{}{}
		result = append(result, path)
	}
	return result, nil
}
