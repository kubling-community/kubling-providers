package agent

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/kubling-community/kubling-providers/providers/host/internal/agent/collector"
	"gopkg.in/yaml.v3"
)

const defaultStateDirectory = "/var/lib/kubling/host-agent"

// BackoffConfig controls reconnect pacing after a gateway session ends.
type BackoffConfig struct {
	Initial time.Duration
	Maximum time.Duration
	Jitter  float64
}

// TLSConfig controls server authentication for the Agent Gateway.
type TLSConfig struct {
	CAFile     string
	ServerName string
}

// Config controls one host-agent process.
type Config struct {
	GatewayAddress     string
	Namespace          string
	StateDirectory     string
	Insecure           bool
	CredentialFile     string
	TLS                TLSConfig
	Hostname           string
	Attributes         map[string]string
	MaxConcurrentScans uint32
	ConnectTimeout     time.Duration
	Backoff            BackoffConfig
	Collector          collector.Config
}

type fileConfig struct {
	GatewayAddress     string              `yaml:"gatewayAddress"`
	Namespace          string              `yaml:"namespace"`
	StateDirectory     string              `yaml:"stateDirectory"`
	Insecure           bool                `yaml:"insecure"`
	CredentialFile     string              `yaml:"credentialFile"`
	TLS                fileTLSConfig       `yaml:"tls"`
	Hostname           string              `yaml:"hostname"`
	Attributes         map[string]string   `yaml:"attributes"`
	MaxConcurrentScans uint32              `yaml:"maxConcurrentScans"`
	ConnectTimeout     string              `yaml:"connectTimeout"`
	Backoff            fileBackoffConfig   `yaml:"backoff"`
	Collector          fileCollectorConfig `yaml:"collector"`
}

type fileTLSConfig struct {
	CAFile     string `yaml:"caFile"`
	ServerName string `yaml:"serverName"`
}

type fileBackoffConfig struct {
	Initial string   `yaml:"initial"`
	Maximum string   `yaml:"maximum"`
	Jitter  *float64 `yaml:"jitter"`
}

type fileCollectorConfig struct {
	IncludePseudoFilesystems bool `yaml:"includePseudoFilesystems"`
}

// DefaultConfig returns operational defaults that do not invent the required
// gateway address or namespace.
func DefaultConfig() Config {
	return Config{
		StateDirectory:     defaultStateDirectory,
		MaxConcurrentScans: 1,
		ConnectTimeout:     10 * time.Second,
		Backoff: BackoffConfig{
			Initial: time.Second,
			Maximum: 30 * time.Second,
			Jitter:  0.2,
		},
	}
}

// LoadConfig reads one strict YAML document and applies agent defaults.
func LoadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open host-agent config: %w", err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	var serialized fileConfig
	if err := decoder.Decode(&serialized); err != nil {
		return Config{}, fmt.Errorf("decode host-agent config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("decode host-agent config: multiple YAML documents are not supported")
		}
		return Config{}, fmt.Errorf("decode host-agent config: %w", err)
	}

	config := DefaultConfig()
	config.GatewayAddress = serialized.GatewayAddress
	config.Namespace = serialized.Namespace
	if serialized.StateDirectory != "" {
		config.StateDirectory = serialized.StateDirectory
	}
	config.Insecure = serialized.Insecure
	config.CredentialFile = serialized.CredentialFile
	config.TLS = TLSConfig{
		CAFile:     serialized.TLS.CAFile,
		ServerName: serialized.TLS.ServerName,
	}
	config.Hostname = serialized.Hostname
	config.Attributes = serialized.Attributes
	if serialized.MaxConcurrentScans != 0 {
		config.MaxConcurrentScans = serialized.MaxConcurrentScans
	}
	if serialized.ConnectTimeout != "" {
		config.ConnectTimeout, err = time.ParseDuration(serialized.ConnectTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse connectTimeout: %w", err)
		}
	}
	if serialized.Backoff.Initial != "" {
		config.Backoff.Initial, err = time.ParseDuration(serialized.Backoff.Initial)
		if err != nil {
			return Config{}, fmt.Errorf("parse backoff.initial: %w", err)
		}
	}
	if serialized.Backoff.Maximum != "" {
		config.Backoff.Maximum, err = time.ParseDuration(serialized.Backoff.Maximum)
		if err != nil {
			return Config{}, fmt.Errorf("parse backoff.maximum: %w", err)
		}
	}
	if serialized.Backoff.Jitter != nil {
		config.Backoff.Jitter = *serialized.Backoff.Jitter
	}
	config.Collector.IncludePseudoFilesystems = serialized.Collector.IncludePseudoFilesystems
	return config, nil
}

// NormalizeConfig validates and detaches a programmatically assembled config.
func NormalizeConfig(config Config) (Config, error) {
	normalized := config
	normalized.GatewayAddress = strings.TrimSpace(config.GatewayAddress)
	if normalized.GatewayAddress == "" {
		return Config{}, errors.New("gateway address is required")
	}
	normalized.Namespace = strings.TrimSpace(config.Namespace)
	if normalized.Namespace == "" {
		return Config{}, errors.New("namespace is required")
	}
	if normalized.Namespace != config.Namespace {
		return Config{}, errors.New("namespace must not contain surrounding whitespace")
	}
	normalized.StateDirectory = strings.TrimSpace(config.StateDirectory)
	if normalized.StateDirectory == "" {
		return Config{}, errors.New("state directory is required")
	}
	normalized.CredentialFile = strings.TrimSpace(config.CredentialFile)
	normalized.TLS.CAFile = strings.TrimSpace(config.TLS.CAFile)
	normalized.TLS.ServerName = strings.TrimSpace(config.TLS.ServerName)
	if normalized.Insecure {
		if normalized.CredentialFile != "" ||
			normalized.TLS.CAFile != "" || normalized.TLS.ServerName != "" {
			return Config{}, errors.New("insecure Agent Gateway mode cannot use credentials or TLS settings")
		}
	} else if normalized.CredentialFile == "" {
		return Config{}, errors.New("Agent Gateway credential file is required unless insecure mode is explicit")
	}
	normalized.Hostname = strings.TrimSpace(config.Hostname)
	if normalized.MaxConcurrentScans == 0 {
		return Config{}, errors.New("max concurrent scans must be positive")
	}
	if normalized.ConnectTimeout <= 0 {
		return Config{}, errors.New("connect timeout must be positive")
	}
	if normalized.Backoff.Initial <= 0 {
		return Config{}, errors.New("backoff initial duration must be positive")
	}
	if normalized.Backoff.Maximum < normalized.Backoff.Initial {
		return Config{}, errors.New("backoff maximum must be at least the initial duration")
	}
	if normalized.Backoff.Jitter < 0 || normalized.Backoff.Jitter > 1 {
		return Config{}, errors.New("backoff jitter must be between 0 and 1")
	}
	normalized.Attributes = make(map[string]string, len(config.Attributes))
	for key, value := range config.Attributes {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" || trimmed != key {
			return Config{}, fmt.Errorf("invalid attribute name %q", key)
		}
		normalized.Attributes[key] = value
	}
	return normalized, nil
}
