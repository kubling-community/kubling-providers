package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kubling-community/kubling-providers/providers/host/internal/agent"
	"github.com/kubling-community/kubling-providers/providers/host/internal/agent/collector"
)

var version = "development"

func main() {
	config, err := resolveConfig(os.Args[1:], os.Getenv)
	if err != nil {
		slog.Error("host agent configuration is invalid", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if err := run(ctx, config); err != nil {
		slog.Error("host agent stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, config agent.Config) error {
	agentID, err := agent.LoadOrCreateIdentity(config.StateDirectory)
	if err != nil {
		return err
	}
	runtime, err := agent.NewRuntime(
		config,
		agentID,
		version,
		collector.New(config.Collector),
	)
	if err != nil {
		return err
	}
	return runtime.Run(ctx)
}

type commandLineOptions struct {
	configPath               string
	gatewayAddress           string
	namespace                string
	stateDirectory           string
	insecure                 optionalBool
	credentialFile           string
	tlsCAFile                string
	tlsServerName            string
	hostname                 string
	maxConcurrentScans       string
	connectTimeout           string
	backoffInitial           string
	backoffMaximum           string
	backoffJitter            string
	includePseudoFilesystems optionalBool
	attributes               stringList
}

func resolveConfig(args []string, getenv func(string) string) (agent.Config, error) {
	options := commandLineOptions{}
	flags := flag.NewFlagSet("kubling-host-agent", flag.ContinueOnError)
	flags.StringVar(&options.configPath, "config", "", "path to the host-agent YAML configuration")
	flags.StringVar(&options.gatewayAddress, "gateway", "", "Agent Gateway gRPC target")
	flags.StringVar(&options.namespace, "namespace", "", "fleet namespace requested during enrollment")
	flags.StringVar(&options.stateDirectory, "state-directory", "", "directory containing persistent agent state")
	flags.Var(&options.insecure, "insecure", "use plaintext Agent Gateway transport for explicit development only")
	flags.StringVar(&options.credentialFile, "credential-file", "", "file containing the Agent Gateway bearer credential")
	flags.StringVar(&options.tlsCAFile, "tls-ca", "", "PEM CA bundle for Agent Gateway server authentication")
	flags.StringVar(&options.tlsServerName, "tls-server-name", "", "Agent Gateway TLS server name override")
	flags.StringVar(&options.hostname, "hostname", "", "hostname reported by this agent")
	flags.StringVar(&options.maxConcurrentScans, "max-concurrent-scans", "", "maximum concurrent scans")
	flags.StringVar(&options.connectTimeout, "connect-timeout", "", "Agent Gateway handshake timeout")
	flags.StringVar(&options.backoffInitial, "backoff-initial", "", "initial reconnect delay")
	flags.StringVar(&options.backoffMaximum, "backoff-maximum", "", "maximum reconnect delay")
	flags.StringVar(&options.backoffJitter, "backoff-jitter", "", "reconnect jitter ratio")
	flags.Var(&options.includePseudoFilesystems, "include-pseudo-filesystems", "include pseudo filesystems in later filesystem scans")
	flags.Var(&options.attributes, "attribute", "non-secret agent attribute in key=value form; repeatable")
	if err := flags.Parse(args); err != nil {
		return agent.Config{}, err
	}
	if flags.NArg() != 0 {
		return agent.Config{}, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}

	configPath := firstNonEmpty(options.configPath, getenv("KUBLING_HOST_AGENT_CONFIG"))
	config := agent.DefaultConfig()
	var err error
	if configPath != "" {
		config, err = agent.LoadConfig(configPath)
		if err != nil {
			return agent.Config{}, err
		}
	}
	environment := commandLineOptions{
		gatewayAddress:     getenv("KUBLING_HOST_AGENT_GATEWAY"),
		namespace:          getenv("KUBLING_HOST_AGENT_NAMESPACE"),
		stateDirectory:     getenv("KUBLING_HOST_AGENT_STATE_DIRECTORY"),
		credentialFile:     getenv("KUBLING_HOST_AGENT_CREDENTIAL_FILE"),
		tlsCAFile:          getenv("KUBLING_HOST_AGENT_TLS_CA"),
		tlsServerName:      getenv("KUBLING_HOST_AGENT_TLS_SERVER_NAME"),
		hostname:           getenv("KUBLING_HOST_AGENT_HOSTNAME"),
		maxConcurrentScans: getenv("KUBLING_HOST_AGENT_MAX_CONCURRENT_SCANS"),
		connectTimeout:     getenv("KUBLING_HOST_AGENT_CONNECT_TIMEOUT"),
		backoffInitial:     getenv("KUBLING_HOST_AGENT_BACKOFF_INITIAL"),
		backoffMaximum:     getenv("KUBLING_HOST_AGENT_BACKOFF_MAXIMUM"),
		backoffJitter:      getenv("KUBLING_HOST_AGENT_BACKOFF_JITTER"),
	}
	if value := getenv("KUBLING_HOST_AGENT_INSECURE"); value != "" {
		if err := environment.insecure.Set(value); err != nil {
			return agent.Config{}, fmt.Errorf("KUBLING_HOST_AGENT_INSECURE: %w", err)
		}
	}
	if value := getenv("KUBLING_HOST_AGENT_INCLUDE_PSEUDO_FILESYSTEMS"); value != "" {
		if err := environment.includePseudoFilesystems.Set(value); err != nil {
			return agent.Config{}, fmt.Errorf("KUBLING_HOST_AGENT_INCLUDE_PSEUDO_FILESYSTEMS: %w", err)
		}
	}
	if value := getenv("KUBLING_HOST_AGENT_ATTRIBUTES"); value != "" {
		for _, attribute := range strings.Split(value, ",") {
			if err := environment.attributes.Set(attribute); err != nil {
				return agent.Config{}, fmt.Errorf("KUBLING_HOST_AGENT_ATTRIBUTES: %w", err)
			}
		}
	}
	if err := applyOverrides(&config, environment); err != nil {
		return agent.Config{}, err
	}
	if err := applyOverrides(&config, options); err != nil {
		return agent.Config{}, err
	}
	return agent.NormalizeConfig(config)
}

func applyOverrides(config *agent.Config, options commandLineOptions) error {
	if options.gatewayAddress != "" {
		config.GatewayAddress = options.gatewayAddress
	}
	if options.namespace != "" {
		config.Namespace = options.namespace
	}
	if options.stateDirectory != "" {
		config.StateDirectory = options.stateDirectory
	}
	if options.insecure.set {
		config.Insecure = options.insecure.value
	}
	if options.credentialFile != "" {
		config.CredentialFile = options.credentialFile
	}
	if options.tlsCAFile != "" {
		config.TLS.CAFile = options.tlsCAFile
	}
	if options.tlsServerName != "" {
		config.TLS.ServerName = options.tlsServerName
	}
	if options.hostname != "" {
		config.Hostname = options.hostname
	}
	if err := setUint32(&config.MaxConcurrentScans, options.maxConcurrentScans, "max concurrent scans"); err != nil {
		return err
	}
	if err := setDuration(&config.ConnectTimeout, options.connectTimeout, "connect timeout"); err != nil {
		return err
	}
	if err := setDuration(&config.Backoff.Initial, options.backoffInitial, "backoff initial"); err != nil {
		return err
	}
	if err := setDuration(&config.Backoff.Maximum, options.backoffMaximum, "backoff maximum"); err != nil {
		return err
	}
	if options.backoffJitter != "" {
		value, err := strconv.ParseFloat(options.backoffJitter, 64)
		if err != nil {
			return fmt.Errorf("parse backoff jitter: %w", err)
		}
		config.Backoff.Jitter = value
	}
	if options.includePseudoFilesystems.set {
		config.Collector.IncludePseudoFilesystems = options.includePseudoFilesystems.value
	}
	if len(options.attributes) > 0 {
		if config.Attributes == nil {
			config.Attributes = make(map[string]string)
		}
		for _, raw := range options.attributes {
			key, value, _ := strings.Cut(raw, "=")
			config.Attributes[key] = value
		}
	}
	return nil
}

func setUint32(destination *uint32, raw string, name string) error {
	if raw == "" {
		return nil
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return fmt.Errorf("parse %s: %w", name, err)
	}
	*destination = uint32(value)
	return nil
}

func setDuration(destination *time.Duration, raw string, name string) error {
	if raw == "" {
		return nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("parse %s: %w", name, err)
	}
	*destination = value
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

type optionalBool struct {
	set   bool
	value bool
}

func (o *optionalBool) Set(raw string) error {
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return err
	}
	o.set = true
	o.value = value
	return nil
}

func (o *optionalBool) String() string {
	if !o.set {
		return ""
	}
	return strconv.FormatBool(o.value)
}

func (o *optionalBool) IsBoolFlag() bool { return true }

type stringList []string

func (s *stringList) Set(raw string) error {
	key, _, found := strings.Cut(raw, "=")
	if !found || strings.TrimSpace(key) == "" || strings.TrimSpace(key) != key {
		return errors.New("attribute must use key=value with a non-empty key")
	}
	*s = append(*s, raw)
	return nil
}

func (s *stringList) String() string { return strings.Join(*s, ",") }
