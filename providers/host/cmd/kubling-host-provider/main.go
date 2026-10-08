package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	host "github.com/kubling-community/kubling-providers/providers/host"
	"github.com/kubling-community/kubling-providers/providers/host/internal/gateway"
	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/query"
	"github.com/kubling-community/kubling-providers/providers/host/internal/registry"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type config struct {
	providerListen       string
	agentListen          string
	stateDirectory       string
	namespaces           []string
	agentInsecure        bool
	agentTLSCertificate  string
	agentTLSPrivateKey   string
	namespaceTokenFiles  map[string]string
	allowPartialResults  bool
	allowUnboundedFanout bool
	scanTimeout          time.Duration
	maxConcurrentTargets int
	maxRowsPerTarget     int
	maxBytesPerTarget    int
}

type serveResult struct {
	name string
	err  error
}

func main() {
	queryDefaults := query.DefaultConfig()
	agentInsecureDefault, err := environmentBool("KUBLING_HOST_AGENT_INSECURE", false)
	if err != nil {
		slog.Error("host provider configuration is invalid", "error", err)
		os.Exit(2)
	}
	partialResultsDefault, err := environmentBool("KUBLING_HOST_ALLOW_PARTIAL_RESULTS", false)
	if err != nil {
		slog.Error("host provider configuration is invalid", "error", err)
		os.Exit(2)
	}
	unboundedFanoutDefault, err := environmentBool("KUBLING_HOST_ALLOW_UNBOUNDED_FANOUT", false)
	if err != nil {
		slog.Error("host provider configuration is invalid", "error", err)
		os.Exit(2)
	}
	scanTimeoutDefault, err := environmentDuration("KUBLING_HOST_SCAN_TIMEOUT", queryDefaults.ScanTimeout)
	if err != nil {
		slog.Error("host provider configuration is invalid", "error", err)
		os.Exit(2)
	}
	maxConcurrentTargetsDefault, err := environmentInt(
		"KUBLING_HOST_MAX_CONCURRENT_TARGETS",
		queryDefaults.MaxConcurrentTargets,
	)
	if err != nil {
		slog.Error("host provider configuration is invalid", "error", err)
		os.Exit(2)
	}
	maxRowsPerTargetDefault, err := environmentInt(
		"KUBLING_HOST_MAX_ROWS_PER_TARGET",
		queryDefaults.MaxRowsPerTarget,
	)
	if err != nil {
		slog.Error("host provider configuration is invalid", "error", err)
		os.Exit(2)
	}
	maxBytesPerTargetDefault, err := environmentInt(
		"KUBLING_HOST_MAX_BYTES_PER_TARGET",
		queryDefaults.MaxBytesPerTarget,
	)
	if err != nil {
		slog.Error("host provider configuration is invalid", "error", err)
		os.Exit(2)
	}
	providerListen := flag.String(
		"provider-listen",
		firstNonEmpty(os.Getenv("KUBLING_HOST_PROVIDER_LISTEN"), ":50051"),
		"address for the northbound ProviderService",
	)
	stateDirectory := flag.String(
		"state-directory",
		firstNonEmpty(os.Getenv("KUBLING_HOST_STATE_DIRECTORY"), "/var/lib/kubling/host-provider"),
		"directory containing durable Host Provider state",
	)
	agentListen := flag.String(
		"agent-listen",
		firstNonEmpty(os.Getenv("KUBLING_HOST_AGENT_LISTEN"), ":50052"),
		"address for the southbound Agent Gateway",
	)
	namespaces := flag.String(
		"namespaces",
		firstNonEmpty(os.Getenv("KUBLING_HOST_NAMESPACES"), "default"),
		"comma-separated fleet namespace allowlist for insecure development mode",
	)
	agentInsecure := flag.Bool(
		"agent-insecure",
		agentInsecureDefault,
		"use plaintext Agent Gateway transport for explicit development only",
	)
	agentTLSCertificate := flag.String(
		"agent-tls-certificate",
		os.Getenv("KUBLING_HOST_AGENT_TLS_CERTIFICATE"),
		"PEM certificate served by the Agent Gateway",
	)
	agentTLSPrivateKey := flag.String(
		"agent-tls-private-key",
		os.Getenv("KUBLING_HOST_AGENT_TLS_PRIVATE_KEY"),
		"PEM private key served by the Agent Gateway",
	)
	namespaceTokenFiles := make(namespaceTokenFileMap)
	if err := namespaceTokenFiles.setEnvironment(os.Getenv("KUBLING_HOST_NAMESPACE_TOKEN_FILES")); err != nil {
		slog.Error("host provider configuration is invalid", "error", err)
		os.Exit(2)
	}
	flag.Var(
		&namespaceTokenFiles,
		"namespace-token-file",
		"namespace=path mapping to an agent bearer-token file; repeatable",
	)
	allowPartialResults := flag.Bool(
		"allow-partial-results",
		partialResultsDefault,
		"advertise and honor partial distributed results",
	)
	allowUnboundedFanout := flag.Bool(
		"allow-unbounded-fanout",
		unboundedFanoutDefault,
		"allow distributed scans without a namespace, host_id or hostname constraint",
	)
	scanTimeout := flag.Duration(
		"scan-timeout",
		scanTimeoutDefault,
		"maximum duration of one agent scan",
	)
	maxConcurrentTargets := flag.Int(
		"max-concurrent-targets",
		maxConcurrentTargetsDefault,
		"maximum agent targets scanned concurrently by one query",
	)
	maxRowsPerTarget := flag.Int(
		"max-rows-per-target",
		maxRowsPerTargetDefault,
		"maximum rows buffered from one agent target",
	)
	maxBytesPerTarget := flag.Int(
		"max-bytes-per-target",
		maxBytesPerTargetDefault,
		"maximum encoded row bytes buffered from one agent target",
	)
	flag.Parse()

	configuration, err := normalizeConfig(config{
		providerListen:       *providerListen,
		agentListen:          *agentListen,
		stateDirectory:       *stateDirectory,
		namespaces:           strings.Split(*namespaces, ","),
		agentInsecure:        *agentInsecure,
		agentTLSCertificate:  *agentTLSCertificate,
		agentTLSPrivateKey:   *agentTLSPrivateKey,
		namespaceTokenFiles:  namespaceTokenFiles,
		allowPartialResults:  *allowPartialResults,
		allowUnboundedFanout: *allowUnboundedFanout,
		scanTimeout:          *scanTimeout,
		maxConcurrentTargets: *maxConcurrentTargets,
		maxRowsPerTarget:     *maxRowsPerTarget,
		maxBytesPerTarget:    *maxBytesPerTarget,
	})
	if err != nil {
		slog.Error("host provider configuration is invalid", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	if err := run(ctx, configuration); err != nil {
		slog.Error("host provider stopped", "error", err)
		os.Exit(1)
	}
}

func normalizeConfig(configuration config) (config, error) {
	queryDefaults := query.DefaultConfig()
	if configuration.maxConcurrentTargets == 0 {
		configuration.maxConcurrentTargets = queryDefaults.MaxConcurrentTargets
	}
	if configuration.maxRowsPerTarget == 0 {
		configuration.maxRowsPerTarget = queryDefaults.MaxRowsPerTarget
	}
	if configuration.maxBytesPerTarget == 0 {
		configuration.maxBytesPerTarget = queryDefaults.MaxBytesPerTarget
	}
	configuration.providerListen = strings.TrimSpace(configuration.providerListen)
	configuration.agentListen = strings.TrimSpace(configuration.agentListen)
	configuration.stateDirectory = strings.TrimSpace(configuration.stateDirectory)
	if configuration.providerListen == "" || configuration.agentListen == "" {
		return config{}, errors.New("provider and agent listen addresses are required")
	}
	if configuration.providerListen == configuration.agentListen {
		return config{}, errors.New("provider and agent listeners must be different")
	}
	if configuration.stateDirectory == "" {
		return config{}, errors.New("Host Provider state directory is required")
	}
	configuration.agentTLSCertificate = strings.TrimSpace(configuration.agentTLSCertificate)
	configuration.agentTLSPrivateKey = strings.TrimSpace(configuration.agentTLSPrivateKey)
	normalizedTokenFiles := make(map[string]string, len(configuration.namespaceTokenFiles))
	for namespace, path := range configuration.namespaceTokenFiles {
		namespace = strings.TrimSpace(namespace)
		path = strings.TrimSpace(path)
		if namespace == "" || path == "" {
			return config{}, errors.New("namespace token file requires non-empty namespace and path")
		}
		if _, exists := normalizedTokenFiles[namespace]; exists {
			return config{}, fmt.Errorf("namespace token file %q is configured more than once", namespace)
		}
		normalizedTokenFiles[namespace] = path
	}
	configuration.namespaceTokenFiles = normalizedTokenFiles
	if configuration.agentInsecure {
		if configuration.agentTLSCertificate != "" ||
			configuration.agentTLSPrivateKey != "" || len(configuration.namespaceTokenFiles) != 0 {
			return config{}, errors.New("insecure Agent Gateway mode cannot use TLS or namespace credentials")
		}
	} else {
		if configuration.agentTLSCertificate == "" || configuration.agentTLSPrivateKey == "" {
			return config{}, errors.New("Agent Gateway TLS certificate and private key are required")
		}
		if len(configuration.namespaceTokenFiles) == 0 {
			return config{}, errors.New("at least one Agent Gateway namespace credential is required")
		}
	}
	if configuration.scanTimeout <= 0 {
		return config{}, errors.New("scan timeout must be positive")
	}
	if configuration.maxConcurrentTargets <= 0 {
		return config{}, errors.New("maximum concurrent targets must be positive")
	}
	if configuration.maxRowsPerTarget <= 0 {
		return config{}, errors.New("maximum rows per target must be positive")
	}
	if configuration.maxBytesPerTarget <= 0 {
		return config{}, errors.New("maximum bytes per target must be positive")
	}
	if configuration.agentInsecure {
		seen := make(map[string]struct{}, len(configuration.namespaces))
		namespaces := make([]string, 0, len(configuration.namespaces))
		for _, namespace := range configuration.namespaces {
			namespace = strings.TrimSpace(namespace)
			if namespace == "" {
				return config{}, errors.New("fleet namespace allowlist contains an empty value")
			}
			if _, exists := seen[namespace]; exists {
				continue
			}
			seen[namespace] = struct{}{}
			namespaces = append(namespaces, namespace)
		}
		if len(namespaces) == 0 {
			return config{}, errors.New("at least one fleet namespace is required")
		}
		configuration.namespaces = namespaces
	} else {
		configuration.namespaces = make([]string, 0, len(configuration.namespaceTokenFiles))
		for namespace := range configuration.namespaceTokenFiles {
			configuration.namespaces = append(configuration.namespaces, namespace)
		}
		sort.Strings(configuration.namespaces)
	}
	return configuration, nil
}

func run(ctx context.Context, configuration config) (returnErr error) {
	fleet, err := registry.OpenBolt(configuration.stateDirectory, time.Now().UTC())
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, fleet.Close()) }()

	providerListener, err := net.Listen("tcp", configuration.providerListen)
	if err != nil {
		return fmt.Errorf("listen for Kubling provider clients: %w", err)
	}
	defer providerListener.Close()
	agentListener, err := net.Listen("tcp", configuration.agentListen)
	if err != nil {
		return fmt.Errorf("listen for host agents: %w", err)
	}
	defer agentListener.Close()

	sessions := gateway.NewDirectory()
	queryConfig := query.Config{
		AllowPartialResults:  configuration.allowPartialResults,
		AllowUnboundedFanout: configuration.allowUnboundedFanout,
		ScanTimeout:          configuration.scanTimeout,
		MaxConcurrentTargets: configuration.maxConcurrentTargets,
		MaxRowsPerTarget:     configuration.maxRowsPerTarget,
		MaxBytesPerTarget:    configuration.maxBytesPerTarget,
	}
	coordinator, err := query.New(fleet, sessions, queryConfig)
	if err != nil {
		return err
	}
	implementation, err := host.New(
		host.ReadinessFunc(func(ctx context.Context) error { return ctx.Err() }),
		coordinator,
		host.Config{PartialResults: configuration.allowPartialResults},
	)
	if err != nil {
		return err
	}
	providerService := providersdk.NewServer(implementation)
	providerServer := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(providerServer, providerService)
	reflection.Register(providerServer)

	var authorizer gateway.Authorizer
	agentServerOptions := make([]grpc.ServerOption, 0, 1)
	if configuration.agentInsecure {
		authorizer = namespaceAllowlistAuthorizer(makeNamespaceSet(configuration.namespaces))
	} else {
		tokens, err := loadNamespaceTokens(configuration.namespaceTokenFiles)
		if err != nil {
			return err
		}
		authorizer, err = gateway.NewTokenAuthorizer(tokens)
		if err != nil {
			return err
		}
		transportCredentials, err := loadAgentServerCredentials(
			configuration.agentTLSCertificate,
			configuration.agentTLSPrivateKey,
		)
		if err != nil {
			return err
		}
		agentServerOptions = append(agentServerOptions, grpc.Creds(transportCredentials))
	}
	gatewayService, err := gateway.NewServer(
		gateway.DefaultConfig(),
		fleet,
		sessions,
		authorizer,
	)
	if err != nil {
		return err
	}
	agentServer := grpc.NewServer(agentServerOptions...)
	gatewaypb.RegisterAgentGatewayServiceServer(agentServer, gatewayService)

	serveResults := make(chan serveResult, 2)
	go func() {
		serveResults <- serveResult{name: "ProviderService", err: providerServer.Serve(providerListener)}
	}()
	go func() {
		serveResults <- serveResult{name: "Agent Gateway", err: agentServer.Serve(agentListener)}
	}()
	slog.Info(
		"host provider listening",
		"provider_address", providerListener.Addr().String(),
		"agent_address", agentListener.Addr().String(),
		"partial_results", configuration.allowPartialResults,
		"unbounded_fanout", configuration.allowUnboundedFanout,
		"max_concurrent_targets", configuration.maxConcurrentTargets,
		"max_rows_per_target", configuration.maxRowsPerTarget,
		"max_bytes_per_target", configuration.maxBytesPerTarget,
		"agent_transport_secure", !configuration.agentInsecure,
	)

	var stopOnce sync.Once
	stopServers := func() {
		stopOnce.Do(func() {
			agentServer.Stop()
			providerServer.GracefulStop()
		})
	}
	results := make([]serveResult, 0, 2)
	select {
	case <-ctx.Done():
		stopServers()
	case result := <-serveResults:
		results = append(results, result)
		stopServers()
	}
	for len(results) < 2 {
		results = append(results, <-serveResults)
	}
	closeErr := providerService.Close(context.Background())
	return errors.Join(
		serveError(results[0]),
		serveError(results[1]),
		closeErr,
	)
}

func loadAgentServerCredentials(
	certificateFile string,
	privateKeyFile string,
) (credentials.TransportCredentials, error) {
	certificate, err := tls.LoadX509KeyPair(certificateFile, privateKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load Agent Gateway TLS key pair: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
	}), nil
}

func loadNamespaceTokens(files map[string]string) (map[string]string, error) {
	tokens := make(map[string]string, len(files))
	for namespace, path := range files {
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read agent credential for namespace %q: %w", namespace, err)
		}
		token := strings.TrimSpace(string(contents))
		if token == "" {
			return nil, fmt.Errorf("agent credential for namespace %q is empty", namespace)
		}
		tokens[namespace] = token
	}
	return tokens, nil
}

type namespaceTokenFileMap map[string]string

func (m *namespaceTokenFileMap) Set(raw string) error {
	namespace, path, found := strings.Cut(raw, "=")
	if !found || strings.TrimSpace(namespace) == "" || strings.TrimSpace(path) == "" {
		return errors.New("namespace token file must use namespace=path")
	}
	if *m == nil {
		*m = make(namespaceTokenFileMap)
	}
	if _, exists := (*m)[namespace]; exists {
		return fmt.Errorf("namespace token file %q is configured more than once", namespace)
	}
	(*m)[namespace] = path
	return nil
}

func (m *namespaceTokenFileMap) String() string {
	namespaces := make([]string, 0, len(*m))
	for namespace := range *m {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	return strings.Join(namespaces, ",")
}

func (m *namespaceTokenFileMap) setEnvironment(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	for _, entry := range strings.Split(raw, ",") {
		if err := m.Set(entry); err != nil {
			return fmt.Errorf("KUBLING_HOST_NAMESPACE_TOKEN_FILES: %w", err)
		}
	}
	return nil
}

type namespaceAllowlistAuthorizer map[string]struct{}

func (a namespaceAllowlistAuthorizer) Authorize(
	ctx context.Context,
	requested string,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", status.FromContextError(err).Err()
	}
	if _, allowed := a[requested]; !allowed {
		return "", status.Error(codes.PermissionDenied, "fleet namespace is not authorized")
	}
	return requested, nil
}

func makeNamespaceSet(namespaces []string) namespaceAllowlistAuthorizer {
	set := make(namespaceAllowlistAuthorizer, len(namespaces))
	for _, namespace := range namespaces {
		set[namespace] = struct{}{}
	}
	return set
}

func serveError(result serveResult) error {
	if result.err == nil || errors.Is(result.err, grpc.ErrServerStopped) {
		return nil
	}
	return fmt.Errorf("serve %s: %w", result.name, result.err)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func environmentBool(name string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}

func environmentDuration(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}

func environmentInt(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}
