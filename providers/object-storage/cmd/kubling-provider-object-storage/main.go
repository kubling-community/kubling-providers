package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend/azureblobstore"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend/fsstore"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend/gcsstore"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend/s3store"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
	objectprovider "github.com/kubling-community/kubling-providers/providers/object-storage/internal/provider"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
)

type runOptions struct {
	check         bool
	printMetadata bool
	output        io.Writer
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	listenAddress := flag.String("listen", "", "override the provider gRPC listen address")
	configPath := flag.String(
		"config",
		os.Getenv("KUBLING_OBJECT_STORAGE_CONFIG"),
		"path to the object-storage provider YAML configuration",
	)
	check := flag.Bool("check", false, "validate configuration and catalog, then exit")
	printMetadata := flag.Bool("print-metadata", false, "print compiled provider metadata as JSON, then exit")
	flag.Parse()

	if err := run(*listenAddress, *configPath, runOptions{
		check:         *check,
		printMetadata: *printMetadata,
		output:        os.Stdout,
	}); err != nil {
		slog.Error("object-storage provider stopped", "error", err)
		os.Exit(1)
	}
}

func run(listenAddress string, configPath string, options runOptions) error {
	if options.check && options.printMetadata {
		return errors.New("-check and -print-metadata are mutually exclusive")
	}
	if strings.TrimSpace(configPath) == "" {
		return errors.New("object-storage provider config path is required")
	}
	configuration, err := config.Load(configPath)
	if err != nil {
		return err
	}
	compiled, err := catalog.Compile(configuration)
	if err != nil {
		return err
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: configuration.LogLevel,
	})))
	if options.check {
		slog.Info(
			"object-storage provider configuration is valid",
			"backends", len(configuration.Backends),
			"buckets", len(configuration.Buckets),
		)
		return nil
	}
	if options.printMetadata {
		document, err := (protojson.MarshalOptions{
			Indent:        "  ",
			UseProtoNames: true,
		}).Marshal(compiled.Metadata())
		if err != nil {
			return fmt.Errorf("marshal object-storage provider metadata: %w", err)
		}
		output := options.output
		if output == nil {
			output = io.Discard
		}
		if _, err := fmt.Fprintln(output, string(document)); err != nil {
			return fmt.Errorf("write object-storage provider metadata: %w", err)
		}
		return nil
	}

	stores, err := buildStores(context.Background(), configuration)
	if err != nil {
		return err
	}
	implementation, err := objectprovider.New(configuration, compiled, stores)
	if err != nil {
		return errors.Join(err, closeStores(stores))
	}
	if strings.TrimSpace(listenAddress) == "" {
		listenAddress = configuration.ListenAddress
	}
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return errors.Join(err, implementation.Close())
	}
	defer listener.Close()

	service := providersdk.NewServer(implementation)
	grpcServer := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(grpcServer, service)
	reflection.Register(grpcServer)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info(
		"object-storage provider listening",
		"address", listener.Addr().String(),
		"backends", len(configuration.Backends),
		"buckets", len(configuration.Buckets),
	)
	serveErr := serveGRPC(ctx, grpcServer, listener, configuration.ShutdownGracePeriod)
	connectionErr := service.Close(context.Background())
	backendErr := implementation.Close()
	return errors.Join(serveErr, connectionErr, backendErr)
}

func buildStores(ctx context.Context, configuration config.Config) (map[string]backend.Store, error) {
	stores := make(map[string]backend.Store, len(configuration.Backends))
	for name, backendConfig := range configuration.Backends {
		var (
			store backend.Store
			err   error
		)
		switch backendConfig.Type {
		case "s3":
			store, err = s3store.New(ctx, backendConfig)
		case "azure-blob":
			store, err = azureblobstore.New(backendConfig)
		case "gcs":
			store, err = gcsstore.New(ctx, backendConfig)
		case "filesystem":
			store, err = fsstore.New(backendConfig)
		default:
			err = fmt.Errorf("backend %q has unsupported type %q", name, backendConfig.Type)
		}
		if err != nil {
			return nil, errors.Join(
				fmt.Errorf("initialize backend %q: %w", name, err),
				closeStores(stores),
			)
		}
		stores[name] = store
	}
	return stores, nil
}

func closeStores(stores map[string]backend.Store) error {
	var failures []error
	for name, store := range stores {
		if err := store.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close backend %q: %w", name, err))
		}
	}
	return errors.Join(failures...)
}

func serveGRPC(
	ctx context.Context,
	server *grpc.Server,
	listener net.Listener,
	gracePeriod time.Duration,
) error {
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	select {
	case serveErr := <-serveDone:
		server.Stop()
		return normalizeServeError(serveErr)
	case <-ctx.Done():
	}

	slog.Info("object-storage provider shutdown requested", "grace_period", gracePeriod)
	gracefulDone := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(gracefulDone)
	}()
	timer := time.NewTimer(gracePeriod)
	select {
	case <-gracefulDone:
		timer.Stop()
	case <-timer.C:
		slog.Warn("object-storage provider graceful shutdown timed out", "grace_period", gracePeriod)
		server.Stop()
		<-gracefulDone
	}
	return normalizeServeError(<-serveDone)
}

func normalizeServeError(err error) error {
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}
