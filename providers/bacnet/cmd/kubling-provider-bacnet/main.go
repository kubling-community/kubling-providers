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

	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/discovery"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	bacnetprovider "github.com/kubling-community/kubling-providers/providers/bacnet/internal/provider"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/transport"
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
		os.Getenv("KUBLING_BACNET_CONFIG"),
		"path to the BACnet provider YAML configuration",
	)
	check := flag.Bool("check", false, "validate configuration and catalog, then exit")
	printMetadata := flag.Bool("print-metadata", false, "print compiled provider metadata as JSON, then exit")
	flag.Parse()

	if err := runWithOptions(*listenAddress, *configPath, runOptions{
		check:         *check,
		printMetadata: *printMetadata,
		output:        os.Stdout,
	}); err != nil {
		slog.Error("BACnet provider stopped", "error", err)
		os.Exit(1)
	}
}

func run(listenAddress string, configPath string) error {
	return runWithOptions(listenAddress, configPath, runOptions{output: io.Discard})
}

func runWithOptions(
	listenAddress string,
	configPath string,
	options runOptions,
) error {
	if options.check && options.printMetadata {
		return errors.New("-check and -print-metadata are mutually exclusive")
	}
	if strings.TrimSpace(configPath) == "" {
		return errors.New("BACnet provider config path is required")
	}
	configuration, err := config.Load(configPath)
	if err != nil {
		return err
	}
	profileCatalog, err := profile.Load(configuration.ProfileFiles())
	if err != nil {
		return err
	}
	compiledCatalog, err := catalog.Compile(profileCatalog)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: configuration.LogLevel(),
	}))
	slog.SetDefault(logger)
	if options.check {
		slog.Info(
			"BACnet provider configuration is valid",
			"profiles", len(profileCatalog.Profiles()),
			"tables", len(compiledCatalog.Metadata().GetTables()),
		)
		return nil
	}
	if options.printMetadata {
		output := options.output
		if output == nil {
			output = io.Discard
		}
		document, err := (protojson.MarshalOptions{
			Indent:        "  ",
			UseProtoNames: true,
		}).Marshal(compiledCatalog.Metadata())
		if err != nil {
			return fmt.Errorf("marshal BACnet provider metadata: %w", err)
		}
		if _, err := fmt.Fprintln(output, string(document)); err != nil {
			return fmt.Errorf("write BACnet provider metadata: %w", err)
		}
		return nil
	}

	if strings.TrimSpace(listenAddress) == "" {
		listenAddress = configuration.ListenAddress()
	}
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return err
	}
	defer listener.Close()
	discoveryManager, err := discovery.New(
		configuration.Attachments(),
		configuration.Scopes(),
		profileCatalog,
		transport.NewClient,
	)
	if err != nil {
		return err
	}
	if err := discoveryManager.Start(context.Background()); err != nil {
		return errors.Join(err, discoveryManager.Close())
	}

	service := providersdk.NewServer(bacnetprovider.New(compiledCatalog, discoveryManager))
	grpcServer := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(grpcServer, service)
	reflection.Register(grpcServer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	slog.Info(
		"BACnet provider listening",
		"address", listener.Addr().String(),
		"profiles", len(profileCatalog.Profiles()),
		"attachments", len(configuration.Attachments()),
		"scopes", len(configuration.Scopes()),
	)
	serveErr := serveGRPC(ctx, grpcServer, listener, configuration.ShutdownGracePeriod())
	closeErr := service.Close(context.Background())
	discoveryErr := discoveryManager.Close()
	return errors.Join(serveErr, closeErr, discoveryErr)
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

	slog.Info("BACnet provider shutdown requested", "grace_period", gracePeriod)
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
		slog.Warn("BACnet provider graceful shutdown timed out", "grace_period", gracePeriod)
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
