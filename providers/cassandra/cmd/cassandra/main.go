package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	cassandra "github.com/kubling-community/kubling-providers/providers/cassandra"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	listenAddress := flag.String(
		"listen",
		":50051",
		"address on which the provider gRPC server listens",
	)
	configPath := flag.String(
		"config",
		os.Getenv("KUBLING_CASSANDRA_CONFIG"),
		"path to the Cassandra provider YAML configuration",
	)
	flag.Parse()

	if err := run(*listenAddress, *configPath); err != nil {
		slog.Error("Cassandra provider stopped", "error", err)
		os.Exit(1)
	}
}

func run(listenAddress string, configPath string) error {
	if configPath == "" {
		return errors.New("Cassandra provider config path is required")
	}

	config, err := cassandra.LoadConfig(configPath)
	if err != nil {
		return err
	}
	implementation, err := cassandra.New(config)
	if err != nil {
		return fmt.Errorf("create Cassandra provider: %w", err)
	}

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return err
	}
	defer listener.Close()

	service := providersdk.NewServer(implementation)
	grpcServer := grpc.NewServer()
	providerv1.RegisterProviderServiceServer(grpcServer, service)
	reflection.Register(grpcServer)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	slog.Info(
		"Cassandra provider listening",
		"address",
		listener.Addr().String(),
		"config",
		configPath,
	)

	serveErr := grpcServer.Serve(listener)
	closeErr := service.Close(context.Background())
	if errors.Is(serveErr, grpc.ErrServerStopped) {
		serveErr = nil
	}

	return errors.Join(serveErr, closeErr)
}
