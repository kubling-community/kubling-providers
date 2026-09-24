package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	inmemory "github.com/kubling-community/kubling-providers/providers/inmemory"
	"github.com/kubling-community/kubling-providers/providers/inmemory/internal/querylog"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	providercache "github.com/kubling-community/kubling-providers/sdk-go/provider/cache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	listenAddress := flag.String(
		"listen",
		":50051",
		"address on which the provider gRPC server listens",
	)
	logQueries := flag.Bool(
		"log-queries",
		false,
		"log safe query summaries without values or connection identifiers",
	)
	flag.Parse()

	if err := run(*listenAddress, *logQueries); err != nil {
		slog.Error("in-memory provider stopped", "error", err)
		os.Exit(1)
	}
}

func run(listenAddress string, logQueries bool) error {
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return err
	}
	defer listener.Close()

	logger := slog.Default()
	var options []inmemory.Option
	if logQueries {
		options = append(options, inmemory.WithQueryLogger(logger))
	}
	implementation := inmemory.New(options...)
	cachedImplementation, _ := providercache.Wrap(
		implementation,
		providercache.Config{},
	)
	service := providersdk.NewServer(cachedImplementation)
	grpcServer := grpc.NewServer()

	registeredService := providerv1.ProviderServiceServer(service)
	if logQueries {
		registeredService = &queryLoggingServer{
			Server: service,
			logger: logger,
		}
	}
	providerv1.RegisterProviderServiceServer(grpcServer, registeredService)
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
		"in-memory provider listening",
		"address",
		listener.Addr().String(),
	)

	serveErr := grpcServer.Serve(listener)
	closeErr := service.Close(context.Background())

	if errors.Is(serveErr, grpc.ErrServerStopped) {
		serveErr = nil
	}

	return errors.Join(serveErr, closeErr)
}

type queryLoggingServer struct {
	*providersdk.Server
	logger *slog.Logger
}

func (s *queryLoggingServer) Query(
	request *providerv1.QueryRequest,
	stream providerv1.ProviderService_QueryServer,
) error {
	querylog.Received(stream.Context(), s.logger, request)
	return s.Server.Query(request, stream)
}
