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

	openapiprovider "github.com/kubling-community/kubling-providers/providers/openapi"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	listenAddress := flag.String("listen", ":50051", "address on which the provider gRPC server listens")
	configPath := flag.String("config", os.Getenv("KUBLING_OPENAPI_CONFIG"), "path to the OpenAPI provider YAML configuration")
	check := flag.Bool("check", false, "validate configuration and exit without starting the server")
	printMetadata := flag.Bool("print-metadata", false, "print discovered provider metadata as JSON and exit")
	generateConfigTemplate := flag.Bool("generate-config-template", false, "print a configuration template containing detected entity candidates and exit")
	flag.Parse()

	if err := run(*listenAddress, *configPath, *check, *printMetadata, *generateConfigTemplate); err != nil {
		slog.Error("OpenAPI provider stopped", "error", err)
		os.Exit(1)
	}
}

func run(listenAddress, configPath string, check, printMetadata, generateConfigTemplate bool) error {
	if configPath == "" {
		return errors.New("OpenAPI provider config path is required")
	}
	selectedModes := 0
	for _, selected := range []bool{check, printMetadata, generateConfigTemplate} {
		if selected {
			selectedModes++
		}
	}
	if selectedModes > 1 {
		return errors.New("-check, -print-metadata and -generate-config-template are mutually exclusive")
	}
	if generateConfigTemplate {
		generated, err := openapiprovider.GenerateConfigTemplate(configPath)
		if err != nil {
			return fmt.Errorf("generate OpenAPI provider configuration template: %w", err)
		}
		_, err = os.Stdout.Write(generated)
		return err
	}
	config, err := openapiprovider.LoadConfig(configPath)
	if err != nil {
		return err
	}
	implementation, err := openapiprovider.New(config)
	if err != nil {
		return fmt.Errorf("create OpenAPI provider: %w", err)
	}
	if check {
		slog.Info("OpenAPI provider configuration is valid", "config", configPath)
		return nil
	}
	if printMetadata {
		metadata, err := implementation.Metadata(context.Background())
		if err != nil {
			return fmt.Errorf("read OpenAPI provider metadata: %w", err)
		}
		serialized, err := protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("encode OpenAPI provider metadata: %w", err)
		}
		_, err = fmt.Fprintln(os.Stdout, string(serialized))
		return err
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	slog.Info("OpenAPI provider listening", "address", listener.Addr().String(), "config", configPath)
	serveErr := grpcServer.Serve(listener)
	closeErr := service.Close(context.Background())
	if errors.Is(serveErr, grpc.ErrServerStopped) {
		serveErr = nil
	}
	return errors.Join(serveErr, closeErr)
}
