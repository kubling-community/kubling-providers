package kubernetes

import (
	"context"
	"net/http"

	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

type kubernetesClient interface {
	Discovery() discovery.DiscoveryInterface
	Dynamic() dynamic.Interface
	DefaultNamespace() string
	Health(context.Context) error
	Close() error
}

type clientFactory func(context.Context, Config) (kubernetesClient, error)

type clusterClient struct {
	discovery        *discovery.DiscoveryClient
	dynamic          *dynamic.DynamicClient
	httpClient       *http.Client
	defaultNamespace string
}

func newClusterClient(ctx context.Context, config Config) (kubernetesClient, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	restConfig, defaultNamespace, err := loadClientConfiguration(config)
	if err != nil {
		return nil, err
	}
	httpClient, err := rest.HTTPClientFor(restConfig)
	if err != nil {
		return nil, err
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfigAndClient(restConfig, httpClient)
	if err != nil {
		httpClient.CloseIdleConnections()
		return nil, err
	}
	dynamicClient, err := dynamic.NewForConfigAndClient(restConfig, httpClient)
	if err != nil {
		httpClient.CloseIdleConnections()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		httpClient.CloseIdleConnections()
		return nil, err
	}
	return &clusterClient{
		discovery:        discoveryClient,
		dynamic:          dynamicClient,
		httpClient:       httpClient,
		defaultNamespace: defaultNamespace,
	}, nil
}

func (c *clusterClient) Discovery() discovery.DiscoveryInterface {
	return c.discovery
}

func (c *clusterClient) Dynamic() dynamic.Interface {
	return c.dynamic
}

func (c *clusterClient) DefaultNamespace() string {
	return c.defaultNamespace
}

func (c *clusterClient) Health(ctx context.Context) error {
	return c.discovery.RESTClient().Get().AbsPath("/readyz").Do(ctx).Error()
}

func (c *clusterClient) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

var _ kubernetesClient = (*clusterClient)(nil)
