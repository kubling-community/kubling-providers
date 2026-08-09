package kubernetes

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const defaultRequestTimeout = 30 * time.Second

const inClusterNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// BlankNamespaceStrategy controls how namespaced operations interpret an
// empty namespace.
type BlankNamespaceStrategy string

const (
	BlankNamespaceDefault BlankNamespaceStrategy = "DEFAULT"
	BlankNamespaceAll     BlankNamespaceStrategy = "ALL"
	BlankNamespaceFail    BlankNamespaceStrategy = "FAIL"
)

// Config configures the single Kubernetes cluster owned by this provider.
type Config struct {
	Kubeconfig             string
	Context                string
	InCluster              bool
	RequestTimeout         time.Duration
	QPS                    float32
	Burst                  int
	BlankNamespaceStrategy BlankNamespaceStrategy
	Schema                 SchemaConfig
}

// SchemaConfig controls which Kubernetes resources are exposed and how deeply
// structured resource fields are expanded into relational columns.
type SchemaConfig struct {
	FieldExpansionDepth int
	Include             []string
	Exclude             []string
	Resources           map[string]ResourceSchemaConfig
}

// ResourceSchemaConfig overrides schema behavior for one exact Kubernetes
// groupVersion/resource key, for example "apps/v1/deployments".
type ResourceSchemaConfig struct {
	FieldExpansionDepth *int
}

type fileConfig struct {
	Kubeconfig             string           `yaml:"kubeconfig"`
	Context                string           `yaml:"context"`
	InCluster              bool             `yaml:"inCluster"`
	RequestTimeout         string           `yaml:"requestTimeout"`
	QPS                    float32          `yaml:"qps"`
	Burst                  int              `yaml:"burst"`
	BlankNamespaceStrategy string           `yaml:"blankNamespaceStrategy"`
	Schema                 fileSchemaConfig `yaml:"schema"`
}

type fileSchemaConfig struct {
	FieldExpansionDepth int                                 `yaml:"fieldExpansionDepth"`
	Include             []string                            `yaml:"include"`
	Exclude             []string                            `yaml:"exclude"`
	Resources           map[string]fileResourceSchemaConfig `yaml:"resources"`
}

type fileResourceSchemaConfig struct {
	FieldExpansionDepth *int `yaml:"fieldExpansionDepth"`
}

// LoadConfig reads a strict Kubernetes provider YAML configuration.
func LoadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open Kubernetes provider config: %w", err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)

	var serialized fileConfig
	if err := decoder.Decode(&serialized); err != nil {
		return Config{}, fmt.Errorf("decode Kubernetes provider config: %w", err)
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("decode Kubernetes provider config: multiple YAML documents are not supported")
		}
		return Config{}, fmt.Errorf("decode Kubernetes provider config: %w", err)
	}

	requestTimeout, err := parseOptionalDuration("requestTimeout", serialized.RequestTimeout)
	if err != nil {
		return Config{}, err
	}

	return normalizeConfig(Config{
		Kubeconfig:             serialized.Kubeconfig,
		Context:                serialized.Context,
		InCluster:              serialized.InCluster,
		RequestTimeout:         requestTimeout,
		QPS:                    serialized.QPS,
		Burst:                  serialized.Burst,
		BlankNamespaceStrategy: BlankNamespaceStrategy(serialized.BlankNamespaceStrategy),
		Schema:                 serialized.Schema.toConfig(),
	})
}

func (c fileSchemaConfig) toConfig() SchemaConfig {
	resources := make(map[string]ResourceSchemaConfig, len(c.Resources))
	for resource, config := range c.Resources {
		resources[resource] = ResourceSchemaConfig{
			FieldExpansionDepth: copyIntPointer(config.FieldExpansionDepth),
		}
	}

	return SchemaConfig{
		FieldExpansionDepth: c.FieldExpansionDepth,
		Include:             append([]string(nil), c.Include...),
		Exclude:             append([]string(nil), c.Exclude...),
		Resources:           resources,
	}
}

func parseOptionalDuration(name string, value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return duration, nil
}

func normalizeConfig(config Config) (Config, error) {
	normalized := config
	normalized.Kubeconfig = strings.TrimSpace(config.Kubeconfig)
	normalized.Context = strings.TrimSpace(config.Context)

	if normalized.InCluster && (normalized.Kubeconfig != "" || normalized.Context != "") {
		return Config{}, errors.New("inCluster cannot be combined with kubeconfig or context")
	}

	if normalized.RequestTimeout == 0 {
		normalized.RequestTimeout = defaultRequestTimeout
	}
	if normalized.RequestTimeout < 0 {
		return Config{}, errors.New("request timeout must not be negative")
	}
	if normalized.QPS < 0 {
		return Config{}, errors.New("qps must not be negative")
	}
	if normalized.Burst < 0 {
		return Config{}, errors.New("burst must not be negative")
	}

	strategy := BlankNamespaceStrategy(
		strings.ToUpper(
			strings.TrimSpace(string(config.BlankNamespaceStrategy)),
		),
	)
	if strategy == "" {
		strategy = BlankNamespaceDefault
	}

	switch strategy {
	case BlankNamespaceDefault, BlankNamespaceAll, BlankNamespaceFail:
		normalized.BlankNamespaceStrategy = strategy
	default:
		return Config{}, fmt.Errorf(
			"invalid blank namespace strategy %q",
			config.BlankNamespaceStrategy,
		)
	}

	schemaConfig, err := normalizeSchemaConfig(config.Schema)
	if err != nil {
		return Config{}, fmt.Errorf("schema: %w", err)
	}
	normalized.Schema = schemaConfig

	return normalized, nil
}

func normalizeSchemaConfig(config SchemaConfig) (SchemaConfig, error) {
	if config.FieldExpansionDepth < 0 {
		return SchemaConfig{}, errors.New(
			"fieldExpansionDepth must not be negative",
		)
	}

	include, err := normalizeResourcePatterns("include", config.Include)
	if err != nil {
		return SchemaConfig{}, err
	}

	exclude, err := normalizeResourcePatterns("exclude", config.Exclude)
	if err != nil {
		return SchemaConfig{}, err
	}

	resources := make(
		map[string]ResourceSchemaConfig,
		len(config.Resources),
	)

	for rawKey, candidate := range config.Resources {
		key, err := normalizeResourceKey(rawKey)
		if err != nil {
			return SchemaConfig{}, fmt.Errorf(
				"resource %q: %w",
				rawKey,
				err,
			)
		}

		if _, exists := resources[key]; exists {
			return SchemaConfig{}, fmt.Errorf(
				"duplicate resource %q",
				key,
			)
		}

		if candidate.FieldExpansionDepth != nil &&
			*candidate.FieldExpansionDepth < 0 {
			return SchemaConfig{}, fmt.Errorf(
				"resource %q fieldExpansionDepth must not be negative",
				key,
			)
		}

		resources[key] = ResourceSchemaConfig{
			FieldExpansionDepth: copyIntPointer(
				candidate.FieldExpansionDepth,
			),
		}
	}

	return SchemaConfig{
		FieldExpansionDepth: config.FieldExpansionDepth,
		Include:             include,
		Exclude:             exclude,
		Resources:           resources,
	}, nil
}

func normalizeResourcePatterns(
	name string,
	patterns []string,
) ([]string, error) {
	normalized := make([]string, 0, len(patterns))
	seen := make(map[string]struct{}, len(patterns))

	for index, rawPattern := range patterns {
		pattern := strings.ToLower(strings.TrimSpace(rawPattern))

		if pattern == "" {
			return nil, fmt.Errorf(
				"%s pattern %d must not be blank",
				name,
				index,
			)
		}

		if strings.ContainsAny(pattern, " \t\r\n") {
			return nil, fmt.Errorf(
				"%s pattern %q must not contain whitespace",
				name,
				rawPattern,
			)
		}

		if !strings.Contains(pattern, "/") {
			return nil, fmt.Errorf(
				"%s pattern %q must use groupVersion/resource form",
				name,
				rawPattern,
			)
		}

		if _, err := path.Match(pattern, "v1/pods"); err != nil {
			return nil, fmt.Errorf(
				"invalid %s pattern %q: %w",
				name,
				rawPattern,
				err,
			)
		}

		if _, exists := seen[pattern]; exists {
			continue
		}

		seen[pattern] = struct{}{}
		normalized = append(normalized, pattern)
	}

	return normalized, nil
}

func normalizeResourceKey(value string) (string, error) {
	key := strings.ToLower(strings.TrimSpace(value))
	if key == "" {
		return "", errors.New("key must not be blank")
	}

	if strings.ContainsAny(key, " \t\r\n") {
		return "", errors.New("key must not contain whitespace")
	}

	separator := strings.LastIndex(key, "/")
	if separator <= 0 || separator == len(key)-1 {
		return "", errors.New(
			"key must use groupVersion/resource form",
		)
	}

	groupVersion := key[:separator]
	resource := key[separator+1:]

	parsed, err := k8sschema.ParseGroupVersion(groupVersion)
	if err != nil || parsed.Version == "" {
		return "", fmt.Errorf(
			"invalid Kubernetes groupVersion %q",
			groupVersion,
		)
	}

	if resource == "" || strings.Contains(resource, "/") {
		return "", fmt.Errorf(
			"invalid Kubernetes resource %q",
			resource,
		)
	}

	return groupVersion + "/" + resource, nil
}

func (c SchemaConfig) includesResource(
	groupVersion string,
	resource string,
) bool {
	key := resourceKey(groupVersion, resource)

	included := len(c.Include) == 0

	for _, pattern := range c.Include {
		matched, _ := path.Match(pattern, key)
		if matched {
			included = true
			break
		}
	}

	if !included {
		return false
	}

	for _, pattern := range c.Exclude {
		matched, _ := path.Match(pattern, key)
		if matched {
			return false
		}
	}

	return true
}

func (c SchemaConfig) expansionDepth(
	groupVersion string,
	resource string,
) int {
	depth := c.FieldExpansionDepth

	if override, exists := c.Resources[resourceKey(groupVersion, resource)]; exists && override.FieldExpansionDepth != nil {
		depth = *override.FieldExpansionDepth
	}

	return depth
}

func resourceKey(
	groupVersion string,
	resource string,
) string {
	return strings.ToLower(
		strings.TrimSpace(groupVersion) +
			"/" +
			strings.TrimSpace(resource),
	)
}

func copyIntPointer(value *int) *int {
	if value == nil {
		return nil
	}

	copied := *value
	return &copied
}

func buildRESTConfig(config Config) (*rest.Config, error) {
	restConfig, _, err := loadClientConfiguration(config)
	return restConfig, err
}

func loadClientConfiguration(
	config Config,
) (*rest.Config, string, error) {
	var (
		restConfig       *rest.Config
		defaultNamespace string
		err              error
	)

	if config.InCluster {
		restConfig, err = rest.InClusterConfig()
		defaultNamespace = inClusterNamespace()
	} else {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		loadingRules.ExplicitPath = config.Kubeconfig

		overrides := &clientcmd.ConfigOverrides{
			CurrentContext: config.Context,
		}

		deferred := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadingRules,
			overrides,
		)

		restConfig, err = deferred.ClientConfig()
		if err == nil {
			defaultNamespace, _, err = deferred.Namespace()
		}
	}

	if err != nil {
		return nil, "", fmt.Errorf(
			"load Kubernetes client configuration: %w",
			err,
		)
	}

	if strings.TrimSpace(defaultNamespace) == "" {
		defaultNamespace = metav1.NamespaceDefault
	}

	restConfig.Timeout = config.RequestTimeout

	if config.QPS > 0 {
		restConfig.QPS = config.QPS
	}

	if config.Burst > 0 {
		restConfig.Burst = config.Burst
	}

	return restConfig, defaultNamespace, nil
}

func inClusterNamespace() string {
	if namespace := strings.TrimSpace(
		os.Getenv("POD_NAMESPACE"),
	); namespace != "" {
		return namespace
	}

	contents, err := os.ReadFile(inClusterNamespacePath)
	if err == nil {
		if namespace := strings.TrimSpace(string(contents)); namespace != "" {
			return namespace
		}
	}

	return metav1.NamespaceDefault
}
