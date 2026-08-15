package cassandra

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"gopkg.in/yaml.v3"
)

const (
	defaultPort           = 9042
	defaultConnectTimeout = 11 * time.Second
	defaultQueryTimeout   = 11 * time.Second
	defaultConsistency    = "LOCAL_QUORUM"
)

// Config defines the logical namespaces and Cassandra connection settings
// owned by this provider process.
type Config struct {
	DataSources     map[string]DataSourceConfig
	NamespaceColumn NamespaceColumnConfig
}

// NamespaceColumnConfig controls whether source namespaces are exposed as a
// relational val_constant column.
type NamespaceColumnConfig struct {
	Enabled bool
	Name    string
}

// DataSourceConfig configures one Cassandra keyspace.
type DataSourceConfig struct {
	Hosts              []string
	Port               int
	LocalDataCenter    string
	Keyspace           string
	Username           string
	Password           string
	Consistency        string
	ConnectTimeout     time.Duration
	QueryTimeout       time.Duration
	ConnectionsPerHost int
	TLS                *TLSConfig
}

// TLSConfig configures TLS for one Cassandra data source.
type TLSConfig struct {
	CAFile             string
	CertificateFile    string
	PrivateKeyFile     string
	ServerName         string
	InsecureSkipVerify bool
}

type fileConfig struct {
	NamespaceColumn fileNamespaceColumnConfig       `yaml:"namespaceColumn"`
	Namespaces      map[string]fileDataSourceConfig `yaml:"namespaces"`
}

type fileNamespaceColumnConfig struct {
	Enabled bool   `yaml:"enabled"`
	Name    string `yaml:"name"`
}

type fileDataSourceConfig struct {
	Hosts              []string       `yaml:"hosts"`
	Port               int            `yaml:"port"`
	LocalDataCenter    string         `yaml:"localDataCenter"`
	Keyspace           string         `yaml:"keyspace"`
	Username           string         `yaml:"username"`
	Password           string         `yaml:"password"`
	Consistency        string         `yaml:"consistency"`
	ConnectTimeout     string         `yaml:"connectTimeout"`
	QueryTimeout       string         `yaml:"queryTimeout"`
	ConnectionsPerHost int            `yaml:"connectionsPerHost"`
	TLS                *fileTLSConfig `yaml:"tls"`
}

type fileTLSConfig struct {
	CAFile             string `yaml:"caFile"`
	CertificateFile    string `yaml:"certificateFile"`
	PrivateKeyFile     string `yaml:"privateKeyFile"`
	ServerName         string `yaml:"serverName"`
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
}

// LoadConfig reads a strict YAML configuration file.
func LoadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open Cassandra provider config: %w", err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)

	var serialized fileConfig
	if err := decoder.Decode(&serialized); err != nil {
		return Config{}, fmt.Errorf("decode Cassandra provider config: %w", err)
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("decode Cassandra provider config: multiple YAML documents are not supported")
		}
		return Config{}, fmt.Errorf("decode Cassandra provider config: %w", err)
	}

	config := Config{
		DataSources: make(map[string]DataSourceConfig, len(serialized.Namespaces)),
		NamespaceColumn: NamespaceColumnConfig{
			Enabled: serialized.NamespaceColumn.Enabled,
			Name:    serialized.NamespaceColumn.Name,
		},
	}
	for namespace, serializedDataSource := range serialized.Namespaces {
		dataSource, err := serializedDataSource.toConfig()
		if err != nil {
			return Config{}, fmt.Errorf("namespace %q: %w", namespace, err)
		}
		config.DataSources[namespace] = dataSource
	}

	return normalizeConfig(config)
}

func (c fileDataSourceConfig) toConfig() (DataSourceConfig, error) {
	connectTimeout, err := parseOptionalDuration("connectTimeout", c.ConnectTimeout)
	if err != nil {
		return DataSourceConfig{}, err
	}
	queryTimeout, err := parseOptionalDuration("queryTimeout", c.QueryTimeout)
	if err != nil {
		return DataSourceConfig{}, err
	}

	dataSource := DataSourceConfig{
		Hosts:              c.Hosts,
		Port:               c.Port,
		LocalDataCenter:    c.LocalDataCenter,
		Keyspace:           c.Keyspace,
		Username:           c.Username,
		Password:           c.Password,
		Consistency:        c.Consistency,
		ConnectTimeout:     connectTimeout,
		QueryTimeout:       queryTimeout,
		ConnectionsPerHost: c.ConnectionsPerHost,
	}
	if c.TLS != nil {
		dataSource.TLS = &TLSConfig{
			CAFile:             c.TLS.CAFile,
			CertificateFile:    c.TLS.CertificateFile,
			PrivateKeyFile:     c.TLS.PrivateKeyFile,
			ServerName:         c.TLS.ServerName,
			InsecureSkipVerify: c.TLS.InsecureSkipVerify,
		}
	}

	return dataSource, nil
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
	if len(config.DataSources) == 0 {
		return Config{}, errors.New("at least one Cassandra data source is required")
	}

	normalized := Config{
		DataSources: make(map[string]DataSourceConfig, len(config.DataSources)),
	}
	namespaceColumn, err := normalizeNamespaceColumnConfig(config.NamespaceColumn)
	if err != nil {
		return Config{}, fmt.Errorf("namespaceColumn: %w", err)
	}
	normalized.NamespaceColumn = namespaceColumn

	for dataSourceRef, dataSource := range config.DataSources {
		if dataSourceRef == "" || strings.TrimSpace(dataSourceRef) != dataSourceRef {
			return Config{}, fmt.Errorf("invalid namespace %q", dataSourceRef)
		}

		normalizedDataSource, err := normalizeDataSource(dataSource)
		if err != nil {
			return Config{}, fmt.Errorf("namespace %q: %w", dataSourceRef, err)
		}
		normalized.DataSources[dataSourceRef] = normalizedDataSource
	}

	return normalized, nil
}

func normalizeNamespaceColumnConfig(config NamespaceColumnConfig) (NamespaceColumnConfig, error) {
	normalized := config
	normalized.Name = strings.TrimSpace(config.Name)
	if !normalized.Enabled {
		if normalized.Name != "" {
			return NamespaceColumnConfig{}, errors.New("enabled must be true when a namespace column name is configured")
		}
		return NamespaceColumnConfig{}, nil
	}
	if normalized.Name == "" {
		normalized.Name = providersdk.DefaultNamespaceColumnName
	}
	if strings.Contains(normalized.Name, "+") {
		return NamespaceColumnConfig{}, errors.New("name must not contain reserved separator +")
	}
	return normalized, nil
}

func normalizeDataSource(config DataSourceConfig) (DataSourceConfig, error) {
	if len(config.Hosts) == 0 {
		return DataSourceConfig{}, errors.New("at least one host is required")
	}

	normalized := config
	normalized.Hosts = make([]string, len(config.Hosts))
	for index, host := range config.Hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			return DataSourceConfig{}, fmt.Errorf("host %d is empty", index)
		}
		normalized.Hosts[index] = host
	}

	normalized.Keyspace = strings.TrimSpace(config.Keyspace)
	if normalized.Keyspace == "" {
		return DataSourceConfig{}, errors.New("keyspace is required")
	}
	normalized.LocalDataCenter = strings.TrimSpace(config.LocalDataCenter)
	normalized.Username = strings.TrimSpace(config.Username)

	if (normalized.Username == "") != (config.Password == "") {
		return DataSourceConfig{}, errors.New("username and password must be configured together")
	}

	if normalized.Port == 0 {
		normalized.Port = defaultPort
	}
	if normalized.Port < 1 || normalized.Port > 65535 {
		return DataSourceConfig{}, fmt.Errorf("port %d is outside the valid range", normalized.Port)
	}

	if normalized.ConnectTimeout == 0 {
		normalized.ConnectTimeout = defaultConnectTimeout
	}
	if normalized.ConnectTimeout < 0 {
		return DataSourceConfig{}, errors.New("connect timeout must not be negative")
	}
	if normalized.QueryTimeout == 0 {
		normalized.QueryTimeout = defaultQueryTimeout
	}
	if normalized.QueryTimeout < 0 {
		return DataSourceConfig{}, errors.New("query timeout must not be negative")
	}
	if normalized.ConnectionsPerHost < 0 {
		return DataSourceConfig{}, errors.New("connections per host must not be negative")
	}

	normalized.Consistency = strings.ToUpper(strings.TrimSpace(config.Consistency))
	if normalized.Consistency == "" {
		normalized.Consistency = defaultConsistency
	}
	if _, err := gocql.ParseConsistencyWrapper(normalized.Consistency); err != nil {
		return DataSourceConfig{}, fmt.Errorf("invalid consistency %q: %w", normalized.Consistency, err)
	}

	if config.TLS != nil {
		tlsConfig := *config.TLS
		tlsConfig.CAFile = strings.TrimSpace(tlsConfig.CAFile)
		tlsConfig.CertificateFile = strings.TrimSpace(tlsConfig.CertificateFile)
		tlsConfig.PrivateKeyFile = strings.TrimSpace(tlsConfig.PrivateKeyFile)
		tlsConfig.ServerName = strings.TrimSpace(tlsConfig.ServerName)
		if (tlsConfig.CertificateFile == "") != (tlsConfig.PrivateKeyFile == "") {
			return DataSourceConfig{}, errors.New("TLS certificate and private key files must be configured together")
		}
		normalized.TLS = &tlsConfig
	}

	return normalized, nil
}

func buildClusterConfig(config DataSourceConfig) (*gocql.ClusterConfig, error) {
	cluster := gocql.NewCluster(config.Hosts...)
	cluster.Port = config.Port
	cluster.Keyspace = config.Keyspace
	cluster.ConnectTimeout = config.ConnectTimeout
	cluster.Timeout = config.QueryTimeout
	if config.ConnectionsPerHost > 0 {
		cluster.NumConns = config.ConnectionsPerHost
	}

	consistency, err := gocql.ParseConsistencyWrapper(config.Consistency)
	if err != nil {
		return nil, fmt.Errorf("parse consistency: %w", err)
	}
	cluster.Consistency = consistency

	fallbackPolicy := gocql.RoundRobinHostPolicy()
	if config.LocalDataCenter != "" {
		fallbackPolicy = gocql.DCAwareRoundRobinPolicy(config.LocalDataCenter)
	}
	cluster.PoolConfig.HostSelectionPolicy = gocql.ShuffledTokenAwareHostPolicy(fallbackPolicy)

	if config.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		}
	}

	if config.TLS != nil {
		sslOptions, err := buildTLSOptions(*config.TLS)
		if err != nil {
			return nil, err
		}
		cluster.SslOpts = sslOptions
	}

	return cluster, nil
}

func buildTLSOptions(config TLSConfig) (*gocql.SslOptions, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         config.ServerName,
		InsecureSkipVerify: config.InsecureSkipVerify,
	}

	if config.CAFile != "" {
		certificateAuthorities, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read TLS CA file: %w", err)
		}
		rootCAs, err := x509.SystemCertPool()
		if err != nil || rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}
		if !rootCAs.AppendCertsFromPEM(certificateAuthorities) {
			return nil, errors.New("TLS CA file contains no valid certificates")
		}
		tlsConfig.RootCAs = rootCAs
	}

	if config.CertificateFile != "" {
		certificate, err := tls.LoadX509KeyPair(
			config.CertificateFile,
			config.PrivateKeyFile,
		)
		if err != nil {
			return nil, fmt.Errorf("load TLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	return &gocql.SslOptions{Config: tlsConfig}, nil
}
