package cassandra

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2"
)

func TestExampleKublingDDLIncludesEquivalentTablesAndNamespace(t *testing.T) {
	ddl, err := os.ReadFile("schema.example.sql")
	if err != nil {
		t.Fatalf("os.ReadFile(schema.example.sql) error = %v", err)
	}
	text := string(ddl)
	for _, table := range []string{"PROJECT", "TASK", "AUDIT_EVENT", "TYPE_SAMPLE"} {
		if !strings.Contains(text, "CREATE FOREIGN TABLE "+table) {
			t.Fatalf("schema.example.sql is missing table %s", table)
		}
	}
	if got := strings.Count(text, `"kbl.namespace" 'sample'`); got != 4 {
		t.Fatalf("schema.example.sql namespace properties = %d, want 4", got)
	}
}

func TestLoadConfig(t *testing.T) {
	path := writeTestConfig(t, `
namespaces:
  analytics:
    hosts:
      - cassandra-1.internal
      - cassandra-2.internal
    port: 9142
    localDataCenter: dc-west
    keyspace: analytics
    username: kubling
    password: secret
    consistency: one
    connectTimeout: 7s
    queryTimeout: 3s
    connectionsPerHost: 4
    tls:
      serverName: cassandra.internal
      insecureSkipVerify: true
`)

	config, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	dataSource := config.DataSources["analytics"]
	if len(dataSource.Hosts) != 2 || dataSource.Hosts[0] != "cassandra-1.internal" {
		t.Fatalf("LoadConfig() hosts = %v", dataSource.Hosts)
	}
	if dataSource.Port != 9142 {
		t.Fatalf("LoadConfig() port = %d, want 9142", dataSource.Port)
	}
	if dataSource.LocalDataCenter != "dc-west" {
		t.Fatalf("LoadConfig() local data center = %q", dataSource.LocalDataCenter)
	}
	if dataSource.Keyspace != "analytics" {
		t.Fatalf("LoadConfig() keyspace = %q", dataSource.Keyspace)
	}
	if dataSource.Consistency != "ONE" {
		t.Fatalf("LoadConfig() consistency = %q, want ONE", dataSource.Consistency)
	}
	if dataSource.ConnectTimeout != 7*time.Second {
		t.Fatalf("LoadConfig() connect timeout = %v", dataSource.ConnectTimeout)
	}
	if dataSource.QueryTimeout != 3*time.Second {
		t.Fatalf("LoadConfig() query timeout = %v", dataSource.QueryTimeout)
	}
	if dataSource.ConnectionsPerHost != 4 {
		t.Fatalf("LoadConfig() connections per host = %d", dataSource.ConnectionsPerHost)
	}
	if dataSource.TLS == nil || dataSource.TLS.ServerName != "cassandra.internal" {
		t.Fatalf("LoadConfig() TLS = %#v", dataSource.TLS)
	}
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
	path := writeTestConfig(t, `
namespaces:
  analytics:
    hosts: [localhost]
    keyspace: analytics
    unexpected: true
`)

	if _, err := LoadConfig(path); err == nil {
		t.Fatal("LoadConfig() error = nil, want unknown field error")
	}
}

func TestNormalizeConfigDefaultsAndCopies(t *testing.T) {
	hosts := []string{" localhost "}
	config, err := normalizeConfig(Config{
		DataSources: map[string]DataSourceConfig{
			"default": {
				Hosts:    hosts,
				Keyspace: " inventory ",
			},
		},
	})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}

	hosts[0] = "changed"
	dataSource := config.DataSources["default"]
	if dataSource.Hosts[0] != "localhost" {
		t.Fatalf("normalizeConfig() host = %q, want localhost", dataSource.Hosts[0])
	}
	if dataSource.Keyspace != "inventory" {
		t.Fatalf("normalizeConfig() keyspace = %q, want inventory", dataSource.Keyspace)
	}
	if dataSource.Port != defaultPort {
		t.Fatalf("normalizeConfig() port = %d, want %d", dataSource.Port, defaultPort)
	}
	if dataSource.ConnectTimeout != defaultConnectTimeout {
		t.Fatalf("normalizeConfig() connect timeout = %v", dataSource.ConnectTimeout)
	}
	if dataSource.QueryTimeout != defaultQueryTimeout {
		t.Fatalf("normalizeConfig() query timeout = %v", dataSource.QueryTimeout)
	}
	if dataSource.Consistency != defaultConsistency {
		t.Fatalf("normalizeConfig() consistency = %q", dataSource.Consistency)
	}
}

func TestNormalizeConfigRejectsInvalidDataSources(t *testing.T) {
	tests := map[string]Config{
		"empty catalog": {},
		"blank reference": {
			DataSources: map[string]DataSourceConfig{" ": validTestDataSource()},
		},
		"missing hosts": {
			DataSources: map[string]DataSourceConfig{
				"source": {Keyspace: "inventory"},
			},
		},
		"missing keyspace": {
			DataSources: map[string]DataSourceConfig{
				"source": {Hosts: []string{"localhost"}},
			},
		},
		"partial credentials": {
			DataSources: map[string]DataSourceConfig{
				"source": {
					Hosts:    []string{"localhost"},
					Keyspace: "inventory",
					Username: "kubling",
				},
			},
		},
		"invalid consistency": {
			DataSources: map[string]DataSourceConfig{
				"source": {
					Hosts:       []string{"localhost"},
					Keyspace:    "inventory",
					Consistency: "eventually",
				},
			},
		},
		"partial TLS identity": {
			DataSources: map[string]DataSourceConfig{
				"source": {
					Hosts:    []string{"localhost"},
					Keyspace: "inventory",
					TLS: &TLSConfig{
						CertificateFile: "client.crt",
					},
				},
			},
		},
	}

	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := normalizeConfig(config); err == nil {
				t.Fatal("normalizeConfig() error = nil, want validation error")
			}
		})
	}
}

func TestBuildClusterConfig(t *testing.T) {
	dataSource, err := normalizeDataSource(DataSourceConfig{
		Hosts:              []string{"cassandra.internal"},
		Port:               9142,
		LocalDataCenter:    "dc-west",
		Keyspace:           "inventory",
		Username:           "kubling",
		Password:           "secret",
		Consistency:        "one",
		ConnectTimeout:     5 * time.Second,
		QueryTimeout:       2 * time.Second,
		ConnectionsPerHost: 3,
		TLS: &TLSConfig{
			ServerName:         "cassandra.internal",
			InsecureSkipVerify: true,
		},
	})
	if err != nil {
		t.Fatalf("normalizeDataSource() error = %v", err)
	}

	cluster, err := buildClusterConfig(dataSource)
	if err != nil {
		t.Fatalf("buildClusterConfig() error = %v", err)
	}
	if cluster.Port != 9142 || cluster.Keyspace != "inventory" {
		t.Fatalf("buildClusterConfig() endpoint = port %d, keyspace %q", cluster.Port, cluster.Keyspace)
	}
	if cluster.ConnectTimeout != 5*time.Second || cluster.Timeout != 2*time.Second {
		t.Fatalf("buildClusterConfig() timeouts = %v, %v", cluster.ConnectTimeout, cluster.Timeout)
	}
	if cluster.NumConns != 3 {
		t.Fatalf("buildClusterConfig() connections = %d, want 3", cluster.NumConns)
	}
	if cluster.Consistency != gocql.One {
		t.Fatalf("buildClusterConfig() consistency = %v, want ONE", cluster.Consistency)
	}
	authenticator, ok := cluster.Authenticator.(gocql.PasswordAuthenticator)
	if !ok || authenticator.Username != "kubling" || authenticator.Password != "secret" {
		t.Fatalf("buildClusterConfig() authenticator = %#v", cluster.Authenticator)
	}
	if cluster.SslOpts == nil || cluster.SslOpts.Config.ServerName != "cassandra.internal" {
		t.Fatalf("buildClusterConfig() TLS = %#v", cluster.SslOpts)
	}
	if cluster.PoolConfig.HostSelectionPolicy == nil {
		t.Fatal("buildClusterConfig() host selection policy = nil")
	}
}

func validTestDataSource() DataSourceConfig {
	return DataSourceConfig{
		Hosts:    []string{"localhost"},
		Keyspace: "inventory",
	}
}

func writeTestConfig(t *testing.T, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "cassandra.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	return path
}
