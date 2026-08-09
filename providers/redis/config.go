package redis

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"gopkg.in/yaml.v3"
)

const (
	defaultAddress        = "127.0.0.1:6379"
	defaultDialTimeout    = 5 * time.Second
	defaultReadTimeout    = 5 * time.Second
	defaultWriteTimeout   = 5 * time.Second
	defaultPoolSize       = 10
	defaultScanCount      = 500
	defaultMaxScannedKeys = 10_000
)

// Config defines every logical Redis namespace exposed by this provider.
type Config struct {
	Namespaces map[string]NamespaceConfig
}

// NamespaceConfig configures one Redis database and its relational model.
type NamespaceConfig struct {
	Address           string
	Username          string
	Password          string
	Database          int
	DialTimeout       time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	PoolSize          int
	ScanCount         int64
	MaxScannedKeys    int
	TLS               *TLSConfig
	Tables            []TableConfig
	tablesByName      map[string]*TableConfig
	tablesByKeyPrefix map[string]*TableConfig
}

// TLSConfig configures TLS for one Redis namespace.
type TLSConfig struct {
	ServerName         string
	InsecureSkipVerify bool
}

// TableConfig maps one logical table to Redis hashes.
type TableConfig struct {
	Name       string
	KeyPrefix  string
	Key        ColumnConfig
	Fields     []ColumnConfig
	Annotation string
	Updatable  bool
	columns    map[string]*ColumnConfig
}

// ColumnConfig describes one logical Redis hash column.
type ColumnConfig struct {
	Name       string
	Type       kublingv1.ValueType
	Nullable   bool
	Updatable  bool
	Annotation string
}

type fileConfig struct {
	Namespaces map[string]fileNamespaceConfig `yaml:"namespaces"`
}

type fileNamespaceConfig struct {
	Address        string         `yaml:"address"`
	Username       string         `yaml:"username"`
	Password       string         `yaml:"password"`
	Database       int            `yaml:"database"`
	DialTimeout    string         `yaml:"dialTimeout"`
	ReadTimeout    string         `yaml:"readTimeout"`
	WriteTimeout   string         `yaml:"writeTimeout"`
	PoolSize       int            `yaml:"poolSize"`
	ScanCount      int64          `yaml:"scanCount"`
	MaxScannedKeys int            `yaml:"maxScannedKeys"`
	TLS            *fileTLSConfig `yaml:"tls"`
	SchemaFile     string         `yaml:"schemaFile"`
}

type fileTLSConfig struct {
	ServerName         string `yaml:"serverName"`
	InsecureSkipVerify bool   `yaml:"insecureSkipVerify"`
}

type fileSchema struct {
	Tables []fileTableConfig `yaml:"tables"`
}

type fileTableConfig struct {
	Name       string             `yaml:"name"`
	Structure  string             `yaml:"structure"`
	KeyPrefix  string             `yaml:"keyPrefix"`
	Key        fileColumnConfig   `yaml:"key"`
	Fields     []fileColumnConfig `yaml:"fields"`
	Annotation string             `yaml:"annotation"`
	Updatable  *bool              `yaml:"updatable"`
}

type fileColumnConfig struct {
	Name       string `yaml:"name"`
	Type       string `yaml:"type"`
	Nullable   *bool  `yaml:"nullable"`
	Updatable  *bool  `yaml:"updatable"`
	Annotation string `yaml:"annotation"`
}

// LoadConfig reads a strict provider YAML file and every referenced schema.
func LoadConfig(path string) (Config, error) {
	var serialized fileConfig
	if err := decodeYAMLFile(path, &serialized); err != nil {
		return Config{}, fmt.Errorf("decode Redis provider config: %w", err)
	}

	baseDirectory := filepath.Dir(path)
	config := Config{Namespaces: make(map[string]NamespaceConfig, len(serialized.Namespaces))}
	for namespace, fileNamespace := range serialized.Namespaces {
		namespaceConfig, err := fileNamespace.toConfig(baseDirectory)
		if err != nil {
			return Config{}, fmt.Errorf("namespace %q: %w", namespace, err)
		}
		config.Namespaces[namespace] = namespaceConfig
	}

	return normalizeConfig(config)
}

func (c fileNamespaceConfig) toConfig(baseDirectory string) (NamespaceConfig, error) {
	if strings.TrimSpace(c.SchemaFile) == "" {
		return NamespaceConfig{}, errors.New("schemaFile is required")
	}
	schemaPath := c.SchemaFile
	if !filepath.IsAbs(schemaPath) {
		schemaPath = filepath.Join(baseDirectory, schemaPath)
	}
	var schema fileSchema
	if err := decodeYAMLFile(schemaPath, &schema); err != nil {
		return NamespaceConfig{}, fmt.Errorf("decode schema file %q: %w", c.SchemaFile, err)
	}

	dialTimeout, err := parseDuration("dialTimeout", c.DialTimeout, defaultDialTimeout)
	if err != nil {
		return NamespaceConfig{}, err
	}
	readTimeout, err := parseDuration("readTimeout", c.ReadTimeout, defaultReadTimeout)
	if err != nil {
		return NamespaceConfig{}, err
	}
	writeTimeout, err := parseDuration("writeTimeout", c.WriteTimeout, defaultWriteTimeout)
	if err != nil {
		return NamespaceConfig{}, err
	}

	tables := make([]TableConfig, 0, len(schema.Tables))
	for index, table := range schema.Tables {
		converted, err := table.toConfig()
		if err != nil {
			return NamespaceConfig{}, fmt.Errorf("table %d: %w", index, err)
		}
		tables = append(tables, converted)
	}

	namespace := NamespaceConfig{
		Address:        c.Address,
		Username:       c.Username,
		Password:       c.Password,
		Database:       c.Database,
		DialTimeout:    dialTimeout,
		ReadTimeout:    readTimeout,
		WriteTimeout:   writeTimeout,
		PoolSize:       c.PoolSize,
		ScanCount:      c.ScanCount,
		MaxScannedKeys: c.MaxScannedKeys,
		Tables:         tables,
	}
	if c.TLS != nil {
		namespace.TLS = &TLSConfig{
			ServerName:         c.TLS.ServerName,
			InsecureSkipVerify: c.TLS.InsecureSkipVerify,
		}
	}
	return namespace, nil
}

func (c fileTableConfig) toConfig() (TableConfig, error) {
	if structure := strings.ToLower(strings.TrimSpace(c.Structure)); structure != "hash" {
		return TableConfig{}, fmt.Errorf("unsupported structure %q; first implementation supports only hash", c.Structure)
	}
	key, err := c.Key.toConfig(false, false)
	if err != nil {
		return TableConfig{}, fmt.Errorf("key: %w", err)
	}
	fields := make([]ColumnConfig, 0, len(c.Fields))
	for index, field := range c.Fields {
		converted, err := field.toConfig(true, true)
		if err != nil {
			return TableConfig{}, fmt.Errorf("field %d: %w", index, err)
		}
		fields = append(fields, converted)
	}
	updatable := true
	if c.Updatable != nil {
		updatable = *c.Updatable
	}
	return TableConfig{
		Name:       c.Name,
		KeyPrefix:  c.KeyPrefix,
		Key:        key,
		Fields:     fields,
		Annotation: c.Annotation,
		Updatable:  updatable,
	}, nil
}

func (c fileColumnConfig) toConfig(defaultNullable bool, defaultUpdatable bool) (ColumnConfig, error) {
	valueType, err := parseValueType(c.Type)
	if err != nil {
		return ColumnConfig{}, err
	}
	nullable := defaultNullable
	if c.Nullable != nil {
		nullable = *c.Nullable
	}
	updatable := defaultUpdatable
	if c.Updatable != nil {
		updatable = *c.Updatable
	}
	return ColumnConfig{
		Name:       c.Name,
		Type:       valueType,
		Nullable:   nullable,
		Updatable:  updatable,
		Annotation: c.Annotation,
	}, nil
}

func normalizeConfig(config Config) (Config, error) {
	if len(config.Namespaces) == 0 {
		return Config{}, errors.New("at least one Redis namespace is required")
	}
	normalized := Config{Namespaces: make(map[string]NamespaceConfig, len(config.Namespaces))}
	for namespace, candidate := range config.Namespaces {
		if namespace == "" || strings.TrimSpace(namespace) != namespace {
			return Config{}, fmt.Errorf("invalid namespace %q", namespace)
		}
		converted, err := normalizeNamespace(candidate)
		if err != nil {
			return Config{}, fmt.Errorf("namespace %q: %w", namespace, err)
		}
		normalized.Namespaces[namespace] = converted
	}
	return normalized, nil
}

func normalizeNamespace(config NamespaceConfig) (NamespaceConfig, error) {
	normalized := config
	normalized.Tables = append([]TableConfig(nil), config.Tables...)
	if config.TLS != nil {
		copiedTLS := *config.TLS
		normalized.TLS = &copiedTLS
	}
	normalized.Address = strings.TrimSpace(config.Address)
	if normalized.Address == "" {
		normalized.Address = defaultAddress
	}
	if normalized.Database < 0 {
		return NamespaceConfig{}, errors.New("database must not be negative")
	}
	if normalized.DialTimeout == 0 {
		normalized.DialTimeout = defaultDialTimeout
	}
	if normalized.ReadTimeout == 0 {
		normalized.ReadTimeout = defaultReadTimeout
	}
	if normalized.WriteTimeout == 0 {
		normalized.WriteTimeout = defaultWriteTimeout
	}
	if normalized.PoolSize == 0 {
		normalized.PoolSize = defaultPoolSize
	}
	if normalized.PoolSize < 1 {
		return NamespaceConfig{}, errors.New("poolSize must be positive")
	}
	if normalized.ScanCount == 0 {
		normalized.ScanCount = defaultScanCount
	}
	if normalized.ScanCount < 1 {
		return NamespaceConfig{}, errors.New("scanCount must be positive")
	}
	if normalized.MaxScannedKeys == 0 {
		normalized.MaxScannedKeys = defaultMaxScannedKeys
	}
	if normalized.MaxScannedKeys < 1 {
		return NamespaceConfig{}, errors.New("maxScannedKeys must be positive")
	}
	if len(normalized.Tables) == 0 {
		return NamespaceConfig{}, errors.New("schema requires at least one table")
	}

	normalized.tablesByName = make(map[string]*TableConfig, len(normalized.Tables))
	normalized.tablesByKeyPrefix = make(map[string]*TableConfig, len(normalized.Tables))
	for index := range normalized.Tables {
		table, err := normalizeTable(normalized.Tables[index])
		if err != nil {
			return NamespaceConfig{}, fmt.Errorf("table %d: %w", index, err)
		}
		normalized.Tables[index] = table
		nameKey := strings.ToUpper(table.Name)
		if _, exists := normalized.tablesByName[nameKey]; exists {
			return NamespaceConfig{}, fmt.Errorf("duplicate table %q", table.Name)
		}
		if _, exists := normalized.tablesByKeyPrefix[table.KeyPrefix]; exists {
			return NamespaceConfig{}, fmt.Errorf("duplicate keyPrefix %q", table.KeyPrefix)
		}
		normalized.tablesByName[nameKey] = &normalized.Tables[index]
		normalized.tablesByKeyPrefix[table.KeyPrefix] = &normalized.Tables[index]
	}
	return normalized, nil
}

func normalizeTable(table TableConfig) (TableConfig, error) {
	normalized := table
	normalized.Fields = append([]ColumnConfig(nil), table.Fields...)
	normalized.Name = strings.TrimSpace(table.Name)
	if normalized.Name == "" {
		return TableConfig{}, errors.New("name is required")
	}
	if normalized.KeyPrefix == "" {
		normalized.KeyPrefix = normalized.Name + ":"
	}
	key, err := normalizeColumn(table.Key, false)
	if err != nil {
		return TableConfig{}, fmt.Errorf("key: %w", err)
	}
	if !keyTypeSupported(key.Type) {
		return TableConfig{}, fmt.Errorf("key type %s is not supported", key.Type)
	}
	normalized.Key = key
	normalized.columns = map[string]*ColumnConfig{strings.ToUpper(key.Name): &normalized.Key}
	for index := range normalized.Fields {
		field, err := normalizeColumn(normalized.Fields[index], true)
		if err != nil {
			return TableConfig{}, fmt.Errorf("field %d: %w", index, err)
		}
		normalized.Fields[index] = field
		fieldKey := strings.ToUpper(field.Name)
		if _, exists := normalized.columns[fieldKey]; exists {
			return TableConfig{}, fmt.Errorf("duplicate column %q", field.Name)
		}
		normalized.columns[fieldKey] = &normalized.Fields[index]
	}
	return normalized, nil
}

func normalizeColumn(column ColumnConfig, allowNullable bool) (ColumnConfig, error) {
	normalized := column
	normalized.Name = strings.TrimSpace(column.Name)
	if normalized.Name == "" {
		return ColumnConfig{}, errors.New("name is required")
	}
	if normalized.Type == kublingv1.ValueType_VALUE_TYPE_UNKNOWN {
		return ColumnConfig{}, errors.New("type is required")
	}
	if !allowNullable {
		normalized.Nullable = false
		normalized.Updatable = false
	}
	return normalized, nil
}

func parseValueType(value string) (kublingv1.ValueType, error) {
	normalized := strings.ToUpper(strings.TrimSpace(value))
	aliases := map[string]string{
		"INT":     "INTEGER",
		"DECIMAL": "BIGDECIMAL",
		"BINARY":  "VARBINARY",
	}
	if alias := aliases[normalized]; alias != "" {
		normalized = alias
	}
	parsed, exists := kublingv1.ValueType_value["VALUE_TYPE_"+normalized]
	if !exists || parsed == int32(kublingv1.ValueType_VALUE_TYPE_UNKNOWN) {
		return kublingv1.ValueType_VALUE_TYPE_UNKNOWN, fmt.Errorf("unsupported Kubling type %q", value)
	}
	return kublingv1.ValueType(parsed), nil
}

func keyTypeSupported(valueType kublingv1.ValueType) bool {
	switch valueType {
	case kublingv1.ValueType_VALUE_TYPE_STRING,
		kublingv1.ValueType_VALUE_TYPE_CHAR,
		kublingv1.ValueType_VALUE_TYPE_BYTE,
		kublingv1.ValueType_VALUE_TYPE_SHORT,
		kublingv1.ValueType_VALUE_TYPE_INTEGER,
		kublingv1.ValueType_VALUE_TYPE_LONG,
		kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		return true
	default:
		return false
	}
}

func parseDuration(name string, value string, defaultValue time.Duration) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return defaultValue, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return parsed, nil
}

func decodeYAMLFile(path string, destination any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple YAML documents are not supported")
		}
		return err
	}
	return nil
}

func (c NamespaceConfig) tlsConfig() *tls.Config {
	if c.TLS == nil {
		return nil
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         c.TLS.ServerName,
		InsecureSkipVerify: c.TLS.InsecureSkipVerify,
	}
}

func sortedNamespaces(config Config) []string {
	namespaces := make([]string, 0, len(config.Namespaces))
	for namespace := range config.Namespaces {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	return namespaces
}
