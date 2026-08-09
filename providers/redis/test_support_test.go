package redis

import (
	"context"
	"errors"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

const testNamespace = "some/path/to/redis"

type fakeRedisClient struct {
	mu         sync.Mutex
	hashes     map[string]map[string]string
	otherTypes map[string]string
	pingErr    error
	closeErr   error
	closeCount atomic.Int32
	scanCalls  atomic.Int32
}

func newFakeRedisClient() *fakeRedisClient {
	return &fakeRedisClient{
		hashes:     make(map[string]map[string]string),
		otherTypes: make(map[string]string),
	}
}

func (c *fakeRedisClient) Ping(context.Context) error { return c.pingErr }

func (c *fakeRedisClient) HGetAll(_ context.Context, key string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return cloneHash(c.hashes[key]), nil
}

func (c *fakeRedisClient) Scan(_ context.Context, _ uint64, pattern string, _ int64) ([]string, uint64, error) {
	c.scanCalls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.hashes)+len(c.otherTypes))
	for key := range c.hashes {
		matched, err := path.Match(pattern, key)
		if err != nil {
			return nil, 0, err
		}
		if matched {
			keys = append(keys, key)
		}
	}
	for key := range c.otherTypes {
		matched, err := path.Match(pattern, key)
		if err != nil {
			return nil, 0, err
		}
		if matched {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, 0, nil
}

func (c *fakeRedisClient) Exists(_ context.Context, key string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, hashExists := c.hashes[key]
	_, otherExists := c.otherTypes[key]
	return hashExists || otherExists, nil
}

func (c *fakeRedisClient) EvalInt(_ context.Context, _ string, keys []string, args ...any) (int64, error) {
	if len(keys) != 1 || len(args)%2 != 0 {
		return 0, errors.New("invalid fake EvalInt arguments")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.hashes[keys[0]]; exists {
		return 0, nil
	}
	if _, exists := c.otherTypes[keys[0]]; exists {
		return 0, nil
	}
	hash := make(map[string]string, len(args)/2)
	for index := 0; index < len(args); index += 2 {
		name, nameOK := args[index].(string)
		value, valueOK := args[index+1].(string)
		if !nameOK || !valueOK {
			return 0, errors.New("invalid fake hash value")
		}
		hash[name] = value
	}
	c.hashes[keys[0]] = hash
	return 1, nil
}

func (c *fakeRedisClient) HSet(_ context.Context, key string, fields map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hashes[key] == nil {
		c.hashes[key] = make(map[string]string)
	}
	for name, value := range fields {
		c.hashes[key][name] = value
	}
	return nil
}

func (c *fakeRedisClient) HDel(_ context.Context, key string, fields ...string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var deleted int64
	for _, field := range fields {
		if _, exists := c.hashes[key][field]; exists {
			delete(c.hashes[key], field)
			deleted++
		}
	}
	if len(c.hashes[key]) == 0 {
		delete(c.hashes, key)
	}
	return deleted, nil
}

func (c *fakeRedisClient) Del(_ context.Context, keys ...string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var deleted int64
	for _, key := range keys {
		if _, exists := c.hashes[key]; exists {
			delete(c.hashes, key)
			deleted++
		}
		if _, exists := c.otherTypes[key]; exists {
			delete(c.otherTypes, key)
			deleted++
		}
	}
	return deleted, nil
}

func (c *fakeRedisClient) Type(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.hashes[key]; exists {
		return "hash", nil
	}
	if keyType := c.otherTypes[key]; keyType != "" {
		return keyType, nil
	}
	return "none", nil
}

func (c *fakeRedisClient) Close() error {
	c.closeCount.Add(1)
	return c.closeErr
}

func cloneHash(source map[string]string) map[string]string {
	if source == nil {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(source))
	for name, value := range source {
		cloned[name] = value
	}
	return cloned
}

func testProvider(t *testing.T, clients map[string]*fakeRedisClient) *Provider {
	t.Helper()
	config, err := normalizeConfig(Config{Namespaces: map[string]NamespaceConfig{
		testNamespace: {
			Tables: []TableConfig{{
				Name:       "TASK",
				KeyPrefix:  "TASK:",
				Key:        ColumnConfig{Name: "id", Type: kublingv1.ValueType_VALUE_TYPE_STRING},
				Annotation: "Work items stored as Redis hashes.",
				Updatable:  true,
				Fields: []ColumnConfig{
					{Name: "title", Type: kublingv1.ValueType_VALUE_TYPE_STRING, Updatable: true},
					{Name: "completed", Type: kublingv1.ValueType_VALUE_TYPE_BOOLEAN, Nullable: true, Updatable: true},
					{Name: "priority", Type: kublingv1.ValueType_VALUE_TYPE_INTEGER, Nullable: true, Updatable: true},
					{Name: "note", Type: kublingv1.ValueType_VALUE_TYPE_STRING, Nullable: true, Updatable: true},
				},
			}},
		},
	}})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	return newProvider(config, func(config NamespaceConfig) redisClient {
		client := clients[config.Address]
		if client == nil {
			client = clients[testNamespace]
		}
		return client
	})
}

func testConnection(t *testing.T, client *fakeRedisClient) *Connection {
	t.Helper()
	provider := testProvider(t, map[string]*fakeRedisClient{testNamespace: client})
	opened, err := provider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return opened.(*Connection)
}

func taskReference() *providerv1.EntityReference {
	return &providerv1.EntityReference{Name: "TASK", Namespace: testNamespace}
}

func literalExpression(value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Literal{
		Literal: &providerv1.Literal{Value: value},
	}}
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func integerValue(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: value}}
}

func booleanValue(value bool) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{BooleanValue: value}}
}

func equalExpression(field string, value *kublingv1.Value) *providerv1.Expression {
	return comparisonExpression(
		providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
		field,
		value,
	)
}

func comparisonExpression(operator providerv1.ComparisonOperator, field string, value *kublingv1.Value) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Comparison{
		Comparison: &providerv1.ComparisonExpression{
			Operator: operator,
			Left:     fieldExpression(field),
			Right:    literalExpression(value),
		},
	}}
}

func likeExpression(field string, pattern string) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Pattern{
		Pattern: &providerv1.PatternExpression{
			Operator: providerv1.PatternOperator_PATTERN_OPERATOR_LIKE,
			Value:    fieldExpression(field),
			Pattern:  literalExpression(stringValue(pattern)),
		},
	}}
}

func andExpression(expressions ...*providerv1.Expression) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Logical{
		Logical: &providerv1.LogicalExpression{
			Operator: providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
			Operands: expressions,
		},
	}}
}
