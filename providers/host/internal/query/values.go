package query

import (
	"fmt"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/protobuf/proto"
)

func hostValues(snapshot model.HostSnapshot, at time.Time) map[string]*kublingv1.Value {
	state := "offline"
	lastSeen := nullValue()
	if snapshot.Session != nil {
		if snapshot.Session.ActiveAt(at) {
			state = "online"
		}
		if !snapshot.Session.LastSeenAt.IsZero() {
			lastSeen = timestampValue(snapshot.Session.LastSeenAt)
		}
	}
	hostname := nullValue()
	if snapshot.Identity.Hostname != "" {
		hostname = stringValue(snapshot.Identity.Hostname)
	}
	return map[string]*kublingv1.Value{
		hostschema.NamespaceColumn: stringValue(snapshot.Identity.Key.Namespace),
		hostschema.HostIDColumn:    stringValue(snapshot.Identity.Key.ID),
		hostschema.HostnameColumn:  hostname,
		"state":                    stringValue(state),
		"enrolled_at":              timestampValue(snapshot.Identity.EnrolledAt),
		"last_seen_at":             lastSeen,
	}
}

func agentValues(
	table string,
	target model.HostIdentity,
	columns []string,
	row *gatewayRow,
) (map[string]*kublingv1.Value, error) {
	valueCount := 0
	if row != nil {
		valueCount = len(row.values)
	}
	if row == nil || valueCount != len(columns) {
		return nil, fmt.Errorf("%s row has %d values, want %d", table, valueCount, len(columns))
	}
	if err := hostschema.ValidateValues(table, columns, row.values); err != nil {
		return nil, err
	}
	values := map[string]*kublingv1.Value{
		hostschema.NamespaceColumn: stringValue(target.Key.Namespace),
		hostschema.HostIDColumn:    stringValue(target.Key.ID),
		hostschema.HostnameColumn:  nullValue(),
	}
	if target.Hostname != "" {
		values[hostschema.HostnameColumn] = stringValue(target.Hostname)
	}
	for index, column := range columns {
		values[column] = proto.Clone(row.values[index]).(*kublingv1.Value)
	}
	return values, nil
}

func projectValues(
	plan queryPlan,
	values map[string]*kublingv1.Value,
) (*providerv1.Tuple, error) {
	tuple := &providerv1.Tuple{Values: make([]*kublingv1.Value, 0, len(plan.projections))}
	for _, projection := range plan.projections {
		value := values[projection.sourceName]
		if value == nil {
			return nil, fmt.Errorf("projected value %q is missing", projection.sourceName)
		}
		tuple.Values = append(tuple.Values, proto.Clone(value).(*kublingv1.Value))
	}
	return tuple, nil
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func timestampValue(value time.Time) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_TimestampValue{
		TimestampValue: value.UTC().Format("2006-01-02T15:04:05.999999999"),
	}}
}

func nullValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_NullValue{NullValue: &kublingv1.NullValue{}}}
}
