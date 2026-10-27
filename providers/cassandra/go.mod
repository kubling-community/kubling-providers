module github.com/kubling-community/kubling-providers/providers/cassandra

go 1.26.0

require (
	github.com/apache/cassandra-gocql-driver/v2 v2.1.2
	github.com/kubling-community/kubling-grpc/sdk-go v1.1.1
	github.com/kubling-community/kubling-providers/sdk-go v0.6.1
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.11
	gopkg.in/inf.v0 v0.9.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)

replace github.com/kubling-community/kubling-providers/sdk-go => ../../sdk-go
