module github.com/kubling-community/kubling-providers/providers/cassandra

go 1.26.0

require (
	github.com/apache/cassandra-gocql-driver/v2 v2.1.2
	github.com/kubling-community/kubling-grpc/sdk-go v0.1.1
	github.com/kubling-community/kubling-providers/sdk-go v0.1.0
	google.golang.org/grpc v1.82.1
	gopkg.in/inf.v0 v0.9.1
	gopkg.in/yaml.v3 v3.0.1
)

require (
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/kubling-community/kubling-providers/sdk-go => ../../sdk-go
