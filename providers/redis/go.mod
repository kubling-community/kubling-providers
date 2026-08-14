module github.com/kubling-community/kubling-providers/providers/redis

go 1.26.0

require (
	github.com/kubling-community/kubling-grpc/sdk-go v0.1.1
	github.com/kubling-community/kubling-providers/sdk-go v0.1.0
	github.com/redis/go-redis/v9 v9.21.0
	google.golang.org/grpc v1.83.0
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)

replace github.com/kubling-community/kubling-providers/sdk-go => ../../sdk-go
