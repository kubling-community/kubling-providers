module github.com/kubling-community/kubling-providers/providers/openapi

go 1.26.0

require (
	github.com/kubling-community/kubling-grpc/sdk-go v0.1.1
	github.com/kubling-community/kubling-providers/sdk-go v0.1.0
	github.com/pb33f/libopenapi v0.38.7
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/buger/jsonparser v1.1.2 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pb33f/jsonpath v0.8.2 // indirect
	github.com/pb33f/ordered-map/v2 v2.3.1 // indirect
	github.com/rogpeppe/go-internal v1.16.0 // indirect
	go.yaml.in/yaml/v4 v4.0.0-rc.6 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
)

replace github.com/kubling-community/kubling-providers/sdk-go => ../../sdk-go
