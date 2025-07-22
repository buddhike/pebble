module sample

go 1.23.4

replace github.com/buddhike/pebble => ../src

require (
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.35.3
	github.com/buddhike/pebble v0.0.0-00010101000000-000000000000
)

require (
	github.com/aws/aws-sdk-go-v2 v1.36.5 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.36 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.36 // indirect
	github.com/aws/smithy-go v1.22.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
)
