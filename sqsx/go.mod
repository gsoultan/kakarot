module github.com/gsoultan/kakarot/sqsx

go 1.27.4

require (
	github.com/aws/aws-sdk-go-v2 v1.48.0
	github.com/aws/aws-sdk-go-v2/service/sqs v1.53.0
	github.com/gsoultan/kakarot v0.1.0
)

require (
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.12 // indirect
	github.com/aws/smithy-go v1.28.9 // indirect
)

replace github.com/gsoultan/kakarot => ..
