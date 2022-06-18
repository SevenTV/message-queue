package messagequeue

import "github.com/aws/aws-sdk-go-v2/aws"

type Config interface {
	isConfig()
}

type ConfigSQS struct {
	Region           string
	Credentials      aws.CredentialsProvider
	RetryMaxAttempts int
}

func (ConfigSQS) isConfig() {}

type ConfigRMQ struct {
	AmqpURI              string
	MaxReconnectAttempts int
}

func (ConfigRMQ) isConfig() {}

type ConfigMock struct{}

func (ConfigMock) isConfig() {}
