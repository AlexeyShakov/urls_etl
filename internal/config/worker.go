package config

type WorkersConfig struct {
	RequestChannelLen int
	WorkerChannelLen  int
	WorkersCount      int
}

func NewDefaultWorkersConfig() WorkersConfig {
	return WorkersConfig{
		RequestChannelLen: 10,
		WorkerChannelLen:  10,
		WorkersCount:      2,
	}
}
