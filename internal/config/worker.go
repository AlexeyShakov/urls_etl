package config

type WorkersConfig struct {
	ChannelsLen  int
	WorkersCount int
}

func NewDefaultWorkersConfig() WorkersConfig {
	return WorkersConfig{
		ChannelsLen:  10,
		WorkersCount: 2,
	}
}
