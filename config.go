package shiva

type Config struct {
	BootstrapServers []string `env:"KAFKA_BOOTSTRAP_SERVERS,default=127.0.0.1:9092"`
}

func DefaultConfig() *Config {
	return nil
}
