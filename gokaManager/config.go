package gokaManager

import (
	"os"

	"gopkg.in/yaml.v2"
)

// AppConfig 구조체 정의
type AppConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
	Group   string   `yaml:"group"`
	SASL    struct {
		Enable    bool   `yaml:"enable"`
		User      string `yaml:"user"`
		Password  string `yaml:"password"`
		Mechanism string `yaml:"mechanism"`
	} `yaml:"sasl"`
	TLS struct {
		Enable bool `yaml:"enable"`
	} `yaml:"tls"`
	Replication struct {
		Table  int `yaml:"table"`
		Stream int `yaml:"stream"`
	} `yaml:"replication"`
	Partitions int `yaml:"partitions"`
}

// LoadConfig 함수
func LoadConfig(path string) (AppConfig, error) {
	var c AppConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return c, err
	}
	return c, nil
}
