package gokaManager

import (
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
	"gopkg.in/yaml.v2"
)

// GlobalConfig 구조체 정의 - 전역 설정
type GlobalConfig struct {
	SASL struct {
		Enable    bool   `yaml:"enable"`
		User      string `yaml:"user"`
		Password  string `yaml:"password"`
		Mechanism string `yaml:"mechanism"`
	} `yaml:"sasl"`
	TLS struct {
		Enable bool `yaml:"enable"`
	} `yaml:"tls"`
	Consumer struct {
		InitialOffset string `yaml:"initial_offset"` // "oldest" or "newest"
	} `yaml:"consumer"`
}

// BrokerConfig 구조체 정의 - 브로커 관련 설정
type BrokerConfig struct {
	Brokers     []string `yaml:"brokers"`
	Topic       string   `yaml:"topic"`
	Group       string   `yaml:"group"`
	Replication struct {
		Table  int `yaml:"table"`
		Stream int `yaml:"stream"`
	} `yaml:"replication"`
	Partitions int `yaml:"partitions"`
}

// AppConfig 구조체 정의 - 전체 설정
type AppConfig struct {
	Global GlobalConfig `yaml:"global"`
	Broker BrokerConfig `yaml:"broker"`
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

// InitGlobalConfig 함수 - Sarama 설정을 초기화하고 전역 설정을 적용
func InitGlobalConfig(cfg *AppConfig) (*sarama.Config, error) {
	config := goka.DefaultConfig()

	// Broker 연결 설정
	config.Net.MaxOpenRequests = 1
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	// SASL 인증 정보 설정
	config.Net.SASL.Enable = cfg.Global.SASL.Enable
	config.Net.SASL.User = cfg.Global.SASL.User
	config.Net.SASL.Password = cfg.Global.SASL.Password
	config.Net.SASL.Mechanism = sarama.SASLMechanism(cfg.Global.SASL.Mechanism)

	// TLS 활성화
	config.Net.TLS.Enable = cfg.Global.TLS.Enable

	// Consumer 설정
	if cfg.Global.Consumer.InitialOffset == "oldest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// 전역 설정 적용
	goka.ReplaceGlobalConfig(config)
	return config, nil
}
