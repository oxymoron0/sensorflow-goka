package gokaManager

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
)

type EmitterMeta struct {
	Topic string
}

type EmitterManager struct {
	emitters map[string]*goka.Emitter
	metadata map[string]*EmitterMeta
	mu       sync.RWMutex
	config   *AppConfig
}

var (
	emitterManager *EmitterManager
	once           sync.Once
)

// 싱글톤 인스턴스 반환
func GetEmitterManager() (*EmitterManager, error) {
	var initErr error
	once.Do(func() {
		emitterManager = &EmitterManager{
			emitters: make(map[string]*goka.Emitter),
			metadata: make(map[string]*EmitterMeta),
		}
		// config.yaml, emitter_keys.yaml 로드
		if err := emitterManager.loadConfig(); err != nil {
			initErr = err
			return
		}
	})
	if initErr != nil {
		return nil, initErr
	}
	log.Println("EmitterManager initialized")
	log.Printf("Current emitters: %v", emitterManager.emitters)
	log.Printf("Current config: %v", emitterManager.config)
	return emitterManager, nil
}

// config.yaml 로드
func (m *EmitterManager) loadConfig() error {
	cfg, err := LoadConfig("config.yaml")
	if err != nil {
		return err
	}
	m.config = &cfg
	return nil
}

// Emitter 생성 및 등록
func (m *EmitterManager) AddEmitter(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.emitters[topic]; exists {
		return errors.New("Emitter already exists")
	}

	// Emitter 옵션 설정
	options := []goka.EmitterOption{
		goka.WithEmitterClientID(fmt.Sprintf("emitter-%s", topic)), // 클라이언트 ID 설정
		goka.WithEmitterDefaultHeaders(goka.Headers{
			"source": []byte("emitter-manager"),
		}), // 기본 헤더 추가
		goka.WithEmitterLogger(log.New(os.Stdout, fmt.Sprintf("[Emitter-%s] ", topic), log.LstdFlags)), // 커스텀 로거 설정
	}

	em, err := goka.NewEmitter(m.config.Broker.Brokers, goka.Stream(topic), new(codec.String), options...)
	if err != nil {
		return err
	}
	log.Printf("Emitter %s created with custom options", topic)
	m.emitters[topic] = em
	m.metadata[topic] = &EmitterMeta{Topic: topic}
	return nil
}

// Emitter 반환
func (m *EmitterManager) GetEmitter(topic string) (*goka.Emitter, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	em, ok := m.emitters[topic]
	if !ok {
		return nil, errors.New("Emitter not found")
	}
	return em, nil
}

// Emitter 삭제
func (m *EmitterManager) RemoveEmitter(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	em, ok := m.emitters[topic]
	if !ok {
		return errors.New("Emitter not found")
	}
	em.Finish() // Graceful shutdown
	delete(m.emitters, topic)
	delete(m.metadata, topic)
	return nil
}

// EmitterCallback is a function type that takes an emitter as parameter
type EmitterCallback func(emitter *goka.Emitter) error

// ExecuteWithEmitter executes the given callback function with the emitter for the specified topic
func (m *EmitterManager) ExecuteWithEmitter(topic string, callback EmitterCallback) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if emitter exists
	emitter, exists := m.emitters[topic]
	if !exists {
		// Create new emitter if it doesn't exist
		if err := m.AddEmitter(topic); err != nil {
			return fmt.Errorf("failed to create emitter for topic %s: %v", topic, err)
		}
		emitter = m.emitters[topic]
	}

	// Execute callback with the emitter
	return callback(emitter)
}
