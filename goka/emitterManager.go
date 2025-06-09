package goka

import (
	"errors"
	"log"
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
func GetEmitterManager() *EmitterManager {
	once.Do(func() {
		emitterManager = &EmitterManager{
			emitters: make(map[string]*goka.Emitter),
			metadata: make(map[string]*EmitterMeta),
		}
		// config.yaml, emitter_keys.yaml 로드
		emitterManager.loadConfig()
	})
	log.Println("EmitterManager initialized")
	log.Printf("Current emitters: %v", emitterManager.emitters)
	log.Printf("Current config: %v", emitterManager.config)
	return emitterManager
}

// config.yaml 로드
func (m *EmitterManager) loadConfig() {
	cfg, err := LoadConfig("config.yaml")
	if err != nil {
		panic("Config load error: " + err.Error())
	}
	m.config = &cfg
}

// Emitter 생성 및 등록
func (m *EmitterManager) AddEmitter(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.emitters[topic]; exists {
		return errors.New("Emitter already exists")
	}

	em, err := goka.NewEmitter(m.config.Brokers, goka.Stream(topic), new(codec.String))
	if err != nil {
		return err
	}
	log.Printf("Emitter %s created", topic)
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
