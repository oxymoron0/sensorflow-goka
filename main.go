package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	gokasdk "github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/oxymoron0/sensorflow-goka/goka"
)

var (
	cfg     goka.AppConfig
	brokers []string
	topic   gokasdk.Stream
	group   gokasdk.Group
	tmc     *gokasdk.TopicManagerConfig
)

func init() {
	var err error
	cfg, err = goka.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Config load error: %v", err)
	}
	brokers = cfg.Brokers
	topic = gokasdk.Stream(cfg.Topic)
	group = gokasdk.Group(cfg.Group)

	tmc = gokasdk.NewTopicManagerConfig()
	tmc.Table.Replication = cfg.Replication.Table
	tmc.Stream.Replication = cfg.Replication.Stream
}

// runEmitter는 메시지를 지속적으로 발행합니다. => Producer
func runEmitter() {
	// Emitter 생성: 브로커, 토픽, 코덱 지정
	emitter, err := gokasdk.NewEmitter(brokers, topic, new(codec.String))
	if err != nil {
		log.Fatalf("error creating emitter: %v", err)
	}
	defer emitter.Finish() // defer를 사용하여 모든 메시지 발행이 완료될 때까지 기다림

	for {
		time.Sleep(1 * time.Second)
		// "some-key"로 "some-value"를 동기적으로 발행
		err = emitter.EmitSync("some-key", "some-value")
		if err != nil {
			log.Fatalf("error emitting message: %v", err)
		}
	}
}

// runProcessor는 메시지를 처리하고 상태를 업데이트합니다. => Consumer
func runProcessor() {
	// 프로세스 콜백: "example-stream" 토픽에서 메시지가 전달될 때마다 호출됨
	cb := func(ctx gokasdk.Context, msg interface{}) {
		var counter int64
		// ctx.Value()를 사용하여 그룹 테이블에서 메시지 키에 저장된 값을 가져옴
		if val := ctx.Value(); val != nil {
			counter = val.(int64)
		}
		counter++
		// SetValue를 사용하여 증가된 카운터를 그룹 테이블에 저장
		ctx.SetValue(counter)
		log.Printf("key = %s, counter = %v, msg = %v", ctx.Key(), counter, msg)
	}

	// 프로세서 그룹 정의: 입력, 출력 및 직렬화 형식 정의
	// 그룹 테이블 토픽은 "example-group-table"
	g := gokasdk.DefineGroup(group,
		gokasdk.Input(topic, new(codec.String), cb), // 입력 스트림 및 콜백 정의
		gokasdk.Persist(new(codec.Int64)),           // 그룹 테이블에 정수형 값 지속
	)

	// 새로운 프로세서 생성
	p, err := gokasdk.NewProcessor(brokers,
		g,
		gokasdk.WithTopicManagerBuilder(gokasdk.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		gokasdk.WithConsumerGroupBuilder(gokasdk.DefaultConsumerGroupBuilder),
	)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			log.Printf("error running processor: %v", err)
		} else {
			log.Printf("Processor shutdown cleanly")
		}
	}()

	// SIGINT/SIGTERM 시그널을 기다림
	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait   // 시그널 수신 대기
	cancel() // 프로세서 정상 종료
	<-done
}

func main() {
	config := gokasdk.DefaultConfig()
	// SASL 인증 정보 설정 (유저명/비밀번호)
	config.Net.SASL.Enable = cfg.SASL.Enable
	config.Net.SASL.User = cfg.SASL.User
	config.Net.SASL.Password = cfg.SASL.Password
	config.Net.SASL.Mechanism = sarama.SASLMechanism(cfg.SASL.Mechanism)
	// TLS 활성화
	config.Net.TLS.Enable = cfg.TLS.Enable

	// 프로세서가 이미터보다 시작이 느릴 수 있으므로, 이 설정을 하지 않으면 첫 메시지를 소비하지 못할 수 있습니다.
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	gokasdk.ReplaceGlobalConfig(config)

	// 토픽 매니저 생성 및 스트림 토픽이 존재하는지 확인
	tm, err := gokasdk.NewTopicManager(brokers, config, tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()

	// 토픽이 없으면 생성 (이 작업이 반드시 끝나야 함)
	err = tm.EnsureStreamExists(string(topic), cfg.Partitions)
	if err != nil {
		log.Fatalf("Error creating kafka topic %s: %v", topic, err)
	}

	// 토픽이 확실히 존재한 후 Emitter/Processor 실행
	go runEmitter() // 메시지 발행 시작
	runProcessor()  // 프로세서 시작
}
