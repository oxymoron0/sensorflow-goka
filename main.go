package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/oxymoron0/sensorflow-goka/gokaManager"
)

var (
	cfg     gokaManager.AppConfig
	brokers []string
	topic   goka.Stream
	group   goka.Group
	tmc     *goka.TopicManagerConfig
	config  *sarama.Config
)

func init() {
	var err error
	cfg, err = gokaManager.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Config load error: %v", err)
	}
	brokers = cfg.Broker.Brokers
	topic = goka.Stream(cfg.Broker.Topic)
	group = goka.Group(cfg.Broker.Group)

	tmc = goka.NewTopicManagerConfig()
	tmc.Table.Replication = cfg.Broker.Replication.Table
	tmc.Stream.Replication = cfg.Broker.Replication.Stream

	// 전역 설정 초기화
	config, err = gokaManager.InitGlobalConfig(&cfg)
	if err != nil {
		log.Fatalf("Error initializing global config: %v", err)
	}
}

// runProcessor는 메시지를 처리하고 상태를 업데이트합니다. => Consumer
func runProcessor() {
	// 프로세스 콜백: "example-stream" 토픽에서 메시지가 전달될 때마다 호출됨
	cb := func(ctx goka.Context, msg interface{}) {
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
	g := goka.DefineGroup(group,
		goka.Input(topic, new(codec.String), cb), // 입력 스트림 및 콜백 정의
		goka.Persist(new(codec.Int64)),           // 그룹 테이블에 정수형 값 지속
	)

	// 새로운 프로세서 생성
	p, err := goka.NewProcessor(brokers,
		g,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(tmc)),
		goka.WithConsumerGroupBuilder(goka.DefaultConsumerGroupBuilder),
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
	// 토픽 매니저 생성 및 스트림 토픽이 존재하는지 확인
	tm, err := goka.NewTopicManager(brokers, config, tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	defer tm.Close()

	// 토픽이 없으면 생성 (이 작업이 반드시 끝나야 함)
	err = tm.EnsureStreamExists(string(topic), cfg.Broker.Partitions)
	if err != nil {
		log.Fatalf("Error creating kafka topic %s: %v", topic, err)
	}

	// Emitter Manager 초기화
	// emitterManager, err := gokaManager.GetEmitterManager()
	// if err != nil {
	// 	log.Fatalf("Error getting emitter manager: %v", err)
	// }

	// // Emitter 추가 및 가져오기
	// emitterManager.AddEmitter(string(topic))
	// emitter, err := emitterManager.GetEmitter(string(topic))
	// if err != nil {
	// 	log.Fatalf("Error getting emitter: %v", err)
	// }

	// go func() {
	// 	for i := 0; i < 10; i++ {
	// 		emitter.EmitSync(fmt.Sprintf("other-key-001-%d", i), fmt.Sprintf("some-value-%d from emitter manager", i))
	// 		fmt.Printf("Emitter %d\n", i)
	// 		time.Sleep(1 * time.Second)
	// 	}
	// }()

	// 토픽이 확실히 존재한 후 Emitter/Processor 실행
	runProcessor() // 프로세서 시작
}
