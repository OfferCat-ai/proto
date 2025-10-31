package events

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// KafkaProducer 全局 Kafka 生产者（由外部服务初始化）
var KafkaProducer sarama.SyncProducer

// PublishBalanceChange 发布余额变更事件到 Kafka（使用 Protobuf）
// 这是共用的发布函数，所有服务都可以使用
func PublishBalanceChange(topic string, event *BalanceChangeEvent) error {
	if KafkaProducer == nil {
		log.Printf("[Warning] Kafka producer is not initialized, skipping balance change event")
		return fmt.Errorf("kafka producer not initialized")
	}

	// 如果没有设置时间戳，设置当前时间
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixMilli()
	}

	// 如果没有设置 event_id，自动生成
	if event.EventId == "" {
		event.EventId = uuid.New().String()
	}

	// 使用 Protobuf 序列化
	eventBytes, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal balance change event: %v", err)
	}

	// 创建 Kafka 消息
	// 使用用户ID作为分区键，确保同一用户的消息有序
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(fmt.Sprintf("%d", event.UserId)),
		Value:     sarama.ByteEncoder(eventBytes),
		Timestamp: time.UnixMilli(event.Timestamp),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("event_type"),
				Value: []byte("balance_change"),
			},
			{
				Key:   []byte("user_id"),
				Value: []byte(fmt.Sprintf("%d", event.UserId)),
			},
			{
				Key:   []byte("source_service"),
				Value: []byte(event.SourceService),
			},
		},
	}

	// 发送消息
	partition, offset, err := KafkaProducer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message to kafka: %v", err)
	}

	log.Printf("Balance change event published: topic=%s, partition=%d, offset=%d, event_id=%s, user_id=%d, amount=%d, source=%s",
		topic, partition, offset, event.EventId, event.UserId, event.Amount, event.SourceService)
	return nil
}

// NewBalanceChangeEvent 创建余额变更事件
func NewBalanceChangeEvent(userId int64, changeType string, amount, balance int64, orderNo, reason string, sourceService string) *BalanceChangeEvent {
	// 如果来源服务为空，使用默认值
	if sourceService == "" {
		sourceService = "unknown-service"
	}

	return &BalanceChangeEvent{
		UserId:        userId,
		ChangeType:    changeType,
		Amount:        amount,
		Balance:       balance,
		OrderNo:       orderNo,
		Reason:        reason,
		Timestamp:     time.Now().UnixMilli(),
		SourceService: sourceService,
		EventId:       uuid.New().String(),
	}
}

// SetProducer 设置 Kafka 生产者（由各个服务在初始化时调用）
func SetProducer(producer sarama.SyncProducer) {
	KafkaProducer = producer
}
