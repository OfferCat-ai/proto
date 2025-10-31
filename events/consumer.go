package events

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

// BalanceChangeHandler 余额变更事件处理函数类型
type BalanceChangeHandler func(event *BalanceChangeEvent) error

// BalanceChangeConsumerGroup 余额变更消费者组（共用实现）
type BalanceChangeConsumerGroup struct {
	ready          chan bool
	currentService string
	handler        BalanceChangeHandler
}

// NewBalanceChangeConsumerGroup 创建余额变更消费者组
func NewBalanceChangeConsumerGroup(currentService string, handler BalanceChangeHandler) *BalanceChangeConsumerGroup {
	return &BalanceChangeConsumerGroup{
		ready:          make(chan bool),
		currentService: currentService,
		handler:        handler,
	}
}

// Setup 在新的会话开始时运行，在 ConsumeClaim 之前
func (group *BalanceChangeConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	close(group.ready)
	return nil
}

// Cleanup 在会话结束时运行，当所有 ConsumeClaim goroutines 退出时
func (group *BalanceChangeConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 必须启动一个 goroutine 来处理会话的消费
func (group *BalanceChangeConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// 使用 Protobuf 反序列化
			var event BalanceChangeEvent
			if err := proto.Unmarshal(message.Value, &event); err != nil {
				log.Printf("Failed to unmarshal balance change event: %v", err)
				session.MarkMessage(message, "")
				continue
			}

			// 处理事件
			log.Printf("[Service %s] Received balance change: event_id=%s, user_id=%d, type=%s, amount=%d, balance=%d, source=%s",
				group.currentService, event.EventId, event.UserId, event.ChangeType, event.Amount, event.Balance, event.SourceService)

			// 调用处理函数
			if group.handler != nil {
				if err := group.handler(&event); err != nil {
					log.Printf("Error handling balance change event %s: %v", event.EventId, err)
					// 注意：这里可以选择重试或者发送到死信队列
					// 目前只是记录错误，可以根据业务需求调整
				}
			}

			// 标记消息已处理
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// StartBalanceChangeConsumer 启动余额变更消费者（共用函数）
func StartBalanceChangeConsumer(ctx context.Context, client sarama.Client, topic, groupID, currentService string, handler BalanceChangeHandler) error {
	if client == nil {
		return fmt.Errorf("kafka client not initialized")
	}

	// 创建消费者组
	consumer, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %v", err)
	}

	handlerGroup := NewBalanceChangeConsumerGroup(currentService, handler)

	// 在 goroutine 中运行消费者
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			// Consume 应该在循环中调用，当服务器端重平衡时，需要重新创建会话
			if err := consumer.Consume(ctx, []string{topic}, handlerGroup); err != nil {
				log.Printf("Error from consumer: %v", err)
				return
			}
			// 检查上下文是否被取消
			if ctx.Err() != nil {
				return
			}
			handlerGroup.ready = make(chan bool)
		}
	}()

	// 等待消费者准备就绪
	<-handlerGroup.ready
	log.Printf("Balance change consumer started: topic=%s, group=%s, service=%s", topic, groupID, currentService)

	// 等待上下文取消
	<-ctx.Done()
	log.Println("Balance change consumer stopping...")

	wg.Wait()
	if err = consumer.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}

	return nil
}

// DefaultBalanceChangeHandler 默认的余额变更事件处理函数（可以作为示例）
func DefaultBalanceChangeHandler(event *BalanceChangeEvent) error {
	// 业务逻辑处理
	switch event.ChangeType {
	case "add":
		log.Printf("User %d recharged %d, new balance: %d", event.UserId, event.Amount, event.Balance)
	case "deduct":
		log.Printf("User %d deducted %d, new balance: %d", event.UserId, event.Amount, event.Balance)
	case "refund":
		log.Printf("User %d refunded %d, new balance: %d", event.UserId, event.Amount, event.Balance)
	default:
		log.Printf("Unknown change type: %s", event.ChangeType)
	}

	// TODO: 在这里添加你的业务逻辑
	// 例如：
	// - 保存到数据库
	// - 发送 WebSocket 通知
	// - 更新 Redis 缓存
	// - 触发其他服务调用

	return nil
}
