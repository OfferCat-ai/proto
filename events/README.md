# Balance Change Events

余额变更事件的 Protobuf 定义和共用实现。

## 特性

- **Protobuf 定义**: 类型安全的消息格式
- **共用发布函数**: `PublishBalanceChange` 可在所有服务中使用
- **共用消费者**: `StartBalanceChangeConsumer` 提供统一的消费者实现
- **事件去重**: 自动生成 UUID 作为 event_id

## 使用方式

### 1. 在服务中初始化 Kafka Producer

```go
import (
    "github.com/IBM/sarama"
    "github.com/OfferCat-ai/proto/events"
)

// 初始化 Kafka producer
producer, _ := sarama.NewSyncProducer(brokers, config)

// 设置到 events 包中
events.SetProducer(producer)
```

### 2. 发布余额变更事件

```go
import "github.com/OfferCat-ai/proto/events"

// 创建事件
event := events.NewBalanceChangeEvent(
    userId,
    "add",        // change_type: "add", "deduct", "refund"
    amount,
    balance,
    orderNo,
    "payment_success",
    "payment-service", // source_service
)

// 发布到 Kafka
err := events.PublishBalanceChange("balance-change-events", event)
```

### 3. 消费余额变更事件

```go
import (
    "context"
    "github.com/IBM/sarama"
    "github.com/OfferCat-ai/proto/events"
)

// 定义事件处理函数
handler := func(event *events.BalanceChangeEvent) error {
    // 处理业务逻辑
    switch event.ChangeType {
    case "add":
        // 处理充值
    case "deduct":
        // 处理扣费
    case "refund":
        // 处理退款
    }
    return nil
}

// 启动消费者
ctx := context.Background()
err := events.StartBalanceChangeConsumer(
    ctx,
    kafkaClient,
    "balance-change-events", // topic
    "balance-consumer-group", // consumer group id
    "my-service",             // current service name
    handler,                  // event handler
)
```

## Protobuf 消息定义

```protobuf
message BalanceChangeEvent {
  int64 user_id = 1;          // 用户ID
  string change_type = 2;     // 变更类型: "add", "deduct", "refund"
  int64 amount = 3;           // 变更金额（分）
  int64 balance = 4;          // 变更后余额（分）
  string order_no = 5;        // 关联订单号（可选）
  string reason = 6;          // 变更原因（可选）
  int64 timestamp = 7;        // 时间戳（Unix 时间戳，毫秒）
  string source_service = 8;  // 来源服务名称
  string event_id = 9;        // 事件ID（UUID，用于去重和追踪）
}
```

## 函数说明

### PublishBalanceChange

发布余额变更事件到 Kafka。

- 自动设置时间戳（如果未设置）
- 自动生成 event_id（如果未设置）
- 使用 user_id 作为分区键，确保同一用户的消息有序

### NewBalanceChangeEvent

创建余额变更事件的辅助函数。

参数：
- `userId`: 用户ID
- `changeType`: 变更类型（"add", "deduct", "refund"）
- `amount`: 变更金额（分）
- `balance`: 变更后余额（分）
- `orderNo`: 订单号（可选）
- `reason`: 变更原因（可选）
- `sourceService`: 来源服务名称

### StartBalanceChangeConsumer

启动余额变更消费者。

参数：
- `ctx`: 上下文
- `client`: Kafka 客户端
- `topic`: Topic 名称
- `groupID`: 消费者组ID
- `currentService`: 当前服务名称（用于日志）
- `handler`: 事件处理函数

### DefaultBalanceChangeHandler

默认的事件处理函数，可作为示例或测试使用。

## 注意事项

1. **Producer 初始化**: 在使用 `PublishBalanceChange` 前，必须先调用 `SetProducer` 设置生产者
2. **消费者组**: 同一消费者组内的多个实例会分摊消费任务
3. **事件去重**: 使用 `event_id` 字段进行去重，避免重复处理
4. **服务名称**: 建议使用统一的命名规范，如 `kebab-case`

