package worker
// PopularityWorker 负责消费视频热度更新相关的 RabbitMQ 消息，处理视频热度更新的逻辑，并更新 Redis 中的视频热度缓存
import (
	"context"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/middleware/rabbitmq"
	rediscache "feedsystem_video_go/internal/middleware/redis"
	"feedsystem_video_go/internal/video"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PopularityWorker struct {
	ch    *amqp.Channel
	cache *rediscache.Client
	queue string
}

func NewPopularityWorker(ch *amqp.Channel, cache *rediscache.Client, queue string) *PopularityWorker {
	return &PopularityWorker{ch: ch, cache: cache, queue: queue}
}

func (w *PopularityWorker) Run(ctx context.Context) error {
	if w == nil || w.ch == nil || w.cache == nil {
		return errors.New("popularity worker is not initialized")
	}
	if w.queue == "" {
		return errors.New("queue is required")
	}

	deliveries, err := w.ch.Consume(
		w.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("deliveries channel closed")
			}
			w.handleDelivery(ctx, d)
		}
	}
}
// handleDelivery 处理接收到的 RabbitMQ 消息
func (w *PopularityWorker) handleDelivery(ctx context.Context, d amqp.Delivery) {
	if err := w.process(ctx, d.Body); err != nil {
		log.Printf("popularity worker: failed to process message: %v", err)
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}
// process 处理 RabbitMQ 消息，进行对应视频的热度更新
func (w *PopularityWorker) process(ctx context.Context, body []byte) error {
	var evt rabbitmq.PopularityEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil
	}
	if evt.VideoID == 0 || evt.Change == 0 {
		return nil
	}
	video.UpdatePopularityCache(ctx, w.cache, evt.VideoID, evt.Change)
	return nil
}

