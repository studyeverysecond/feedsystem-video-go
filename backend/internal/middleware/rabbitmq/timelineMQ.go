package rabbitmq

import (
	"context"
	"errors"
	"time"
)
// TimelineMQ 是一个用于处理视频时间线相关事件的 RabbitMQ 客户端。它提供了将新视频发布事件发送到 RabbitMQ 的功能。
type TimelineMQ struct {
	*RabbitMQ
}

const (
	timelineExchange   = "video.timeline.events"
	timelineQueue      = "video.timeline.update.queue"
	timelineBindingKey = "video.timeline.*"
	timelinePublishRK  = "video.timeline.publish"
)

type TimelineEvent struct {
	EventID    string    `json:"event_id"`
	VideoID    uint      `json:"video_id"`
	CreateTime int64     `json:"create_time"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewTimelineMQ(base *RabbitMQ) (*TimelineMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareTopic(timelineExchange, timelineQueue, timelineBindingKey); err != nil {
		return nil, err
	}
	return &TimelineMQ{RabbitMQ: base}, nil
}
// PublishVideo 将一个新的视频发布事件发送到 RabbitMQ 中。它接受视频 ID 和创建时间，并将这些信息封装成一个 TimelineEvent 结构体，然后调用 PublishJSON 方法将事件发送到 RabbitMQ。
func (t *TimelineMQ) PublishVideo(ctx context.Context, videoID uint, createTime time.Time) error {
	if t == nil || t.RabbitMQ == nil {
		return errors.New("timeline mq is not initialized")
	}
	if videoID == 0 {
		return errors.New("videoID are required")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	timeline := TimelineEvent{
		EventID:    id,
		VideoID:    videoID,
		CreateTime: createTime.UnixMilli(),
		OccurredAt: time.Now(),
	}
	return t.PublishJSON(ctx, timelineExchange, timelinePublishRK, timeline)
}
