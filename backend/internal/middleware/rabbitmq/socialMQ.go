package rabbitmq

import (
	"context"
	"errors"
	"time"
)

type SocialMQ struct {
	*RabbitMQ
}

const (
	socialExchange   = "social.events"
	socialQueue      = "social.events"
	socialBindingKey = "social.*"

	socialFollowRK   = "social.follow"
	socialUnfollowRK = "social.unfollow"
)

type SocialEvent struct {
	EventID    string    `json:"event_id"`
	Action     string    `json:"action"`
	FollowerID uint      `json:"follower_id"`
	VloggerID  uint      `json:"vlogger_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewSocialMQ(base *RabbitMQ) (*SocialMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareTopic(socialExchange, socialQueue, socialBindingKey); err != nil {
		return nil, err
	}
	return &SocialMQ{RabbitMQ: base}, nil
}

func (s *SocialMQ) Follow(ctx context.Context, followerID, vloggerID uint) error {
	return s.publish(ctx, "follow", socialFollowRK, followerID, vloggerID)
}

func (s *SocialMQ) UnFollow(ctx context.Context, followerID, vloggerID uint) error {
	return s.publish(ctx, "unfollow", socialUnfollowRK, followerID, vloggerID)
}
// publish 是一个内部方法，用于将 SocialEvent 发布到 RabbitMQ 中。它会生成一个唯一的事件 ID，并设置事件的发生时间。根据 action 参数，事件的内容会有所不同。
func (s *SocialMQ) publish(ctx context.Context, action, routingKey string, followerID, vloggerID uint) error {
	if s == nil || s.RabbitMQ == nil {
		return errors.New("social mq is not initialized")
	}
	if followerID == 0 || vloggerID == 0 {
		return errors.New("followerID and vloggerID are required")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	evt := SocialEvent{
		EventID:    id,
		Action:     action,
		FollowerID: followerID,
		VloggerID:  vloggerID,
		OccurredAt: time.Now().UTC(),
	}
	return s.PublishJSON(ctx, socialExchange, routingKey, evt)
}
