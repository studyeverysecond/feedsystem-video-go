package rabbitmq

import (
	"context"
	"errors"
	"time"
)

type CommentMQ struct {
	*RabbitMQ
}

const (
	commentExchange   = "comment.events"
	commentQueue      = "comment.events"
	commentBindingKey = "comment.*"

	commentPublishRK = "comment.publish"
	commentDeleteRK  = "comment.delete"
)

type CommentEvent struct {
	EventID    string    `json:"event_id"`
	Action     string    `json:"action"`
	CommentID  uint      `json:"comment_id,omitempty"`
	Username   string    `json:"username,omitempty"`
	VideoID    uint      `json:"video_id,omitempty"`
	AuthorID   uint      `json:"author_id,omitempty"`
	Content    string    `json:"content,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewCommentMQ(base *RabbitMQ) (*CommentMQ, error) {
	if base == nil {
		return nil, errors.New("rabbitmq base is nil")
	}
	if err := base.DeclareTopic(commentExchange, commentQueue, commentBindingKey); err != nil {
		return nil, err
	}
	return &CommentMQ{RabbitMQ: base}, nil
}
// Publish 将一个新的评论事件发布到 RabbitMQ 中。它接受评论的相关信息，如用户名、视频 ID、作者 ID 和评论内容，并将这些信息封装成一个 CommentEvent 结构体，然后调用内部的 publish 方法将事件发送到 RabbitMQ。
func (c *CommentMQ) Publish(ctx context.Context, username string, videoID, authorID uint, content string) error {
	return c.publish(ctx, "publish", commentPublishRK, CommentEvent{
		Username: username,
		VideoID:  videoID,
		AuthorID: authorID,
		Content:  content,
	})
}

func (c *CommentMQ) Delete(ctx context.Context, commentID uint) error {
	return c.publish(ctx, "delete", commentDeleteRK, CommentEvent{
		CommentID: commentID,
	})
}
// publish 是一个内部方法，用于将 CommentEvent 发布到 RabbitMQ 中。它会生成一个唯一的事件 ID，并设置事件的发生时间。根据 action 参数，事件的内容会有所不同。
func (c *CommentMQ) publish(ctx context.Context, action, routingKey string, evt CommentEvent) error {
	if c == nil || c.RabbitMQ == nil {
		return errors.New("comment mq is not initialized")
	}
	id, err := newEventID(16)
	if err != nil {
		return err
	}
	evt.EventID = id
	evt.Action = action
	evt.OccurredAt = time.Now().UTC()
	return c.PublishJSON(ctx, commentExchange, routingKey, evt)
}

