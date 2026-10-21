package rabbitmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"feedsystem_video_go/internal/config"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
}

func NewRabbitMQ(cfg *config.RabbitMQConfig) (*RabbitMQ, error) {
	if cfg == nil {
		return nil, errors.New("rabbitmq config is nil")
	}
	url := "amqp://" + cfg.Username + ":" + cfg.Password + "@" + cfg.Host + ":" + strconv.Itoa(cfg.Port) + "/"
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return &RabbitMQ{Conn: conn, Ch: ch}, nil
}

func (r *RabbitMQ) Close() error {
	if r == nil || r.Ch == nil || r.Conn == nil {
		return nil
	}
	//分别关闭 RabbitMQ 的频道和连接，确保资源得到正确释放。如果在关闭过程中发生错误，会返回相应的错误信息。
	if err := r.Ch.Close(); err != nil {
		return err
	}
	if err := r.Conn.Close(); err != nil {
		return err
	}
	return nil
}
// DeclareTopic 是一个方法，用于在 RabbitMQ 中声明一个主题交换机、一个队列，并将它们绑定在一起。它接受交换机名称、队列名称和绑定键作为参数，并执行相应的 RabbitMQ 操作来完成这些任务。
func (r *RabbitMQ) DeclareTopic(exchange string, queue string, bindingKey string) error {
	if r == nil || r.Ch == nil {
		return errors.New("rabbitmq is not initialized")
	}
	if exchange == "" || queue == "" || bindingKey == "" {
		return errors.New("exchange/queue/bindingKey is required")
	}
	// ExchangeDeclare 用于声明一个交换机，参数包括交换机名称、类型（这里是 "topic"）、是否持久化、是否自动删除、是否内部使用以及其他可选参数。
	if err := r.Ch.ExchangeDeclare(
		exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	// QueueDeclare 用于声明一个队列，参数包括队列名称、是否持久化、是否自动删除、是否独占以及其他可选参数。它返回一个 amqp.Queue 对象，其中包含了队列的相关信息。
	q, err := r.Ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	// QueueBind 用于将队列绑定到交换机上，参数包括队列名称、绑定键、交换机名称以及其他可选参数。通过这个绑定，RabbitMQ 可以根据消息的路由键将消息正确地发送到对应的队列中。
	return r.Ch.QueueBind(
		q.Name,
		bindingKey,
		exchange,
		false,
		nil,
	)
}
//将一个消息发布到 RabbitMQ 中。它接受上下文、交换机名称、路由键和消息负载作为参数，并将消息以 JSON 格式发布到指定的交换机和路由键上。发布的消息包含内容类型、持久化设置、时间戳和消息体等信息。
func (r *RabbitMQ) PublishJSON(ctx context.Context, exchange string, routingKey string, payload any) error {
	if r == nil || r.Ch == nil {
		return errors.New("rabbitmq is not initialized")
	}
	if exchange == "" || routingKey == "" {
		return errors.New("exchange and routingKey are required")
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return r.Ch.PublishWithContext(ctx, exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Body:         b,
	})
}

func newEventID(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
