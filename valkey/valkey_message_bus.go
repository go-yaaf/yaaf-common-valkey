// Structure definitions and factory method for redis implementation of IDataCache and IMessageBus
//

package facilities

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strings"
	"time"

	. "github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/logger"
	. "github.com/go-yaaf/yaaf-common/messaging"
)

// region Message Bus actions ------------------------------------------------------------------------------------------

// Publish messages to a channel (topic)
func (r *ValkeyAdapter) Publish(messages ...IMessage) error {
	for _, message := range messages {
		if bytes, err := messageToRaw(message); err != nil {
			return err
		} else {
			if res := r.rc.Publish(r.ctx, message.Topic(), bytes); res.Err() != nil {
				return res.Err()
			}
		}
	}
	return nil
}

// Subscribe on topics
func (r *ValkeyAdapter) Subscribe(subscriberName string, factory MessageFactory, callback SubscriptionCallback, topics ...string) (string, error) {

	topicArray := make([]string, 0)

	// Check if topics include * - in this case it should be patterned subscribe
	isPattern := false
	for _, t := range topics {
		if strings.Contains(t, "*") {
			isPattern = true
		}
		topicArray = append(topicArray, t)
	}

	var ps *redis.PubSub

	if isPattern {
		ps = r.rc.PSubscribe(r.ctx, topics...)
	} else {
		ps = r.rc.Subscribe(r.ctx, topics...)
	}

	subscriptionId := NanoID()

	r.Lock()
	defer r.Unlock()
	r.subs[subscriptionId] = subscriber{ps: ps, topics: topicArray}
	go r.subscriber(ps, callback, factory)
	return subscriptionId, nil
}

// subscriber is a function running infinite loop to get messages from channel
func (r *RedisAdapter) subscriber(ps *redis.PubSub, callback SubscriptionCallback, factory MessageFactory) {

LOOP:
	for {
		select {
		case m := <-ps.Channel():
			if m == nil {
				break LOOP
			}
			message := factory()
			if err := Unmarshal([]byte(m.Payload), &message); err != nil {
				continue
			} else {
				go callback(message)
			}
		}
	}
}

// Unsubscribe with the given subscriber id
func (r *ValkeyAdapter) Unsubscribe(subscriptionId string) bool {
	r.Lock()
	defer r.Unlock()

	if v, ok := r.subs[subscriptionId]; !ok {
		return false
	} else {
		if err := v.ps.Unsubscribe(r.ctx, v.topics...); err != nil {
			logger.Warn("Unsubscribe error unsubscribe: %s\n", err.Error())
		}
		if err := v.ps.Close(); err != nil {
			logger.Warn("Unsubscribe error closing PubSub: %s\n", err.Error())
		}
		delete(r.subs, subscriptionId)
		return true
	}
}

// Push Append one or multiple messages to a queue
func (r *ValkeyAdapter) Push(messages ...IMessage) error {
	for _, message := range messages {
		if bytes, err := messageToRaw(message); err != nil {
			return err
		} else {
			if er := r.rc.LPush(r.ctx, message.Topic(), bytes).Err(); er != nil {
				return er
			}
		}
	}
	return nil
}

// Pop Remove and get the last message in a queue or block until timeout expires
func (r *ValkeyAdapter) Pop(factory MessageFactory, timeout time.Duration, queue ...string) (IMessage, error) {

	message := factory()

	if len(queue) == 0 {
		queue = append(queue, message.Topic())
	}

	if timeout == 0 {
		if cmd := r.rc.RPop(r.ctx, queue[0]); cmd.Err() != nil {
			return nil, cmd.Err()
		} else {
			if bytes, er := cmd.Bytes(); er != nil {
				return nil, er
			} else {
				return rawToMessage(factory, bytes)
			}
		}
	} else {
		if cmd := r.rc.BRPop(r.ctx, timeout, queue...); cmd.Err() != nil {
			return nil, cmd.Err()
		} else {
			if result, err := cmd.Result(); err != nil {
				return nil, err
			} else {
				return rawToMessage(factory, []byte(result[1]))
			}
		}
	}
}

// CreateProducer creates message producer for specific topic
func (r *ValkeyAdapter) CreateProducer(topic string) (IMessageProducer, error) {
	return &producer{
		rc:    r.rc,
		topic: topic,
	}, nil
}

// CreateConsumer creates message consumer for a specific topic
func (r *ValkeyAdapter) CreateConsumer(subscription string, mf MessageFactory, topics ...string) (IMessageConsumer, error) {

	topicArray := make([]string, 0)

	// Check if topics include * - in this case it should be patterned subscribe
	isPattern := false
	for _, t := range topics {
		if strings.Contains(t, "*") {
			isPattern = true
		}
		topicArray = append(topicArray, t)
	}

	var ps *redis.PubSub

	if isPattern {
		ps = r.rc.PSubscribe(r.ctx, topics...)
	} else {
		ps = r.rc.Subscribe(r.ctx, topics...)
	}

	return &consumer{
		ps:        ps,
		factory:   mf,
		isPattern: isPattern,
		topics:    topicArray,
	}, nil
}

// endregion

// region Producer actions ---------------------------------------------------------------------------------------------

type producer struct {
	rc    *redis.Client
	topic string
}

// Close cache and free resources
func (p *producer) Close() error {
	return nil
}

// Publish messages to a channel (topic)
func (p *producer) Publish(messages ...IMessage) error {
	for _, message := range messages {
		if bytes, err := messageToRaw(message); err != nil {
			return err
		} else {
			if res := p.rc.Publish(context.Background(), message.Topic(), bytes); res.Err() != nil {
				return res.Err()
			}
		}
	}
	return nil
}

// endregion

// region Consumer methods  --------------------------------------------------------------------------------------------

type consumer struct {
	ps        *redis.PubSub
	factory   MessageFactory
	isPattern bool
	topics    []string
}

// Close cache and free resources
func (p *consumer) Close() error {

	if p.ps == nil {
		return nil
	}

	if p.isPattern {
		return p.ps.PUnsubscribe(context.Background(), p.topics...)
	} else {
		return p.ps.Unsubscribe(context.Background(), p.topics...)
	}
}

// Read message from topic, blocks until a new message arrive or until timeout expires
// Use 0 instead of time.Duration for unlimited time
// The standard way to use Read is by using infinite loop:
//
//	for {
//		if msg, err := consumer.Read(time.Second * 5); err != nil {
//			// Handle error
//		} else {
//			// Process message in a dedicated go routine
//			go processTisMessage(msg)
//		}
//	}
func (p *consumer) Read(timeout time.Duration) (IMessage, error) {

	if timeout == 0 {
		timeout = time.Hour * 24
	}

LOOP:
	for {
		select {
		case m := <-p.ps.Channel():
			if m == nil {
				break LOOP
			}
			message := p.factory()
			if err := Unmarshal([]byte(m.Payload), &message); err != nil {
				return nil, err
			} else {
				return message, nil
			}
		case <-time.After(timeout):
			return nil, fmt.Errorf("read timeout")
		}
	}
	return nil, fmt.Errorf("read timeout")
}

// endregion
