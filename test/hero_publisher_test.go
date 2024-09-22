package test

import (
	"math/rand"
	"sync"
	"time"

	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/messaging"
)

type HeroPublisher struct {
	name       string
	topic      string
	messageBus messaging.IMessageBus
	duration   time.Duration
	interval   time.Duration
	error      error
}

// NewHeroPublisher is a factory method
func NewHeroPublisher(bus messaging.IMessageBus) *HeroPublisher {
	return &HeroPublisher{messageBus: bus, name: "demo", topic: "topic", duration: time.Hour, interval: time.Second}
}

// Name configure message queue (topic) name
func (p *HeroPublisher) Name(name string) *HeroPublisher {
	p.name = name
	return p
}

// Topic configure message topic name
func (p *HeroPublisher) Topic(topic string) *HeroPublisher {
	p.topic = topic
	return p
}

// Duration configure for how long the publisher will run
func (p *HeroPublisher) Duration(duration time.Duration) *HeroPublisher {
	p.duration = duration
	return p
}

// Interval configure the time interval between messages
func (p *HeroPublisher) Interval(interval time.Duration) *HeroPublisher {
	p.interval = interval
	return p
}

// Start the publisher
func (p *HeroPublisher) Start(wg *sync.WaitGroup) {
	go p.run(wg, p.messageBus)
}

// GetError return error
func (p *HeroPublisher) GetError() error {
	return p.error
}

// Run starts the publisher
func (p *HeroPublisher) run(wg *sync.WaitGroup, mq messaging.IMessageBus) {

	rand.NewSource(time.Now().UnixNano())

	// Run publisher until timeout and push status message every time interval
	after := time.After(p.duration)
	for {
		select {
		case _ = <-time.Tick(p.interval):
			idx := rand.Intn(len(list_of_heroes))
			hero := list_of_heroes[idx].(*Hero)
			message := newHeroMessage(p.topic, hero)
			if err := mq.Publish(message); err != nil {
				logger.Error("error publishing message: %s", err.Error())
			} else {
				logger.Info("message: %s published", message.SessionId())
			}
		case <-after:
			if wg != nil {
				wg.Done()
			}
			return
		}
	}
}
