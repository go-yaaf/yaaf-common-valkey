package test

import (
	"fmt"

	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/messaging"
)

type HeroSubscriber struct {
	name       string
	topic      string
	messageBus messaging.IMessageBus
	error      error
}

// NewHeroPublisher is a factory method
func NewHeroSubscriber(bus messaging.IMessageBus) *HeroSubscriber {
	return &HeroSubscriber{messageBus: bus}
}

// Name configure message queue (topic) name
func (p *HeroSubscriber) Name(name string) *HeroSubscriber {
	p.name = name
	return p
}

// Topic configure message topic name
func (p *HeroSubscriber) Topic(topic string) *HeroSubscriber {
	p.topic = topic
	return p
}

// Start the publisher
func (p *HeroSubscriber) Start() {
	if sub, err := p.messageBus.Subscribe("hero_subscriber", NewHeroMessage, p.processMessage, p.topic); err != nil {
		logger.Error("could not subscribe to topic: %s: %s", p.topic, err.Error())
	} else {
		logger.Info("subscribe to topic: %s: %s", p.topic, sub)
	}
}

// This consumer just print the message to the console
func (p *HeroSubscriber) processMessage(message messaging.IMessage) bool {
	sm := message.(*HeroMessage)

	fmt.Println("process hero")
	logger.Debug("[%s] process hero: %s", p.name, sm.Hero.NAME())
	return true
}

// GetError return error
func (p *HeroSubscriber) GetError() error {
	return p.error
}
