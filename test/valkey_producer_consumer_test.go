//
// Integration tests of Valkey consumer - producer
//

package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-yaaf/yaaf-common-valkey/valkey"
	"github.com/go-yaaf/yaaf-common/messaging"
)

// This test is used to run message producer and message consumer
// You must run the docker-compose.yml to run Valkey instance in order to run these tests

var ValkeyURI = "valkey://localhost:6379"
var ValkeyTopic = "hero"

func TestValkeyProducer(t *testing.T) {
	skipCI(t)

	cli, err := facilities.NewValkeyMessageBus(ValkeyURI)
	if err != nil {
		panic(any(err))
	}

	if er := cli.Ping(5, 5); er != nil {
		fmt.Println("error pinging database")
		panic(any(err))
	}

	prod, er2 := cli.CreateProducer(ValkeyTopic)
	if er2 != nil {
		fmt.Println("error creating producer")
		panic(any(err))
	}

	// Run in loop and produce heroes every 100 milli
	count := 0
	for {
		count++

		msg := GetRandomHeroMessage(ValkeyTopic)
		fmt.Printf("%d: creating hero: %s [%s] \n", count, msg.Addressee(), msg.SessionId())
		if er := prod.Publish(msg); er != nil {
			panic(any(er))
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func TestValkeyConsumer(t *testing.T) {
	skipCI(t)

	cli, err := facilities.NewValkeyMessageBus(ValkeyURI)
	if err != nil {
		panic(any(err))
	}

	if er := cli.Ping(5, 5); er != nil {
		fmt.Println("error pinging database")
		panic(any(err))
	}

	cons, er2 := cli.CreateConsumer("subscription", NewHeroMessage, ValkeyTopic)
	if er2 != nil {
		fmt.Println("error creating consumer")
		panic(any(err))
	}

	// Run in loop and produce heroes every 100 milli
	count := 0

	for {
		count++

		if msg, er3 := cons.Read(time.Hour); er3 != nil {
			fmt.Printf("error reading message: %s \n", er3.Error())
			panic(any(er3))
		} else {
			fmt.Printf("%d: CONSUME hero: %s [%s]\n", count, msg.Addressee(), msg.SessionId())
		}
	}
}

func TestValkeyProducerConsumer(t *testing.T) {
	skipCI(t)

	cli, err := facilities.NewValkeyMessageBus(ValkeyURI)
	if err != nil {
		panic(any(err))
	}

	if er := cli.Ping(5, 5); er != nil {
		fmt.Println("error pinging database")
		panic(any(err))
	}

	prod, er2 := cli.CreateProducer(ValkeyTopic)
	if er2 != nil {
		fmt.Println("error creating producer")
		panic(any(err))
	}

	// Listen to topic
	cb := func(msg messaging.IMessage) bool {
		fmt.Printf("CONSUME hero: %s [%s]\n", msg.Addressee(), msg.SessionId())
		return true
	}

	if sub, er := cli.Subscribe("subscriber", NewHeroMessage, cb, ValkeyTopic); er != nil {
		panic(any(er))
	} else {
		fmt.Println("Subscribe to topic returns", sub)
	}

	// Run in loop and produce heroes every 100 milli
	count := 0
	for {
		count++

		msg := GetRandomHeroMessage(ValkeyTopic)
		fmt.Printf("%d: creating hero: %s [%s] \n", count, msg.Addressee(), msg.SessionId())
		if er := prod.Publish(msg); er != nil {
			panic(any(er))
		}
		time.Sleep(time.Millisecond * 500)
	}
}
