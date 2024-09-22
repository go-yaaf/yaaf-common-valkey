//
// Integration tests of Valkey data cache implementations
//

package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-yaaf/yaaf-common-valkey/valkey"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/go-yaaf/yaaf-common/messaging"
	"github.com/go-yaaf/yaaf-common/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// ValkeyQueueTestSuite creates a Valkey container with data for a suite of message queue tests and release it when done
type ValkeyQueueTestSuite struct {
	suite.Suite
	containerID string
	mq          messaging.IMessageBus
}

func TestValkeyQueueSuite(t *testing.T) {
	skipCI(t)
	suite.Run(t, new(ValkeyQueueTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *ValkeyQueueTestSuite) SetupSuite() {

	// Create command to run Valkey container
	err := utils.DockerUtils().CreateContainer("valkey/valkey:8.0.0").
		Name(containerName).
		Port(dbPort, dbPort).
		Label("env", "test").
		Run()

	assert.Nil(s.T(), err)

	// Give it 5 seconds to warm up
	time.Sleep(5 * time.Second)

	// Create and initialize
	s.mq = s.createSUT()
}

// TearDownSuite will be run once at the end of the testing suite, after all tests have been run
func (s *ValkeyQueueTestSuite) TearDownSuite() {
	err := utils.DockerUtils().StopContainer(containerName)
	assert.Nil(s.T(), err)
}

// createSUT creates the system-under-test which is postgresql implementation of IDatabase
func (s *ValkeyQueueTestSuite) createSUT() messaging.IMessageBus {

	uri := fmt.Sprintf("valkey://localhost:%s", dbPort)
	sut, err := facilities.NewValkeyMessageBus(uri)
	if err != nil {
		panic(any(err))
	}

	if err := sut.Ping(5, 5); err != nil {
		fmt.Println("error pinging database")
		panic(any(err))
	}

	// Push messages to 4 queues (queue_0, queue_1, queue_2, queue_3)
	for idx, hero := range list_of_heroes {
		queue := fmt.Sprintf("queue_%d", idx%4)
		_ = sut.Push(newHeroMessage(queue, hero.(*Hero)))
	}
	return sut
}

func (s *ValkeyQueueTestSuite) TestValkeyMessageBus_Pop() {

	for {
		if msg, err := s.mq.Pop(NewHeroMessage, 0, "queue_0"); err == nil {
			hero := msg.Payload().(*Hero)
			fmt.Println(msg.Topic(), hero.Id, hero.Name)
		} else {
			break
		}
	}
	fmt.Println("done")
}

func (s *ValkeyQueueTestSuite) TestValkeyMessageBus_PopWithTimeout() {

	// Push message to queue_y after 10 seconds
	go func() {
		time.Sleep(time.Second * 5)
		_ = s.mq.Push(newHeroMessage("queue_x", &Hero{
			BaseEntity: entity.BaseEntity{},
			Key:        100,
			Name:       "Delayed hero",
		}))
	}()

	if msg, err := s.mq.Pop(NewHeroMessage, time.Second*12, "queue_x", "queue_y", "queue_z"); err != nil {
		fmt.Println(err.Error())
	} else {
		hero := msg.Payload().(*Hero)
		fmt.Println(msg.Topic(), hero.Id, hero.Name)
	}

	fmt.Println("done")
}
