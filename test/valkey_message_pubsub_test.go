//
// Integration tests of Valkey data cache implementations
//

package test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-yaaf/yaaf-common-valkey/valkey"
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/messaging"
	"github.com/go-yaaf/yaaf-common/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// ValkeyPubSubTestSuite creates a Valkey container with data for a suite of message queue tests and release it when done
type ValkeyPubSubTestSuite struct {
	suite.Suite
	containerID string
	mq          messaging.IMessageBus
}

func TestValkeyPubSubTestSuite(t *testing.T) {
	skipCI(t)
	suite.Run(t, new(ValkeyPubSubTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *ValkeyPubSubTestSuite) SetupSuite() {

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
func (s *ValkeyPubSubTestSuite) TearDownSuite() {
	err := utils.DockerUtils().StopContainer(containerName)
	assert.Nil(s.T(), err)
}

// createSUT creates the system-under-test which is postgresql implementation of IDatabase
func (s *ValkeyPubSubTestSuite) createSUT() messaging.IMessageBus {

	uri := fmt.Sprintf("valkey://localhost:%s", dbPort)
	sut, err := facilities.NewValkeyMessageBus(uri)
	if err != nil {
		panic(any(err))
	}

	if err := sut.Ping(5, 5); err != nil {
		fmt.Println("error pinging database")
		panic(any(err))
	}

	return sut
}

func (s *ValkeyPubSubTestSuite) TestValkeyMessageBus_PubSub() {

	// Sync all publishers and consumers
	wg := &sync.WaitGroup{}
	wg.Add(2)

	// Create and run hero subscriber
	NewHeroSubscriber(s.mq).Name("logger").Topic("hero").Start()

	// Create status message publisher
	NewHeroPublisher(s.mq).Name("publisher").Topic("hero").Duration(time.Minute).Interval(time.Millisecond * 500).Start(wg)

	wg.Wait()
	logger.Info("Done")
}
