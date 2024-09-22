// Integration tests of Valkey data cache implementations
//

package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-yaaf/yaaf-common-valkey/valkey"
	"github.com/go-yaaf/yaaf-common/database"
	"github.com/go-yaaf/yaaf-common/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	dbName        = "test_db"
	dbPort        = "6379"
	containerName = "test-valkey"
)

// ValkeyCacheTestSuite creates a Valkey container with data for a suite of database tests and release it when done
type ValkeyCacheTestSuite struct {
	suite.Suite
	containerID string
	cache       database.IDataCache
}

func TestValkeyCacheTestSuite(t *testing.T) {
	skipCI(t)
	suite.Run(t, new(ValkeyCacheTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *ValkeyCacheTestSuite) SetupSuite() {

	// Create command to run postgresql container
	err := utils.DockerUtils().CreateContainer("valkey/valkey:8.0.0").
		Name(containerName).
		Port(dbPort, dbPort).
		Label("env", "test").
		Run()

	assert.Nil(s.T(), err)

	// Give it 5 seconds to warm up
	time.Sleep(5 * time.Second)

	// Create and initialize
	s.cache = s.createSUT()
}

// TearDownSuite will be run once at the end of the testing suite, after all tests have been run
func (s *ValkeyCacheTestSuite) TearDownSuite() {
	err := utils.DockerUtils().StopContainer(containerName)
	assert.Nil(s.T(), err)
}

// createSUT creates the system-under-test which is postgresql implementation of IDatabase
func (s *ValkeyCacheTestSuite) createSUT() database.IDataCache {

	uri := fmt.Sprintf("valkey://localhost:%s", dbPort)
	sut, err := facilities.NewValkeyDataCache(uri)
	if err != nil {
		panic(any(err))
	}

	if err := sut.Ping(5, 5); err != nil {
		fmt.Println("error pinging database")
		panic(any(err))
	}

	// Initialize sut
	for _, hero := range list_of_heroes {
		if er := sut.Set(fmt.Sprintf("%s:%s", hero.TABLE(), hero.ID()), hero); er != nil {
			s.T().Errorf("error: %s", er.Error())
		}
	}
	return sut
}

// TestDatabaseSet operation
func (s *ValkeyCacheTestSuite) TestDataCacheSet() {

	for _, item := range list_of_heroes {
		heroId := fmt.Sprintf("%s:%s", item.TABLE(), item.ID())

		// Set data in cache
		if err := s.cache.Set(heroId, item); err != nil {
			s.T().Errorf("error: %s", err.Error())
		}
	}
	hero := NewHero1("100", 100, "New Hero")
	heroId := fmt.Sprintf("%s:%s", hero.TABLE(), hero.ID())

	// Set data in cache
	if err := s.cache.Set(heroId, hero); err != nil {
		s.T().Errorf("error: %s", err.Error())
	}

	// Get data from cache
	if result, err := s.cache.Get(NewHero, heroId); err != nil {
		s.T().Errorf(err.Error())
	} else {
		fmt.Println(result.ID(), result.NAME())
		require.Equal(s.T(), hero.NAME(), result.NAME(), "not expected result")
	}
}

// TestDataCacheScan operation
func (s *ValkeyCacheTestSuite) TestDataCacheScan() {

	current := uint64(0)
	for {
		fmt.Println("get bulk of 10 entries ...")
		keys, cur, err := s.cache.Scan(current, "*", 100)
		if err != nil {
			s.T().Errorf("error: %s", err.Error())
		}
		for i, key := range keys {
			fmt.Println(i, key)
		}
		fmt.Println("-----------------")
		current = cur
		if cur == 0 {
			return
		} else {
			current = cur
		}
	}
}
