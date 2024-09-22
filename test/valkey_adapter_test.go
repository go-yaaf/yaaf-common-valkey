// Integration tests of Redis data cache implementations
//

package test

import (
	"fmt"
	"testing"

	"github.com/go-yaaf/yaaf-common-valkey/valkey"
)

func TestValkeyAdapterKey(t *testing.T) {
	skipCI(t)

	uri := fmt.Sprintf("valkey://localhost:%s", "6379")
	sut, err := facilities.NewValkeyDataCache(uri)
	if err != nil {
		panic(any(err))
	}

	if err = sut.Ping(5, 5); err != nil {
		fmt.Println("error pinging database")
		panic(any(err))
	}

	// Initialize database
	for _, hero := range list_of_heroes {
		if er := sut.Set(fmt.Sprintf("%s:%s", hero.TABLE(), hero.ID()), hero); er != nil {
			t.Errorf("error: %s", er.Error())
		}
	}

	// wait here
	fmt.Println("done")
}
