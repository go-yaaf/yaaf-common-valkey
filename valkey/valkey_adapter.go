// Valkey based implementation of IDataCache interface
//

package facilities

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
	. "github.com/go-yaaf/yaaf-common/messaging"
)

// region Data structure and methods  ----------------------------------------------------------------------------------

type subscriber struct {
	ps     *valkey.PubSubHooks
	topics []string
}

type ValkeyAdapter struct {
	rc   valkey.Client
	ctx  context.Context
	subs map[string]subscriber
	sync.RWMutex

	tmp   []byte
	tmpMu sync.Mutex
	uri   string
}

// NewValkeyDataCache factory method for Valkey IDataCache implementation
//
// param: URI - represents the Valkey connection string in the format of: valkey://user:password@host:port
// return: IDataCache instance, error
func NewValkeyDataCache(URI string) (dbs database.IDataCache, error error) {
	URI = strings.Replace(URI, "valkey://", "redis://", -1)
	if valkeyClient, err := getValkeyClient(URI); err != nil {
		return nil, err
	} else {
		return &ValkeyAdapter{
			rc:   valkeyClient,
			subs: make(map[string]subscriber),
			ctx:  context.Background(),
			uri:  URI,
		}, nil
	}
}

// NewValkeyMessageBus factory method for Valkey IMessageBus implementation
//
// param: URI - represents the Valkey connection string in the format of: valkey://user:password@host:port
// return: IDataCache instance, error
func NewValkeyMessageBus(URI string) (mq IMessageBus, error error) {

	URI = strings.Replace(URI, "valkey://", "redis://", -1)
	if valkeyClient, err := getValkeyClient(URI); err != nil {
		return nil, err
	} else {
		return &ValkeyAdapter{
			rc:   valkeyClient,
			subs: make(map[string]subscriber),
			ctx:  context.Background(),
		}, nil
	}
}

// Ping Test connectivity for retries number of time with time interval (in seconds) between retries
func (r *ValkeyAdapter) Ping(retries uint, intervalInSeconds uint) error {

	if r.rc == nil {
		return fmt.Errorf("valkey client not initialized")
	}

	cmd := r.rc.B().Ping().Build()

	for i := 0; i < int(retries); i++ {
		if res := r.rc.Do(context.Background(), cmd); res.Error() == nil {
			return nil
		}
		time.Sleep(time.Second * time.Duration(intervalInSeconds))
	}
	return fmt.Errorf("no connection")
}

// Close cache and free resources
func (r *ValkeyAdapter) Close() error {
	if r.rc != nil {
		r.rc.Close()
		return nil
	} else {
		return nil
	}
}

// CloneDataCache creates a clone of this instance
func (r *ValkeyAdapter) CloneDataCache() (dbs database.IDataCache, err error) {
	return NewValkeyDataCache(r.uri)
}

// CloneMessageBus creates a clone of this instance
func (r *ValkeyAdapter) CloneMessageBus() (dbs IMessageBus, err error) {
	return NewValkeyMessageBus(r.uri)
}

// endregion

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// Get native Valkey client and provide client name
func getValkeyClient(URI string) (valkey.Client, error) {

	if options, err := valkey.ParseURL(URI); err != nil {
		return nil, err
	} else {
		// Create Valkey client and set client name
		valkeyClient, er := valkey.NewClient(options)
		if er != nil {
			return nil, fmt.Errorf("can't create Valkey client: %s", er.Error())
		}
		if valkeyClient == nil {
			return nil, fmt.Errorf("valkey client is nil")
		}

		clientName := fmt.Sprintf("_:%d", os.Getegid())
		if path, er2 := os.Executable(); er2 == nil {
			clientName = fmt.Sprintf("%s:%d", filepath.Base(path), os.Getegid())
		}
		fmt.Println(clientName)

		cmd := valkeyClient.B().ClientSetname().ConnectionName(clientName).Build()
		_ = valkeyClient.Do(context.Background(), cmd)
		return valkeyClient, nil
	}
}

// convert raw data to entity
func rawToEntity(factory EntityFactory, bytes []byte) (Entity, error) {
	entity := factory()
	if err := Unmarshal(bytes, &entity); err != nil {
		return nil, err
	} else {
		return entity, nil
	}
}

// convert entity to raw data
func entityToRaw(entity Entity) ([]byte, error) {
	return Marshal(entity)
}

// convert raw data to message
func rawToMessage(factory MessageFactory, bytes []byte) (IMessage, error) {
	message := factory()
	if err := Unmarshal(bytes, &message); err != nil {
		return nil, err
	} else {
		return message, nil
	}
}

// convert message to raw data
func messageToRaw(message IMessage) ([]byte, error) {
	return Marshal(message)
}

// Check if the byte array representing a JSON string
func isJsonString(bytes []byte) bool {
	if len(bytes) < 2 {
		return false
	}
	if string(bytes[0:1]) == "{" && string(bytes[len(bytes)-1:]) == "}" {
		return true
	}
	if string(bytes[0:1]) == "[" && string(bytes[len(bytes)-1:]) == "]" {
		return true
	}
	return false
}

// endregion
