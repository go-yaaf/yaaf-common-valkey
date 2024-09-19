// Structure definitions and factory method for Valkey implementation of IDataCache and IMessageBus
//

package facilities

import (
	"fmt"
	"time"

	. "github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
)

// region Key actions ----------------------------------------------------------------------------------------------

// GetRaw gets the value of a key in a byte array format
func (r *ValkeyAdapter) GetRaw(key string) ([]byte, error) {
	var bytes []byte
	cmd := r.rc.Get(r.ctx, key)
	if err := cmd.Err(); err != nil {
		return nil, err
	} else {
		if bytes, err = cmd.Bytes(); err != nil {
			return nil, err
		} else {
			return bytes, nil
		}
	}
}

// Get gets the value of a key as entity
func (r *ValkeyAdapter) Get(factory EntityFactory, key string) (Entity, error) {
	if bytes, err := r.GetRaw(key); err != nil {
		return nil, err
	} else {
		return rawToEntity(factory, bytes)
	}
}

// SetRaw sets value of key in a byte array format
func (r *ValkeyAdapter) SetRaw(key string, bytes []byte, expiration ...time.Duration) error {
	if len(expiration) > 0 {
		return r.rc.Set(r.ctx, key, bytes, expiration[0]).Err()
	} else {
		return r.rc.Set(r.ctx, key, bytes, 0).Err()
	}
}

// Set sets value of key with optional expiration
func (r *ValkeyAdapter) Set(key string, entity Entity, expiration ...time.Duration) error {
	if bytes, err := entityToRaw(entity); err != nil {
		return err
	} else {
		return r.SetRaw(key, bytes, expiration...)
	}
}

// SetNX sets value of key only if it is not exist with optional expiration, return false if the key exists
func (r *ValkeyAdapter) SetNX(key string, entity Entity, expiration ...time.Duration) (bool, error) {
	if bytes, err := entityToRaw(entity); err != nil {
		return false, err
	} else {
		return r.SetRawNX(key, bytes, expiration...)
	}
}

// SetRawNX sets bytes value of key only if it is not exist with optional expiration, return false if the key exists
func (r *ValkeyAdapter) SetRawNX(key string, bytes []byte, expiration ...time.Duration) (bool, error) {
	var exp time.Duration = 0
	if len(expiration) > 0 {
		exp = expiration[0]
	}
	return r.rc.SetNX(r.ctx, key, bytes, exp).Result()
}

// Del Delete keys
func (r *ValkeyAdapter) Del(keys ...string) error {
	return r.rc.Del(r.ctx, keys...).Err()
}

// GetKeys Get the value of all the given keys
func (r *ValkeyAdapter) GetKeys(factory EntityFactory, keys ...string) ([]Entity, error) {
	cmd := r.rc.MGet(r.ctx, keys...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	if list, err := cmd.Result(); err != nil {
		return nil, err
	} else {
		entities := make([]Entity, 0)
		for _, item := range list {
			if entity, err := rawToEntity(factory, item.([]byte)); err == nil {
				entities = append(entities, entity)
			}
		}
		return entities, nil
	}
}

// GetRawKeys Get the value of all the given keys
func (r *ValkeyAdapter) GetRawKeys(keys ...string) ([]Tuple[string, []byte], error) {
	cmd := r.rc.MGet(r.ctx, keys...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	if list, err := cmd.Result(); err != nil {
		return nil, err
	} else {
		tuples := make([]Tuple[string, []byte], 0)
		for i, item := range list {
			tuple := Tuple[string, []byte]{Key: fmt.Sprintf("%d", i), Value: item.([]byte)}
			tuples = append(tuples, tuple)
		}
		return tuples, nil
	}
}

// AddRaw Set the byte array value of a key only if the key does not exist
func (r *ValkeyAdapter) AddRaw(key string, bytes []byte, expiration time.Duration) (bool, error) {
	if cmd := r.rc.SetNX(r.ctx, key, bytes, expiration); cmd.Err() != nil {
		return false, cmd.Err()
	} else {
		return cmd.Result()
	}
}

// Add Set the value of a key only if the key does not exist
func (r *ValkeyAdapter) Add(key string, entity Entity, expiration time.Duration) (bool, error) {
	if bytes, err := entityToRaw(entity); err != nil {
		return false, err
	} else {
		return r.AddRaw(key, bytes, expiration)
	}
}

// Rename a key
func (r *ValkeyAdapter) Rename(key string, newKey string) error {
	return r.rc.Rename(r.ctx, key, newKey).Err()
}

// Scan keys from the provided cursor
func (r *ValkeyAdapter) Scan(from uint64, match string, count int64) (keys []string, cursor uint64, err error) {
	scanCmd := r.rc.Scan(r.ctx, from, match, count)
	if err = scanCmd.Err(); err != nil {
		return nil, 0, err
	} else {
		if list, cur, er := scanCmd.Result(); er != nil {
			return nil, 0, er
		} else {
			return list, cur, nil
		}
	}
}

// Exists Check if key exists
func (r *ValkeyAdapter) Exists(key string) (result bool, err error) {
	if cmd := r.rc.Exists(r.ctx, key); cmd.Err() != nil {
		return false, cmd.Err()
	} else {
		return cmd.Val() > 0, nil
	}
}

// endregion

// region Hash actions ---------------------------------------------------------------------------------------------

// HGet Get the value of a hash field
func (r *ValkeyAdapter) HGet(factory EntityFactory, key, field string) (Entity, error) {
	if bytes, err := r.HGetRaw(key, field); err != nil {
		return nil, err
	} else {
		return rawToEntity(factory, bytes)
	}
}

// HGetRaw gets the rae value of a hash field
func (r *ValkeyAdapter) HGetRaw(key, field string) ([]byte, error) {
	cmd := r.rc.HGet(r.ctx, key, field)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	} else {
		if bytes, err := cmd.Bytes(); err != nil {
			return nil, err
		} else {
			return bytes, nil
		}
	}
}

// HKeys Get all the fields in a hash
func (r *ValkeyAdapter) HKeys(key string) ([]string, error) {
	if cmd := r.rc.HKeys(r.ctx, key); cmd.Err() != nil {
		return nil, cmd.Err()
	} else {
		return cmd.Val(), nil
	}
}

// HGetAll Get all the fields and values in a hash
func (r *ValkeyAdapter) HGetAll(factory EntityFactory, key string) (map[string]Entity, error) {
	if cmd := r.rc.HGetAll(r.ctx, key); cmd.Err() != nil {
		return nil, cmd.Err()
	} else {
		result := make(map[string]Entity)
		for k, str := range cmd.Val() {
			if entity, er := rawToEntity(factory, []byte(str)); er == nil {
				result[k] = entity
			}
		}
		return result, nil
	}
}

// HGetRawAll gets all the fields and raw values in a hash
func (r *ValkeyAdapter) HGetRawAll(key string) (map[string][]byte, error) {
	if cmd := r.rc.HGetAll(r.ctx, key); cmd.Err() != nil {
		return nil, cmd.Err()
	} else {
		result := make(map[string][]byte)
		for k, str := range cmd.Val() {
			result[k] = []byte(str)
		}
		return result, nil
	}
}

// HSet Set the value of a hash field
func (r *ValkeyAdapter) HSet(key, field string, entity Entity) error {
	if bytes, err := entityToRaw(entity); err != nil {
		return err
	} else {
		return r.rc.HSet(r.ctx, key, field, bytes).Err()
	}
}

// HSetRaw sets the raw value of a hash field
func (r *ValkeyAdapter) HSetRaw(key, field string, bytes []byte) error {
	return r.rc.HSet(r.ctx, key, field, bytes).Err()
}

// HSetNX Set value of key only if it is not exist with optional expiration, return false if the key exists
func (r *ValkeyAdapter) HSetNX(key string, field string, entity Entity) (bool, error) {
	if bytes, err := entityToRaw(entity); err != nil {
		return false, err
	} else {
		return r.rc.HSetNX(r.ctx, key, field, bytes).Result()
	}
}

// HSetRawNX sets the raw value of key only if it is not exist with optional expiration, return false if the key exists
func (r *ValkeyAdapter) HSetRawNX(key string, field string, bytes []byte) (bool, error) {
	return r.rc.HSetNX(r.ctx, key, field, bytes).Result()
}

// HDel Delete one or more hash fields
func (r *ValkeyAdapter) HDel(key string, fields ...string) error {
	return r.rc.HDel(r.ctx, key, fields...).Err()
}

// HAdd sets the value of a key only if the key does not exist
func (r *ValkeyAdapter) HAdd(key, field string, entity Entity) (bool, error) {
	if bytes, err := entityToRaw(entity); err != nil {
		return false, err
	} else {
		if err = r.rc.HSetNX(r.ctx, key, field, bytes).Err(); err != nil {
			return false, err
		} else {
			return true, nil
		}
	}
}

// HAddRaw sets the raw value of a key only if the key does not exist
func (r *ValkeyAdapter) HAddRaw(key, field string, bytes []byte) (bool, error) {
	if err := r.rc.HSetNX(r.ctx, key, field, bytes).Err(); err != nil {
		return false, err
	} else {
		return true, nil
	}
}

// HExists Check if key exists
func (r *ValkeyAdapter) HExists(key, field string) (bool, error) {
	if cmd := r.rc.HExists(r.ctx, key, field); cmd.Err() != nil {
		return false, cmd.Err()
	} else {
		return cmd.Val(), nil
	}
}

// endregion

// region List actions ---------------------------------------------------------------------------------------------

// RPush Append one or multiple values to a list
func (r *ValkeyAdapter) RPush(key string, value ...Entity) error {
	values := make([]interface{}, 0)
	for _, v := range value {
		values = append(values, v)
	}
	return r.rc.RPush(r.ctx, key, values...).Err()
}

// LPush Prepend one or multiple values to a list
func (r *ValkeyAdapter) LPush(key string, value ...Entity) error {
	values := make([]interface{}, 0)
	for _, v := range value {
		values = append(values, v)
	}
	return r.rc.LPush(r.ctx, key, values...).Err()
}

// RPop Remove and get the last element in a list
func (r *ValkeyAdapter) RPop(factory EntityFactory, key string) (Entity, error) {
	if cmd := r.rc.RPop(r.ctx, key); cmd.Err() != nil {
		return nil, cmd.Err()
	} else {
		if bytes, err := cmd.Bytes(); err != nil {
			return nil, err
		} else {
			return rawToEntity(factory, bytes)
		}
	}
}

// LPop Remove and get the first element in a list
func (r *ValkeyAdapter) LPop(factory EntityFactory, key string) (entity Entity, err error) {
	if cmd := r.rc.LPop(r.ctx, key); cmd.Err() != nil {
		return nil, cmd.Err()
	} else {
		if bytes, er := cmd.Bytes(); er != nil {
			return nil, er
		} else {
			return rawToEntity(factory, bytes)
		}
	}
}

// BRPop Remove and get the last element in a list or block until one is available
func (r *ValkeyAdapter) BRPop(factory EntityFactory, timeout time.Duration, keys ...string) (key string, entity Entity, err error) {
	if cmd := r.rc.BRPop(r.ctx, timeout, keys...); cmd.Err() != nil {
		return "", nil, err
	} else {
		if result, er := cmd.Result(); er != nil {
			return "", nil, err
		} else {
			key = result[0]
			entity, err = rawToEntity(factory, []byte(result[1]))
			return
		}
	}
}

// BLPop Remove and get the first element in a list or block until one is available
func (r *ValkeyAdapter) BLPop(factory EntityFactory, timeout time.Duration, keys ...string) (key string, entity Entity, err error) {
	if cmd := r.rc.BLPop(r.ctx, timeout, keys...); cmd.Err() != nil {
		return "", nil, err
	} else {
		if result, er := cmd.Result(); er != nil {
			return "", nil, err
		} else {
			key = result[0]
			entity, err = rawToEntity(factory, []byte(result[1]))
			return
		}
	}
}

// LRange Get a range of elements from list
func (r *ValkeyAdapter) LRange(factory EntityFactory, key string, start, stop int64) ([]Entity, error) {
	if list, err := r.rc.LRange(r.ctx, key, start, stop).Result(); err != nil {
		return nil, err
	} else {
		result := make([]Entity, 0)
		for _, str := range list {
			if entity, err := rawToEntity(factory, []byte(str)); err == nil {
				result = append(result, entity)
			}
		}
		return result, nil
	}
}

// LLen Get the length of a list
func (r *ValkeyAdapter) LLen(key string) (result int64) {
	return r.rc.LLen(r.ctx, key).Val()
}

// endregion

// region Distribute Locker actions ------------------------------------------------------------------------------------

// ObtainLocker tries to obtain a new lock using a key with the given TTL
func (r *ValkeyAdapter) ObtainLocker(key string, ttl time.Duration) (ILocker, error) {
	// Create a random token
	token := ID()

	if ok, err := r.SetRawNX(key, []byte(token), ttl); err != nil {
		return nil, err
	} else {
		if ok {
			return &Locker{rc: r.rc, key: key, token: token}, nil
		} else {
			return nil, fmt.Errorf("locker key already exists")
		}
	}
}

// endregion
