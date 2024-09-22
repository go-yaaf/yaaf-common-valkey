// Structure definitions and factory method for Valkey implementation of IDataCache and IMessageBus
//

package facilities

import (
	"context"
	"fmt"
	"time"

	. "github.com/go-yaaf/yaaf-common/database"
	. "github.com/go-yaaf/yaaf-common/entity"
)

// region Key actions ----------------------------------------------------------------------------------------------

// GetRaw gets the value of a key in a byte array format
func (r *ValkeyAdapter) GetRaw(key string) ([]byte, error) {
	var bytes []byte

	cmd := r.rc.B().Get().Key(key).Build()
	res := r.rc.Do(context.Background(), cmd)

	if err := res.Error(); err != nil {
		return nil, err
	} else {
		if bytes, err = res.AsBytes(); err != nil {
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
		cmd := r.rc.B().Set().Key(key).Value(string(bytes)).Ex(expiration[0]).Build()
		res := r.rc.Do(context.Background(), cmd)
		return res.Error()
	} else {
		cmd := r.rc.B().Set().Key(key).Value(string(bytes)).Build()
		res := r.rc.Do(context.Background(), cmd)
		return res.Error()
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

	cmd := r.rc.B().Set().Key(key).Value(string(bytes)).Build()
	if len(expiration) > 0 {
		cmd = r.rc.B().Set().Key(key).Value(string(bytes)).Ex(expiration[0]).Build()
	}
	res := r.rc.Do(context.Background(), cmd)
	return res.AsBool()
}

// Del Delete keys
func (r *ValkeyAdapter) Del(keys ...string) error {
	cmd := r.rc.B().Del().Key(keys...).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.Error()
}

// GetKeys Get the value of all the given keys
func (r *ValkeyAdapter) GetKeys(factory EntityFactory, keys ...string) ([]Entity, error) {

	cmd := r.rc.B().Mget().Key(keys...).Build()
	res := r.rc.Do(context.Background(), cmd)
	if res.Error() != nil {
		return nil, res.Error()
	}

	if list, err := res.AsStrSlice(); err != nil {
		return nil, err
	} else {
		entities := make([]Entity, 0)
		for _, str := range list {
			if entity, er := rawToEntity(factory, []byte(str)); er == nil {
				entities = append(entities, entity)
			}
		}
		return entities, nil
	}
}

// GetRawKeys Get the value of all the given keys
func (r *ValkeyAdapter) GetRawKeys(keys ...string) ([]Tuple[string, []byte], error) {

	cmd := r.rc.B().Mget().Key(keys...).Build()
	res := r.rc.Do(context.Background(), cmd)
	if res.Error() != nil {
		return nil, res.Error()
	}

	if list, err := res.AsStrSlice(); err != nil {
		return nil, err
	} else {
		tuples := make([]Tuple[string, []byte], 0)
		for i, str := range list {
			tuple := Tuple[string, []byte]{Key: fmt.Sprintf("%d", i), Value: []byte(str)}
			tuples = append(tuples, tuple)
		}
		return tuples, nil
	}
}

// AddRaw Set the byte array value of a key only if the key does not exist
func (r *ValkeyAdapter) AddRaw(key string, bytes []byte, expiration time.Duration) (bool, error) {

	cmd := r.rc.B().Setnx().Key(key).Value(string(bytes)).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.AsBool()
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

	cmd := r.rc.B().Rename().Key(key).Newkey(newKey).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.Error()
}

// Scan keys from the provided cursor
func (r *ValkeyAdapter) Scan(from uint64, match string, count int64) (keys []string, cursor uint64, err error) {

	if len(match) == 0 {
		match = "*"
	}

	cmd := r.rc.B().Scan().Cursor(from).Match(match).Count(count).Build()
	res := r.rc.Do(context.Background(), cmd)
	if se, er := res.AsScanEntry(); er != nil {
		return nil, 0, res.Error()
	} else {
		return se.Elements, se.Cursor, nil
	}
}

// Exists Check if key exists
func (r *ValkeyAdapter) Exists(key string) (result bool, err error) {
	cmd := r.rc.B().Exists().Key(key).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.AsBool()
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
	cmd := r.rc.B().Hget().Key(key).Field(field).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.AsBytes()
}

// HKeys Get all the fields in a hash
func (r *ValkeyAdapter) HKeys(key string) ([]string, error) {
	cmd := r.rc.B().Hkeys().Key(key).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.AsStrSlice()
}

// HGetAll Get all the fields and values in a hash
func (r *ValkeyAdapter) HGetAll(factory EntityFactory, key string) (map[string]Entity, error) {
	cmd := r.rc.B().Hgetall().Key(key).Build()
	res := r.rc.Do(context.Background(), cmd)

	result := make(map[string]Entity)

	if list, err := res.AsStrMap(); err != nil {
		return nil, err
	} else {
		for k, str := range list {
			if entity, er := rawToEntity(factory, []byte(str)); er == nil {
				result[k] = entity
			}
		}
		return result, nil
	}
}

// HGetRawAll gets all the fields and raw values in a hash
func (r *ValkeyAdapter) HGetRawAll(key string) (map[string][]byte, error) {
	cmd := r.rc.B().Hgetall().Key(key).Build()
	res := r.rc.Do(context.Background(), cmd)

	result := make(map[string][]byte)

	if list, err := res.AsStrMap(); err != nil {
		return nil, err
	} else {
		for k, str := range list {
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
		cmd := r.rc.B().Hset().Key(key).FieldValue().FieldValue(field, string(bytes)).Build()
		res := r.rc.Do(context.Background(), cmd)
		return res.Error()
	}
}

// HSetRaw sets the raw value of a hash field
func (r *ValkeyAdapter) HSetRaw(key, field string, bytes []byte) error {
	cmd := r.rc.B().Hset().Key(key).FieldValue().FieldValue(field, string(bytes)).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.Error()
}

// HSetNX Set value of key only if it is not exist with optional expiration, return false if the key exists
func (r *ValkeyAdapter) HSetNX(key string, field string, entity Entity) (bool, error) {
	if bytes, err := entityToRaw(entity); err != nil {
		return false, err
	} else {
		cmd := r.rc.B().Hsetnx().Key(key).Field(field).Value(string(bytes)).Build()
		res := r.rc.Do(context.Background(), cmd)
		return res.AsBool()
	}
}

// HSetRawNX sets the raw value of key only if it is not exist with optional expiration, return false if the key exists
func (r *ValkeyAdapter) HSetRawNX(key string, field string, bytes []byte) (bool, error) {
	cmd := r.rc.B().Hsetnx().Key(key).Field(field).Value(string(bytes)).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.AsBool()
}

// HDel Delete one or more hash fields
func (r *ValkeyAdapter) HDel(key string, fields ...string) error {
	cmd := r.rc.B().Hdel().Key(key).Field(fields...).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.Error()
}

// HAdd sets the value of a key only if the key does not exist
func (r *ValkeyAdapter) HAdd(key, field string, entity Entity) (bool, error) {
	if bytes, err := entityToRaw(entity); err != nil {
		return false, err
	} else {
		cmd := r.rc.B().Hsetnx().Key(key).Field(field).Value(string(bytes)).Build()
		res := r.rc.Do(context.Background(), cmd)
		return res.AsBool()
	}
}

// HAddRaw sets the raw value of a key only if the key does not exist
func (r *ValkeyAdapter) HAddRaw(key, field string, bytes []byte) (bool, error) {
	cmd := r.rc.B().Hsetnx().Key(key).Field(field).Value(string(bytes)).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.AsBool()
}

// HExists Check if key exists
func (r *ValkeyAdapter) HExists(key, field string) (bool, error) {
	cmd := r.rc.B().Hexists().Key(key).Field(field).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.AsBool()
}

// endregion

// region List actions ---------------------------------------------------------------------------------------------

// RPush Append one or multiple values to a list
func (r *ValkeyAdapter) RPush(key string, value ...Entity) error {
	values := make([]string, 0)
	for _, v := range value {

		if bytes, err := entityToRaw(v); err != nil {
			continue
		} else {
			values = append(values, string(bytes))
		}
	}
	cmd := r.rc.B().Rpush().Key(key).Element(values...).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.Error()
}

// LPush Prepend one or multiple values to a list
func (r *ValkeyAdapter) LPush(key string, value ...Entity) error {
	values := make([]string, 0)
	for _, v := range value {

		if bytes, err := entityToRaw(v); err != nil {
			continue
		} else {
			values = append(values, string(bytes))
		}
	}
	cmd := r.rc.B().Lpush().Key(key).Element(values...).Build()
	res := r.rc.Do(context.Background(), cmd)
	return res.Error()
}

// RPop Remove and get the last element in a list
func (r *ValkeyAdapter) RPop(factory EntityFactory, key string) (Entity, error) {

	cmd := r.rc.B().Rpop().Key(key).Build()
	res := r.rc.Do(context.Background(), cmd)

	if bytes, err := res.AsBytes(); err != nil {
		return nil, err
	} else {
		return rawToEntity(factory, bytes)
	}
}

// LPop Remove and get the first element in a list
func (r *ValkeyAdapter) LPop(factory EntityFactory, key string) (entity Entity, err error) {
	cmd := r.rc.B().Lpop().Key(key).Build()
	res := r.rc.Do(context.Background(), cmd)

	if bytes, err := res.AsBytes(); err != nil {
		return nil, err
	} else {
		return rawToEntity(factory, bytes)
	}
}

// BRPop Remove and get the last element in a list or block until one is available
func (r *ValkeyAdapter) BRPop(factory EntityFactory, timeout time.Duration, keys ...string) (key string, entity Entity, err error) {

	cmd := r.rc.B().Brpop().Key(keys...).Timeout(float64(timeout)).Build()
	res := r.rc.Do(context.Background(), cmd)
	if res.Error() != nil {
		return "", nil, res.Error()
	}

	/*
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

	*/
	return "", nil, fmt.Errorf("not implemented yet")
}

// BLPop Remove and get the first element in a list or block until one is available
func (r *ValkeyAdapter) BLPop(factory EntityFactory, timeout time.Duration, keys ...string) (key string, entity Entity, err error) {

	cmd := r.rc.B().Blpop().Key(keys...).Timeout(float64(timeout)).Build()
	res := r.rc.Do(context.Background(), cmd)
	if res.Error() != nil {
		return "", nil, res.Error()
	}

	/*
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

	*/
	return "", nil, fmt.Errorf("not implemented yet")
}

// LRange Get a range of elements from list
func (r *ValkeyAdapter) LRange(factory EntityFactory, key string, start, stop int64) ([]Entity, error) {
	cmd := r.rc.B().Lrange().Key(key).Start(start).Stop(stop).Build()
	res := r.rc.Do(context.Background(), cmd)
	list, err := res.AsStrSlice()
	if err != nil {
		return nil, err
	}

	result := make([]Entity, 0)
	for _, str := range list {
		if entity, er := rawToEntity(factory, []byte(str)); er == nil {
			result = append(result, entity)
		}
	}
	return result, nil
}

// LLen Get the length of a list
func (r *ValkeyAdapter) LLen(key string) (result int64) {
	cmd := r.rc.B().Llen().Key(key).Build()
	res := r.rc.Do(context.Background(), cmd)
	if rt, err := res.AsInt64(); err != nil {
		return 0
	} else {
		return rt
	}
}

// endregion

// region Distribute Locker actions ------------------------------------------------------------------------------------

// ObtainLocker tries to obtain a new lock using a key with the given TTL
func (r *ValkeyAdapter) ObtainLocker(key string, ttl time.Duration) (ILocker, error) {
	// Create locker
	return createNewLocker(r.uri, key, ttl)
}

// endregion
