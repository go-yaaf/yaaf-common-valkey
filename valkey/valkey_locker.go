// Valkey based implementation of IDataCache interface
//

package facilities

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeylock"

	. "github.com/go-yaaf/yaaf-common/database"
)

// This implementation of distributed locker based on Valkey
// TODO: Add implementation for locker TTL

// LockerImpl represents an obtained, distributed lock.
type LockerImpl struct {
	locker  valkeylock.Locker
	key     string
	token   string
	ctx     context.Context
	ctxFunc context.CancelFunc
	ttlFunc context.CancelFunc
}

func createNewLocker(URI string, key string, ttl time.Duration) (ILocker, error) {
	options, er := valkey.ParseURL(URI)
	if er != nil {
		return nil, er
	}

	locker, err := valkeylock.NewLocker(valkeylock.LockerOption{
		ClientOption:   options,
		KeyMajority:    1,    // Use KeyMajority=1 if you have only one Valkey instance. Also make sure that all your `Locker`s share the same KeyMajority.
		NoLoopTracking: true, // Enable this to have better performance if all your Valkey are >= 7.0.5.
	})
	if err != nil {
		return nil, err
	}

	result := &LockerImpl{key: key}

	// set context
	lockerCtx := context.Background()
	if ttl > 0 {
		lockerCtx, result.ttlFunc = context.WithTimeout(lockerCtx, ttl)
	}

	ctx, cancel, err := locker.WithContext(lockerCtx, key)
	if err != nil {
		return nil, err
	}

	result.locker = locker
	result.ctx = ctx
	result.ctxFunc = cancel

	return result, nil
}

// Key returns the locker key used by the lock.
func (l *LockerImpl) Key() string {
	return l.key
}

// Token returns the token value set by the lock.
func (l *LockerImpl) Token() string {
	return l.token
}

// TTL returns the remaining time-to-live. Returns 0 if the lock has expired.
func (l *LockerImpl) TTL(ctx context.Context) (time.Duration, error) {
	// TODO: GET TTL implementation
	return 0, fmt.Errorf("not yet implemented")
}

// Refresh extends the lock with a new TTL.
func (l *LockerImpl) Refresh(ctx context.Context, ttl time.Duration) error {

	// TODO: Extend TTL implementation
	return fmt.Errorf("not yet implemented")
}

// Release manually releases the lock.
func (l *LockerImpl) Release(ctx context.Context) error {

	if l.ctxFunc != nil {
		l.ctxFunc()
	}

	if l.ttlFunc != nil {
		l.ttlFunc()
	}

	if l.locker != nil {
		l.locker.Close()
	}
	return nil
}
