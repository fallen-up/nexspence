package distlock

import (
	"context"
	"errors"
	"time"
)

// ErrLockHeld is returned by Acquire when the lock is already held by another caller.
var ErrLockHeld = errors.New("distlock: lock already held")

// ErrLockLost is returned by Refresh when the lock is no longer this holder's:
// it expired, and possibly another caller has taken it since.
var ErrLockLost = errors.New("distlock: lock no longer held")

// Locker acquires distributed locks identified by key.
type Locker interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (Lock, error)
	// ForceRelease drops key whoever holds it. It exists for cleaning up after a
	// holder that can no longer release its own lock — a node that crashed
	// mid-run — where waiting out the TTL would block the work for hours. Every
	// other caller releases through the Lock it acquired.
	ForceRelease(ctx context.Context, key string) error
}

// Lock represents an acquired distributed lock that can be released.
type Lock interface {
	Release(ctx context.Context) error
}

// Refresher is implemented by a Lock whose holder can extend its TTL, for work
// that can outlive the TTL it was acquired with but cannot simply stop at a
// deadline. Refresh returns ErrLockLost when the lock is no longer held.
type Refresher interface {
	Refresh(ctx context.Context, ttl time.Duration) error
}
