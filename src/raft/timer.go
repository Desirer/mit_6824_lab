package raft

import (
	"math/rand"
	"sync"
	"time"
)

const FRESH_TIME = time.Duration(50) * time.Millisecond

const HEART_BEAT_INTERVAL = time.Duration(100) * time.Millisecond

const ELECTION_MIN_TIME = 800

const ELECTION_MAX_TIME = 1200

type Timer struct {
	mu        sync.Mutex
	last_time time.Time
	interval  time.Duration
}

func (t *Timer) passTime() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return time.Since(t.last_time) > t.interval
}

func (t *Timer) reset(interval time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.last_time = time.Now()
	t.interval = interval
}

func getRandElectTimeout() time.Duration {
	return time.Duration(ELECTION_MIN_TIME+rand.Int63()%(ELECTION_MAX_TIME-ELECTION_MIN_TIME)) * time.Millisecond
}
