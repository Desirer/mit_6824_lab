package raft

import (
	"math/rand"
	"time"
)

const FRESH_TIME = time.Duration(50) * time.Millisecond

const HEART_BEAT_INTERVAL = time.Duration(150) * time.Millisecond

const ELECTION_BASE_TIME = 400

type Timer struct {
	last_time time.Time
	interval  time.Duration
}

func (t *Timer) passTime() bool {
	return time.Since(t.last_time) > t.interval
}

func (t *Timer) reset(interval time.Duration) {
	t.last_time = time.Now()
	t.interval = interval
}

func getRandElectTimeout() time.Duration {
	return time.Duration(ELECTION_BASE_TIME+rand.Int63()%ELECTION_BASE_TIME) * time.Millisecond
}
