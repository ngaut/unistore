package raftstore

import (
	"time"
	"sync/atomic"
)

type LeaseState int

const (
	LeaseState_Suspect LeaseState = 1
	LeaseState_Valid
	LeaseState_Expired
)

type Lease struct {
	boundSuspect *time.Time
	boundValid *time.Time
	maxLease time.Duration

	maxDrift time.Duration
	lastUpdate time.Time
	remote *RemoteLease
}

func NewLease(maxLease time.Duration) *Lease {
	return &Lease{
		maxLease: maxLease,
		maxDrift: maxLease / 3,
		lastUpdate: time.Unix(0, 0),
	}
}

func (l *Lease) nextExpiredTime(sendTs time.Time) time.Time {
	return sendTs.Add(l.maxLease)
}

func (l *Lease) Renew(sendTs time.Time) {
	bound := l.nextExpiredTime(sendTs)
	if l.boundSuspect != nil || l.boundValid != nil {
		if l.boundSuspect != nil && l.boundSuspect < bound {
			l.boundSuspect = nil
			l.boundValid = &bound
		}
		if l.boundValid != nil && l.boundValid < bound {
			l.boundValid = &bound
		}
	} else {
		l.boundValid = &sendTs
	}

	// Renew remote if it's valid.
	if l.boundValid != nil {
		if l.boundValid.Sub(l.lastUpdate) > l.maxDrift {
			l.lastUpdate = l.boundValid
			if l.remote != nil {
				l.remote.Renew(l.boundValid)
			}
		}
	}
}

func (l *Lease) Suspect(sendTs time.Time) {
	l.ExpireRemoteLease()
	bound := l.nextExpiredTime(sendTs)
	l.boundValid = nil
	l.boundSuspect = &bound
}

func (l *Lease) Inspect(ts *time.Time) LeaseState {
	if l.boundSuspect != nil {
		return LeaseState_Suspect
	}
	if l.boundValid != nil {
		if ts == nil {
			ts = &time.Now()
		}
		if ts < l.boundValid {
			return LeaseState_Valid
		} else {
			return LeaseState_Expired
		}
	}
	return LeaseState_Expired
}

func (l *Lease) Expire() {
	l.ExpireRemoteLease()
	l.boundValid = nil
	l.boundSuspect = nil
}

func (l *Lease) ExpireRemoteLease() {
	if l.remote != nil {
		l.remote.Expire()
		l.remote = nil
	}
}

func (l *Lease) MaybeNewRemoteLease(term uint64) *RemoteLease {
	if l.remote != nil {
		if l.remote.term == term {
			return nil
		} else {
			panic("Must expire the old remote lease first!")
		}
	}
	expiredTime := 0
	if l.boundValid != nil {
		expiredTime = TimeToU64(l.boundValid)
	}
	remote := &RemoteLease {
		expiredTime: &expiredTime,
		term: term,
	}
	remoteClone := &RemoteLease {
		expiredTime: &expiredTime,
		term: term,
	}
	l.remote = remote
	return remoteClone
}

type RemoteLease struct {
	expiredTime *uint64
	term uint64
}

func (r *RemoteLease) Inspect(ts *time.Time) LeaseState {
	expiredTime := atomic.LoadInt64(r.expiredTime)
	if ts == nil {
		ts = &time.Now()
	}
	if ts.Before(U64ToTime(expiredTime)) {
		return LeaseState_Valid
	} else {
		return LeaseState_Expired
	}
}

func (r *RemoteLease) Renew(bound time.Time) {
	atomic.StoreUint64(r.expiredTime, TimeToU64(bound))
}

func (r *RemoteLease) Expire() {
	atomic.StoreUint64(r.expiredTime, 0)
}

func (r *RemoteLease) Term() uint64 {
	return r.term
}

const (
	NSEC_PER_MSEC uint64 = 1000000
	SEC_SHIFT uint64 = 10
	MSEC_MASK uint64 = (1 << SEC_SHIFT) - 1
)

func TimeToU64(t time.Time) uint64 {
	sec := t.Unix()
	msec := t.Nanosecond() / NSEC_PER_MSEC
	sec <<= SEC_SHIFT
	return sec | msec
}

func U64ToTime(u uint64) time.Time {
	sec := u >> SEC_SHIFT
	nsec := (u & MSEC_MASK) * NSEC_PER_MSEC
	time.Unix(sec, nsec)
}