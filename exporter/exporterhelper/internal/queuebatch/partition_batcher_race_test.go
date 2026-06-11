// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queuebatch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/requesttest"
)

// Reproduces https://github.com/open-telemetry/opentelemetry-collector/issues/15422.
//
// flush() calls stopWG.Add(1) after consumeInternal has released currentBatchMu,
// while shutdownInternal calls stopWG.Wait() outside the lock. A Consume goroutine
// that observed active=true can therefore call Add after Wait has started with a
// zero counter, which panics with "sync: WaitGroup misuse: Add called concurrently
// with Wait".
//
// The timer must not fire during the test (FlushTimeout is one hour): the same
// unguarded Add runs on the timer goroutine, where a panic cannot be recovered
// and would crash the process instead of failing the test.
func TestPartitionBatcher_ConsumeShutdownRace(t *testing.T) {
	for attempt := 0; attempt < 500; attempt++ {
		cfg := BatchConfig{
			FlushTimeout: time.Hour,
			Sizer:        request.SizerTypeItems,
			MinSize:      0, // every Consume flushes immediately through the worker pool
		}
		sink := requesttest.NewSink()
		ba := newPartitionBatcher(cfg, request.NewItemsSizer(), nil, newWorkerPool(4), sink.Export, zap.NewNop(), nil)
		require.NoError(t, ba.Start(context.Background(), componenttest.NewNopHost()))

		var panicMsg atomic.Value
		capture := func(f func()) {
			defer func() {
				if r := recover(); r != nil {
					panicMsg.Store(fmt.Sprint(r))
				}
			}()
			f()
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 10; i++ {
					capture(func() {
						ba.Consume(context.Background(), &requesttest.FakeRequest{Items: 1, Bytes: 1}, newFakeDone())
					})
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			capture(func() { _ = ba.Shutdown(context.Background()) })
		}()

		close(start)
		finished := make(chan struct{})
		go func() { wg.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Fatalf("attempt %d: deadlock, likely WaitGroup state corruption after Add/Wait race", attempt)
		}
		if p := panicMsg.Load(); p != nil {
			t.Fatalf("attempt %d: panic: %v", attempt, p)
		}
	}
}
