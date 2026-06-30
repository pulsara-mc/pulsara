package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pulsara-mc/pulsara/internal/core/model"
	"github.com/stretchr/testify/assert"
)

type mockPublisher struct {
	mu    sync.Mutex
	calls []model.Target
	ch    chan model.Target
}

func (pub *mockPublisher) Publish(ctx context.Context, target model.Target) error {
	pub.mu.Lock()
	defer pub.mu.Unlock()

	pub.calls = append(pub.calls, target)
	pub.ch <- target

	return nil
}

func (pub *mockPublisher) getCalls() []model.Target {
	pub.mu.Lock()
	defer pub.mu.Unlock()

	cp := make([]model.Target, len(pub.calls))
	copy(cp, pub.calls)
	return cp
}

func TestScheduler_ProcessCommands(t *testing.T) {
	t.Parallel()

	id1, id2 := uuid.New(), uuid.New()

	tgt1 := model.Target{ID: id1, Name: "service-1", Interval: 10 * time.Millisecond}
	tgt2 := model.Target{ID: id2, Name: "service-2", Interval: 10 * time.Millisecond}
	tgt1upd := model.Target{ID: id1, Name: "service-1", Interval: 20 * time.Millisecond}

	tests := []struct {
		name        string
		cmdBatches  [][]model.Command
		cmdInterval time.Duration
		timeout     time.Duration
		ordered     bool
		expect      []model.Target
	}{
		{
			name: "Atomic service add",
			cmdBatches: [][]model.Command{
				{{model.ActionAdd, tgt1}},
			},
			timeout: 15 * time.Millisecond,
			expect:  []model.Target{tgt1},
		},
		{
			name: "Idempotent service add",
			cmdBatches: [][]model.Command{
				{
					{model.ActionAdd, tgt1},
					{model.ActionAdd, tgt1},
				},
			},
			timeout: 15 * time.Millisecond,
			expect:  []model.Target{tgt1},
		},
		{
			name: "Two parallel services",
			cmdBatches: [][]model.Command{
				{
					{model.ActionAdd, tgt1},
					{model.ActionAdd, tgt2},
				},
			},
			timeout: 25 * time.Millisecond,
			expect:  []model.Target{tgt1, tgt1, tgt2, tgt2},
		},
		{
			name: "Stop on remove",
			cmdBatches: [][]model.Command{
				{{model.ActionAdd, tgt1}},
				{{model.ActionRemove, tgt1}},
			},
			cmdInterval: 15 * time.Millisecond,
			timeout:     25 * time.Millisecond,
			expect:      []model.Target{tgt1},
		},
		{
			name: "Real-time update",
			cmdBatches: [][]model.Command{
				{{model.ActionAdd, tgt1}},
				{{model.ActionUpdate, tgt1upd}},
			},
			cmdInterval: 25 * time.Millisecond,
			timeout:     50 * time.Millisecond,
			expect:      []model.Target{tgt1, tgt1, tgt1upd},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := &mockPublisher{
				calls: make([]model.Target, 0),
				ch:    make(chan model.Target, 100),
			}
			scheduler := New(WithPublisher(mock))

			go func() {
				for _, batch := range test.cmdBatches {
					scheduler.ProcessCommands(context.Background(), batch)
					time.Sleep(test.cmdInterval)
				}
			}()

			time.Sleep(test.timeout)
			scheduler.Stop()

			actual := mock.getCalls()
			if test.ordered {
				assert.Equal(t, test.expect, actual)
			} else {
				assert.ElementsMatch(t, test.expect, actual)
			}
		})
	}
}
