package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pulsara-mc/pulsara/internal/core/model"
)

type targetController struct {
	target model.Target
	cancel context.CancelFunc
}

type Publisher interface {
	Publish(ctx context.Context, target model.Target) error
}

type Scheduler struct {
	mu       sync.Mutex
	registry map[uuid.UUID]*targetController
	pub      Publisher
}

type Option func(s *Scheduler)

func WithPublisher(pub Publisher) Option {
	return func(s *Scheduler) {
		s.pub = pub
	}
}

func New(opts ...Option) *Scheduler {
	s := &Scheduler{
		registry: map[uuid.UUID]*targetController{},
	}

	for _, option := range opts {
		option(s)
	}

	return s
}

func (s *Scheduler) ProcessCommands(ctx context.Context, commands []model.Command) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, cmd := range commands {
		switch cmd.Action {
		case model.ActionAdd:
			s.executeAdd(ctx, cmd.Target)
		case model.ActionRemove:
			s.executeRemove(cmd.Target)
		case model.ActionUpdate:
			s.executeRemove(cmd.Target)
			s.executeAdd(ctx, cmd.Target)
		}
	}
}

func (s *Scheduler) executeAdd(ctx context.Context, target model.Target) {
	if _, exist := s.registry[target.ID]; exist {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	s.registry[target.ID] = &targetController{
		target: target,
		cancel: cancel,
	}

	go s.startTargetProcessing(ctx, target)
}

func (s *Scheduler) executeRemove(target model.Target) {
	controller, exist := s.registry[target.ID]
	if !exist {
		return
	}

	controller.cancel()
	delete(s.registry, target.ID)
}

func (s *Scheduler) startTargetProcessing(ctx context.Context, target model.Target) {
	ticker := time.NewTicker(target.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.pub.Publish(ctx, target); err != nil {
				continue
			}
		}
	}
}

func (s *Scheduler) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, controller := range s.registry {
		controller.cancel()
		delete(s.registry, id)
	}
}
