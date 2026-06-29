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
	mu       sync.RWMutex
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
	s := &Scheduler{}

	for _, option := range opts {
		option(s)
	}

	return s
}

func (s *Scheduler) ProcessCommands(ctx context.Context, commands ...model.Command) {
	for _, cmd := range commands {
		switch cmd.Action {
		case model.ActionAdd:
			s.addTarget(ctx, cmd.Target)
		case model.ActionRemove:
			s.removeTarget(cmd.Target)
		case model.ActionUpdate:
			s.removeTarget(cmd.Target)
			s.addTarget(ctx, cmd.Target)
		}
	}
}

func (s *Scheduler) addTarget(ctx context.Context, target model.Target) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

func (s *Scheduler) removeTarget(target model.Target) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exist := s.registry[target.ID]; !exist {
		return
	}

	s.registry[target.ID].cancel()
	delete(s.registry, target.ID)
}

func (s *Scheduler) startTargetProcessing(ctx context.Context, target model.Target) {
	ticker := time.NewTicker(target.Interval)
	defer ticker.Stop()

	if err := s.pub.Publish(ctx, target); err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.pub.Publish(ctx, target); err != nil {
				return
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
