package model

import (
	"time"

	"github.com/google/uuid"
)

type Target struct {
	ID       uuid.UUID
	Name     string
	Address  string
	Interval time.Duration
	Timeout  time.Duration
}
