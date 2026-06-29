package model

import "time"

type Target struct {
	Name     string
	Address  string
	Interval time.Time
	Timeout  time.Time
}
