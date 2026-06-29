package model

import "time"

type Metric struct {
	Target    Target
	Name      string
	Value     float64
	Timestamp time.Time
}
