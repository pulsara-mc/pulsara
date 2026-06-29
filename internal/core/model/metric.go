package model

import "time"

type Metric struct {
	Target    string
	Name      string
	Value     float64
	Timestamp time.Time
}
