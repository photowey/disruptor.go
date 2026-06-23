package availability

type Checker interface {
	Available(sequence int64) bool
}

type Scanner interface {
	HighestPublished(request ScanRequest) int64
}

type ScanRequest struct {
	LowerBound        int64
	AvailableSequence int64
	Availability      Checker
}
