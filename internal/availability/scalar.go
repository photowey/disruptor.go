package availability

type ScalarScanner struct{}

func NewScalarScanner() Scanner {
	return ScalarScanner{}
}

func (s ScalarScanner) HighestPublished(request ScanRequest) int64 {
	if request.Availability == nil || request.AvailableSequence < request.LowerBound {
		return request.LowerBound - 1
	}

	for sequence := request.LowerBound; sequence <= request.AvailableSequence; sequence++ {
		if !request.Availability.Available(sequence) {
			return sequence - 1
		}
	}

	return request.AvailableSequence
}
