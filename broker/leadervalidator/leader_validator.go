package leadervalidator

import "github.com/apoydence/talaria/logging"

type ReplicaFetcher interface {
	FetchReplicas(name string) []string
}

type Validator interface {
	ValidateLeader(name string, index uint64) bool
}

type ValidatorFetcher interface {
	FetchValidator(URL string) Validator
}

type LeaderValidator struct {
	log              logging.Logger
	replicaFetcher   ReplicaFetcher
	validatorFetcher ValidatorFetcher
}

func New(replicaFetcher ReplicaFetcher, validatorFetcher ValidatorFetcher) *LeaderValidator {
	leaderValidator := &LeaderValidator{
		log:              logging.Log("LeaderValidator"),
		replicaFetcher:   replicaFetcher,
		validatorFetcher: validatorFetcher,
	}

	return leaderValidator
}

func (l *LeaderValidator) Validate(name string, index uint64, callback func(string, bool)) {
	l.log.Debug("Validating leader for %s (index=%d)", name, index)
	go l.core(name, index, callback)
}

func (l *LeaderValidator) core(name string, index uint64, callback func(string, bool)) {
	callback(name, l.validate(name, index))
}

func (l *LeaderValidator) validate(name string, index uint64) bool {
	URLs := l.replicaFetcher.FetchReplicas(name)
	for _, URL := range URLs {
		v := l.validatorFetcher.FetchValidator(URL)
		if v != nil && !v.ValidateLeader(name, index) {
			return false
		}
	}

	return true
}
