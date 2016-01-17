package leadervalidator_test

import "github.com/apoydence/talaria/broker/leadervalidator"

type mockValidatorFetcher struct {
	urlCh       chan string
	validatorCh chan leadervalidator.Validator
}

func newMockValidatorFetcher() *mockValidatorFetcher {
	return &mockValidatorFetcher{
		urlCh:       make(chan string, 100),
		validatorCh: make(chan leadervalidator.Validator, 100),
	}
}

func (m *mockValidatorFetcher) FetchValidator(URL string) leadervalidator.Validator {
	m.urlCh <- URL
	return <-m.validatorCh
}
