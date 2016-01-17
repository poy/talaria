package leadervalidator_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestLeadervalidator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Leadervalidator Suite")
}
