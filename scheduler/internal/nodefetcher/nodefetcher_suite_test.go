package nodefetcher_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestNodefetcher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Nodefetcher Suite")
}
