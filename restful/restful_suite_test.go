package restful_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestRestful(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Restful Suite")
}
