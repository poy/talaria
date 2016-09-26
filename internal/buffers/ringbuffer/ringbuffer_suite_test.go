package ringbuffer_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestRingbuffer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ringbuffer Suite")
}
