package readers_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestReaders(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Readers Suite")
}
