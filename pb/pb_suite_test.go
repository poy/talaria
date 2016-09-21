package pb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestPb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pb Suite")
}
