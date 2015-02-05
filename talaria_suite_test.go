package talaria_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestTalaria(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Talaria Suite")
}
