package intra_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestIntra(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Intra Suite")
}
