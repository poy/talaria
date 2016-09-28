package intraserver_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestIntraserver(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Intraserver Suite")
}
