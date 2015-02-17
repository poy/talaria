package neighbors_test

import (
	. "github.com/apoydence/talaria/neighbors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NeighborCollection", func() {
	Context("GetNeighbors", func() {
		It("Should return the all the neighbors with an empty blacklist", func() {
			na := NewNeighbor("a")
			nb := NewNeighbor("b")
			nc := NewNeighbor("c")
			n := NewNeighborCollection(na, nb, nc)

			ns := n.GetNeighbors()
			Expect(ns).To(Equal([]Neighbor{na, nb, nc}))
		})
		It("Should return only the neighbors that are not in the blacklist", func() {
			na := NewNeighbor("a")
			nb := NewNeighbor("b")
			nc := NewNeighbor("c")
			n := NewNeighborCollection(na, nb, nc)

			ns := n.GetNeighbors("a", "b")
			Expect(ns).To(Equal([]Neighbor{nc}))
		})
	})
})
