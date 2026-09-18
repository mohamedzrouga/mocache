package cluster

import "math/bits"

// slotSet is a 16384-bit set of slots.
//
// Ownership travels as ranges, but every change to it is set arithmetic: a
// reshard moves one slot from a node that keeps the rest, and gossip has to
// void exactly the part of a claim that moved and no more. Expressing that as
// interval splicing is where off-by-one bugs live, and getting it wrong means
// two owners for a slot — so it is done on a 2 KiB bitmap instead, which is
// exact by construction. These calls happen on gossip merges and reshards, a
// few times a second, never per request.
type slotSet [SlotCount / 64]uint64

func (s *slotSet) add(i int)      { s[i>>6] |= 1 << uint(i&63) }
func (s *slotSet) del(i int)      { s[i>>6] &^= 1 << uint(i&63) }
func (s *slotSet) has(i int) bool { return s[i>>6]&(1<<uint(i&63)) != 0 }

func (s *slotSet) count() int {
	n := 0
	for _, w := range s {
		n += bits.OnesCount64(w)
	}
	return n
}

func (s *slotSet) sub(o *slotSet) {
	for i := range s {
		s[i] &^= o[i]
	}
}

func (s *slotSet) intersects(o *slotSet) bool {
	for i := range s {
		if s[i]&o[i] != 0 {
			return true
		}
	}
	return false
}

// setOf builds the set a list of ranges describes. Slots outside 0-16383 are
// dropped rather than wrapping: a malformed claim from a peer must not be able
// to corrupt our view of a slot it never mentioned.
func setOf(ranges []SlotRange) *slotSet {
	var s slotSet
	for _, r := range ranges {
		for i := r.Start; i <= r.End; i++ {
			if i >= 0 && i < SlotCount {
				s.add(i)
			}
		}
	}
	return &s
}

// ranges renders the set back to the sorted, merged form nodes advertise.
func (s *slotSet) ranges() []SlotRange {
	var out []SlotRange
	for i := 0; i < SlotCount; {
		if !s.has(i) {
			i++
			continue
		}
		start := i
		for i < SlotCount && s.has(i) {
			i++
		}
		out = append(out, SlotRange{Start: start, End: i - 1})
	}
	return out
}

// addSlot returns ranges with slot included.
func addSlot(ranges []SlotRange, slot int) []SlotRange {
	s := setOf(ranges)
	s.add(slot)
	return s.ranges()
}

// removeSlot returns ranges with slot excluded.
func removeSlot(ranges []SlotRange, slot int) []SlotRange {
	s := setOf(ranges)
	s.del(slot)
	return s.ranges()
}

func slotsOverlap(a, b []SlotRange) bool {
	for _, x := range a {
		for _, y := range b {
			if x.Start <= y.End && y.Start <= x.End {
				return true
			}
		}
	}
	return false
}
