package cluster

import (
	"errors"
	"fmt"
)

// Live slot migration: the state behind CLUSTER SETSLOT.
//
// Moving a slot between two live nodes cannot be atomic — keys are copied one
// at a time while clients keep reading and writing. Redis closes that window
// with two per-slot markers and one extra redirect, and MoCache uses the same
// ones because the clients already implement them:
//
//	MIGRATING on the source       a key that is still here is served here; a key
//	                              that is gone gets -ASK, pointing at the target
//	IMPORTING on the destination  serve this slot only for a client that sent
//	                              ASKING; everyone else gets -MOVED to the source
//
// The pair is what makes the move invisible: a key is always served by exactly
// one of the two nodes, and the client is told which one without ever being
// told the slot has moved. Only the final SETSLOT NODE changes ownership, and
// only that is gossiped — as a raised config epoch, the same mechanism a
// failover uses. A reshard abandoned halfway therefore leaves no trace beyond
// the two nodes involved, and clearing it is CLUSTER SETSLOT STABLE.
//
// Nothing here moves data. MIGRATE does that, in internal/server, and it can
// only run because these markers make the half-moved state safe to serve.

// ErrNotOwner and friends carry the wording Redis uses, because redis-cli
// --cluster matches on it when it decides whether a reshard can continue.
var (
	errSlotRange = errors.New("Invalid or out of range slot")
)

// SetSlotMigrating marks a slot as leaving this node for dest. Only the owner
// of a slot may migrate it away: two nodes both claiming to be the source would
// send clients to each other.
func (m *Manager) SetSlotMigrating(slot int, destID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slot < 0 || slot >= SlotCount {
		return errSlotRange
	}
	if m.nodes[destID] == nil {
		return fmt.Errorf("I don't know about node %s", destID)
	}
	if destID == m.me {
		return errors.New("Target node is myself")
	}
	if !m.ownsSlotLocked(m.me, slot) {
		return fmt.Errorf("I'm not the owner of hash slot %d", slot)
	}
	m.migrating[slot] = destID
	m.rebuildLocked()
	return nil
}

// SetSlotImporting marks a slot as arriving here from src.
func (m *Manager) SetSlotImporting(slot int, srcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slot < 0 || slot >= SlotCount {
		return errSlotRange
	}
	if m.nodes[srcID] == nil {
		return fmt.Errorf("I don't know about node %s", srcID)
	}
	if srcID == m.me {
		return errors.New("Target node is myself")
	}
	if m.ownsSlotLocked(m.me, slot) {
		return fmt.Errorf("I'm already the owner of hash slot %d", slot)
	}
	m.importing[slot] = srcID
	m.rebuildLocked()
	return nil
}

// SetSlotStable abandons a migration, leaving ownership where it already is.
func (m *Manager) SetSlotStable(slot int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slot < 0 || slot >= SlotCount {
		return errSlotRange
	}
	delete(m.migrating, slot)
	delete(m.importing, slot)
	m.rebuildLocked()
	return nil
}

// SetSlotOwner commits the move: the slot now belongs to nodeID, and both
// markers are cleared.
//
// redis-cli sends this to the source, the destination and every other primary.
// Only the destination — the node that was importing — raises a config epoch,
// because only one new claim may exist per move; the others apply it locally
// and would converge on that claim through gossip anyway.
func (m *Manager) SetSlotOwner(slot int, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slot < 0 || slot >= SlotCount {
		return errSlotRange
	}
	target := m.nodes[nodeID]
	if target == nil {
		return fmt.Errorf("Unknown node %s", nodeID)
	}
	wasImporting := m.importing[slot] != ""
	delete(m.migrating, slot)
	delete(m.importing, slot)

	for _, n := range m.nodes {
		if n.id == nodeID {
			continue
		}
		if setOf(n.slots).has(slot) {
			// The old owner keeps every other slot it serves and stays a
			// primary even if this was its last one: an empty primary is a
			// valid, addressable state, and CLUSTER REPLICATE is how an
			// operator turns it into a replica afterwards.
			n.slots = removeSlot(n.slots, slot)
		}
	}
	target.slots = addSlot(target.slots, slot)
	target.role = RolePrimary
	target.primaryAddr = ""
	if wasImporting && nodeID == m.me {
		target.configEpoch = m.bumpEpochLocked()
	}
	m.rebuildLocked()
	return nil
}

// BumpConfigEpoch implements CLUSTER BUMPEPOCH: take a config epoch above every
// one we know of, so this node's claims win wherever they are compared.
func (m *Manager) BumpConfigEpoch() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.bumpEpochLocked()
	if n := m.nodes[m.me]; n != nil {
		n.configEpoch = e
	}
	m.rebuildLocked()
	return e
}

// bumpEpochLocked returns an epoch above every claim currently known. Two
// reshards running at once could draw the same number; Redis has the same
// property, and the answer is the same — drive a reshard from one place.
func (m *Manager) bumpEpochLocked() uint64 {
	highest := m.currentEpoch
	for _, n := range m.nodes {
		if n.configEpoch > highest {
			highest = n.configEpoch
		}
	}
	m.currentEpoch = highest + 1
	return m.currentEpoch
}

func (m *Manager) ownsSlotLocked(id string, slot int) bool {
	n := m.nodes[id]
	return n != nil && n.role == RolePrimary && setOf(n.slots).has(slot)
}
