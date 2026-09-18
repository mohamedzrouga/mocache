"""Slot routing parity with the Go SDK and with internal/cluster.

The vectors below are duplicated verbatim in sdk/go/slots_test.go. All three
implementations must agree, or a key written through one access path is read as
a miss through another. Changing any of them means changing all three.
"""

from __future__ import annotations

import unittest

from mocache import MoCacheClient, SlotRouter, hash_tag, key_slot
from mocache.client import SLOT_COUNT, _crc16, parse_slot_ranges

SLOT_VECTORS = {
    "hello": 866,
    "abc": 7638,
    "foo": 12182,
    "user:1": 10778,  # the tag below is literally this key
    "{user:1}:name": 10778,
    "{user:1}:email": 10778,
    "": 0,
    "{}": 15257,
    "{a": 10276,
    "{a}b": 15495,
}

THREE_NODES = {
    "http://a:8090": "0-5460",
    "http://b:8090": "5461-10922",
    "http://c:8090": "10923-16383",
}


class TestSlots(unittest.TestCase):
    def test_crc16_check_value(self) -> None:
        # The standard CRC-16/XMODEM check value. Every Redis client computes
        # slots itself, so a change here misroutes silently rather than failing.
        self.assertEqual(_crc16(b"123456789"), 0x31C3)

    def test_key_slot_vectors(self) -> None:
        for key, want in SLOT_VECTORS.items():
            self.assertEqual(key_slot(key), want, f"key_slot({key!r})")

    def test_hash_tag_groups_keys(self) -> None:
        self.assertEqual(key_slot("{user:1}:name"), key_slot("{user:1}:email"))
        # An empty or unterminated tag is not a tag: the whole key is hashed.
        self.assertEqual(hash_tag("{}"), "{}")
        self.assertEqual(hash_tag("{a"), "{a")
        self.assertEqual(hash_tag("no-tag"), "no-tag")
        self.assertEqual(hash_tag("{a}b"), "a")

    def test_router_places_keys(self) -> None:
        router = SlotRouter(THREE_NODES)
        self.assertEqual(router.node("hello"), "http://a:8090")
        self.assertEqual(router.node("abc"), "http://b:8090")
        self.assertEqual(router.node("foo"), "http://c:8090")

    def test_router_rejects_bad_maps(self) -> None:
        # A gap would route part of the keyspace nowhere.
        with self.assertRaises(ValueError):
            SlotRouter({"http://a:8090": "0-16382"})
        # An overlap would route one key to two nodes depending on map order.
        with self.assertRaises(ValueError):
            SlotRouter({"http://a:8090": "0-10000", "http://b:8090": "9000-16383"})

    def test_parse_slot_ranges(self) -> None:
        self.assertEqual(
            parse_slot_ranges("5461-10922, 12000-12100 ,15000"),
            [(5461, 10922), (12000, 12100), (15000, 15000)],
        )
        for bad in ("", "abc", "10-5", f"0-{SLOT_COUNT}", "-1-5"):
            with self.assertRaises(ValueError, msg=bad):
                parse_slot_ranges(bad)

    def test_client_uses_slots_when_given(self) -> None:
        client = MoCacheClient(list(THREE_NODES), slots=THREE_NODES)
        try:
            self.assertIsInstance(client._ring, SlotRouter)
            self.assertEqual(client._ring.node("hello"), "http://a:8090")
        finally:
            client.close()

    def test_ring_remains_the_default(self) -> None:
        # Without slots an existing deployment's keys must not move.
        from mocache import HashRing

        client = MoCacheClient(list(THREE_NODES))
        try:
            self.assertIsInstance(client._ring, HashRing)
            ring = HashRing(list(THREE_NODES))
            for key in SLOT_VECTORS:
                self.assertEqual(client._ring.node(key), ring.node(key))
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
