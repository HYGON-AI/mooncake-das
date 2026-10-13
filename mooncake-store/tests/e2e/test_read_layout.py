# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Layout API checks against an isolated/existing master.

MOONCAKE_MASTER=host:port python3 test_read_layout.py
Only UUID-prefixed keys and this process's registered buffers are modified.
"""

import ctypes
import os
import unittest
from uuid import uuid4

from mooncake.store import (
    MooncakeDistributedStore,
    ReadComponent,
    ReadKeyMapping,
    ReadLayout,
)


class ReadLayoutTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.store = MooncakeDistributedStore()
        rc = cls.store.setup(
            "127.0.0.1:0",
            "P2PHANDSHAKE",
            64 * 1024**2,
            16 * 1024**2,
            "tcp",
            "",
            os.environ["MOONCAKE_MASTER"],
        )
        if rc != 0:
            raise RuntimeError(f"Store setup failed: {rc}")

    @classmethod
    def tearDownClass(cls):
        cls.store.close()

    def setUp(self):
        self.buffer = (ctypes.c_ubyte * 64)()
        self.base = ctypes.addressof(self.buffer)
        self.keys = [f"layout-{uuid4().hex}-{i}" for i in range(2)]
        self.assertEqual(self.store.register_buffer(self.base, 64), 0)
        for key, data in zip(self.keys, (b"abcdefgh", b"ijklmnop")):
            self.assertEqual(self.store.put(key, data), 0)
        self.assertEqual(list(self.store.batch_get_session_start(self.keys)), [0, 0])

    def tearDown(self):
        self.assertEqual(self.store.batch_get_session_end(self.keys), 0)
        self.assertEqual(list(self.store.batch_remove(self.keys, True)), [0, 0])
        self.assertEqual(self.store.unregister_buffer(self.base), 0)

    def component(self, base=None, count=8, offset=0):
        return ReadComponent(
            destination_base=self.base if base is None else base,
            destination_stride=8,
            byte_count=count,
            source_offset=offset,
        )

    def layout(self, keys, rows, components=None, mapping=ReadKeyMapping.PER_ROW):
        return ReadLayout(
            keys=keys,
            rows=rows,
            components=[self.component()] if components is None else components,
            key_mapping=mapping,
        )

    def test_snapshot_reordered_rows_and_readonly_fields(self):
        keys, rows, components = self.keys.copy(), [7, 2], [self.component()]
        layout = self.layout(keys, rows, components)
        keys.clear()
        rows.clear()
        components.clear()
        # Getters return copies; modifying them cannot change native storage.
        layout.keys.clear()
        layout.rows.clear()
        layout.components.clear()
        self.assertEqual(self.store.batch_get_into_layouts([layout]), [[8, 8]])
        self.assertEqual(bytes(self.buffer)[56:64], b"abcdefgh")
        self.assertEqual(bytes(self.buffer)[16:24], b"ijklmnop")
        self.assertEqual(bytes(self.buffer)[:16], bytes(16))
        with self.assertRaises(AttributeError):
            layout.keys = []
        with self.assertRaises(AttributeError):
            layout.components[0].byte_count = 1

    def test_duplicate_keys_multiple_components_and_layouts(self):
        parts = [self.component(count=4), self.component(self.base + 4, 4, 4)]
        self.assertEqual(
            self.store.batch_get_into_layouts(
                [
                    self.layout([self.keys[0]] * 2, [7, 2], parts),
                    self.layout([self.keys[0]], [0]),
                ]
            ),
            [[8, 8], [8]],
        )
        for row in (0, 2, 7):
            self.assertEqual(bytes(self.buffer)[row * 8 : row * 8 + 8], b"abcdefgh")

    def test_per_component_mapping_preserves_zero_byte_position(self):
        self.assertEqual(
            self.store.batch_get_into_layouts(
                [
                    self.layout(
                        self.keys,
                        [1],
                        [self.component(count=0), self.component(count=8)],
                        ReadKeyMapping.PER_ROW_COMPONENT,
                    )
                ]
            ),
            [[0, 8]],
        )
        self.assertEqual(bytes(self.buffer)[8:16], b"ijklmnop")

    def test_local_validation_and_no_partial_write(self):
        good = self.layout(self.keys, [0, 1])
        with self.assertRaises(ValueError):
            self.store.batch_get_into_layouts([good, None])
        self.assertEqual(bytes(self.buffer), bytes(64))
        for mapping in (True, 0, "PER_ROW"):
            with self.assertRaises(TypeError):
                self.layout(self.keys, [0, 1], mapping=mapping)
        with self.assertRaises(ValueError):
            self.layout(self.keys, [-1, 0])
        with self.assertRaises(ValueError):
            self.layout(self.keys, [0])
        with self.assertRaises(OverflowError):
            self.component(offset=2**64)
        with self.assertRaises(OverflowError):
            self.layout(self.keys, [0, 2**63])
        with self.assertRaises(OverflowError):
            self.layout([self.keys[0]], [0], [self.component(count=2**31)])

    def test_component_integer_validation(self):
        fields = dict(
            destination_base=self.base,
            destination_stride=8,
            byte_count=8,
            source_offset=0,
        )
        size_t_limit = 1 << (ctypes.sizeof(ctypes.c_size_t) * 8)
        for field in fields:
            for value, error in (
                (-1, ValueError),
                (True, TypeError),
                (1.5, TypeError),
                ("8", TypeError),
                (size_t_limit, OverflowError),
            ):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(error):
                        ReadComponent(**{**fields, field: value})
        # These fields fit size_t individually, but their range ends do not.
        for field in ("destination_base", "source_offset"):
            with self.subTest(field=field, value=size_t_limit - 1):
                with self.assertRaises(OverflowError):
                    ReadComponent(**{**fields, field: size_t_limit - 1})

    def test_descriptor_constructors_require_keyword_arguments(self):
        with self.assertRaises(TypeError):
            ReadComponent(self.base, 8, 8)
        with self.assertRaises(TypeError):
            ReadLayout(self.keys, [0, 1], ReadKeyMapping.PER_ROW, [self.component()])

    def test_zero_byte_destination_and_source_validation(self):
        for mapping in (ReadKeyMapping.PER_ROW, ReadKeyMapping.PER_ROW_COMPONENT):
            with self.assertRaisesRegex(ValueError, "null destination"):
                self.layout(
                    [self.keys[0]],
                    [0],
                    [self.component(base=0, count=0)],
                    mapping,
                )
        # A valid destination is still required when no bytes are copied.
        self.assertEqual(
            self.store.batch_get_into_layouts(
                [self.layout([self.keys[0]], [0], [self.component(count=0, offset=8)])]
            ),
            [[0]],
        )
        # Zero-byte entries must not bypass source bounds validation.
        results = self.store.batch_get_into_layouts(
            [self.layout([self.keys[0]], [0], [self.component(count=0, offset=9)])]
        )
        self.assertLess(results[0][0], 0)
        self.assertEqual(bytes(self.buffer), bytes(64))

    def test_empty_and_missing_session(self):
        self.assertEqual(self.store.batch_get_into_layouts([]), [])
        skipped = self.layout(["not-present"], [], [])
        no_rows = self.layout([], [])
        self.assertEqual(
            self.store.batch_get_into_layouts([skipped, no_rows]), [[], []]
        )
        results = self.store.batch_get_into_layouts(
            [
                skipped,
                self.layout(["not-present"], [0], [self.component(count=0)]),
                no_rows,
                self.layout(self.keys, [7, 2]),
                skipped,
            ]
        )
        self.assertEqual(len(results), 5)
        self.assertEqual(results[0], [])
        self.assertLess(results[1][0], 0)
        self.assertEqual(results[2:], [[], [8, 8], []])

    def test_partial_error_preserves_valid_destination(self):
        good = self.layout([self.keys[0]], [7])
        bad = self.layout([self.keys[1]], [2], [self.component(offset=8)])
        for bad_first in (False, True):
            with self.subTest(bad_first=bad_first):
                self.buffer[:] = bytes(64)
                layouts = [bad, good] if bad_first else [good, bad]
                bad_index = 0 if bad_first else 1
                results = self.store.batch_get_into_layouts(layouts)
                self.assertEqual(len(results), 2)
                self.assertLess(results[bad_index][0], 0)
                self.assertEqual(results[1 - bad_index], [8])
                self.assertEqual(bytes(self.buffer)[56:64], b"abcdefgh")
                self.assertEqual(bytes(self.buffer)[:56], bytes(56))

    def test_partial_error_within_layout(self):
        missing = f"layout-missing-{uuid4().hex}"
        results = self.store.batch_get_into_layouts(
            [self.layout([self.keys[0], missing], [7, 2])]
        )
        self.assertEqual(len(results[0]), 2)
        self.assertEqual(results[0][0], 8)
        self.assertLess(results[0][1], 0)
        self.assertEqual(bytes(self.buffer)[56:64], b"abcdefgh")
        self.assertEqual(bytes(self.buffer)[:56], bytes(56))

    def test_source_bounds_failure(self):
        results = self.store.batch_get_into_layouts(
            [self.layout(self.keys, [0, 1], [self.component(offset=4)])]
        )
        self.assertEqual(len(results[0]), 2)
        self.assertTrue(all(rc < 0 for rc in results[0]))


if __name__ == "__main__":
    unittest.main()
