"""Check L4's additive API and old Store callers against an installed wheel.

python3 mooncake-store/tests/e2e/test_store_compatibility.py
MOONCAKE_MASTER=host:port additionally enables isolated TCP put/get cases.
"""

import ctypes
import os
import unittest
from uuid import uuid4

from mooncake.store import MooncakeDistributedStore, ReplicateConfig


class StoreCapabilitiesTest(unittest.TestCase):
    def test_query_before_setup_requires_no_master_and_returns_independent_dicts(self):
        store = MooncakeDistributedStore()
        capabilities = store.get_capabilities()
        self.assertEqual(capabilities["api_version"], 1)
        self.assertEqual(capabilities["scope"], "local_package")
        self.assertEqual(capabilities["client_mode"], "embedded")
        self.assertTrue(capabilities["rpc_protocol_version"].endswith("-dfs-epoch-1"))
        self.assertEqual(
            capabilities["features"],
            dict.fromkeys(
                (
                    "dfs_health",
                    "dfs_prefetch",
                    "read_plan",
                    "read_layout",
                    "service_metrics",
                    "client_http_auto_port",
                ),
                True,
            ),
        )
        self.assertIn("enable_dfs_prefetch", capabilities["setup_parameters"])
        self.assertIn("tenant_id", capabilities["setup_parameters"])
        capabilities["features"]["read_plan"] = False
        capabilities["setup_parameters"].clear()
        self.assertTrue(store.get_capabilities()["features"]["read_plan"])
        self.assertIn("tenant_id", store.get_capabilities()["setup_parameters"])

    def test_community_and_l3_entry_points_are_retained(self):
        store = MooncakeDistributedStore()
        for name in (
            "setup",
            "setup_dummy",
            "put",
            "get",
            "batch_is_exist",
            "batch_get_session_start",
            "batch_get_into_multi_buffer_ranges",
            "batch_get_session_end",
            "batch_get_into_layouts",
            "batch_put_session_start",
            "batch_put_from_multi_buffer_ranges",
            "batch_put_session_end",
            "batch_put_session_revoke",
        ):
            with self.subTest(method=name):
                self.assertTrue(callable(getattr(store, name)))
        config = ReplicateConfig()
        self.assertEqual(config.dfs_replica_num, 0)


@unittest.skipUnless(
    os.environ.get("MOONCAKE_MASTER"), "set MOONCAKE_MASTER for native I/O"
)
class OldStoreCallerTest(unittest.TestCase):
    def test_old_and_new_setup_shapes_preserve_registered_range_reads(self):
        for shape in (
            "community_positional",
            "community_keywords",
            "community_dict",
            "l4",
        ):
            with self.subTest(setup=shape):
                store = MooncakeDistributedStore()
                args = (
                    "127.0.0.1:0",
                    "P2PHANDSHAKE",
                    64 * 1024**2,
                    16 * 1024**2,
                    "tcp",
                    "",
                    os.environ["MOONCAKE_MASTER"],
                )
                kwargs = dict(
                    zip(store.get_capabilities()["setup_parameters"][:7], args)
                )
                if shape == "community_positional":
                    code = store.setup(*args)
                elif shape == "community_keywords":
                    code = store.setup(
                        **kwargs, tenant_id="default", enable_ssd_offload=False
                    )
                elif shape == "community_dict":
                    code = store.setup(kwargs)
                else:
                    code = store.setup(*args, enable_dfs_prefetch=False)
                self.assertEqual(code, 0)
                key = "compat-" + uuid4().hex
                buffer = (ctypes.c_ubyte * 4)()
                address = ctypes.addressof(buffer)
                session_started = False
                registered = False
                try:
                    self.assertEqual(store.put(key, b"abcdefgh"), 0)
                    self.assertEqual(store.get(key), b"abcdefgh")
                    self.assertEqual(store.register_buffer(address, 4), 0)
                    registered = True
                    result = list(store.batch_get_session_start([key]))
                    session_started = True
                    self.assertEqual(result, [0])
                    self.assertEqual(
                        store.batch_get_into_multi_buffer_ranges(
                            [key], [[address]], [[4]], [[2]]
                        ),
                        [4],
                    )
                    self.assertEqual(bytes(buffer), b"cdef")
                finally:
                    if session_started:
                        store.batch_get_session_end([key])
                    if registered:
                        store.unregister_buffer(address)
                    store.remove(key)
                    store.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
