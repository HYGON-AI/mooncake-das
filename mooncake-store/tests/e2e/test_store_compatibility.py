"""Check L4's additive API and old Store callers against an installed wheel.

python3 mooncake-store/tests/e2e/test_store_compatibility.py
MOONCAKE_MASTER=host:port additionally enables isolated TCP put/get cases.
"""

import ctypes
import os
import threading
import unittest
from uuid import uuid4

from mooncake.store import MooncakeDistributedStore, ReplicateConfig


class StoreCapabilitiesTest(unittest.TestCase):
    def test_embedded_metrics_snapshot_is_unavailable_before_setup(self):
        self.assertIsNone(MooncakeDistributedStore().get_client_metrics())

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
    def test_snapshot_thread_can_overlap_close(self):
        store = MooncakeDistributedStore()
        self.assertEqual(
            store.setup(
                "127.0.0.1",
                "P2PHANDSHAKE",
                0,
                0,
                "tcp",
                "",
                os.environ["MOONCAKE_MASTER"],
                enable_dfs_prefetch=False,
            ),
            0,
        )
        self.assertIsNotNone(store.get_client_metrics())
        stopped = threading.Event()
        started = threading.Event()
        errors = []

        def report():
            try:
                while not stopped.is_set():
                    store.get_client_metrics()
                    started.set()
            except Exception as error:
                errors.append(error)
            finally:
                started.set()

        reporter = threading.Thread(target=report)
        reporter.start()
        try:
            self.assertTrue(started.wait(timeout=10))
            self.assertEqual(store.close(), 0)
        finally:
            stopped.set()
            reporter.join(timeout=10)
        self.assertFalse(reporter.is_alive())
        self.assertEqual(errors, [])
        self.assertIsNone(store.get_client_metrics())

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
                    zip(
                        (
                            "local_hostname",
                            "metadata_server",
                            "global_segment_size",
                            "local_buffer_size",
                            "protocol",
                            "rdma_devices",
                            "master_server_addr",
                        ),
                        args,
                    )
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

    def test_glm53_l3_direct_calls_without_capability_query(self):
        """Replay the native calls used by maxiaobetter SGLang 35130b2f.

        Use two registered objects and two layers so strided destinations,
        source offsets and multi-buffer packing are checked together. The
        caller deliberately never uses get_capabilities or any L4-only API.
        CPU buffers check the API/data contract; real GPU/RDMA is a separate
        integration run using the same test's configurable transport.
        """
        store = MooncakeDistributedStore()
        protocol = os.environ.get("MOONCAKE_COMPAT_PROTOCOL", "tcp")
        device = os.environ.get("MOONCAKE_COMPAT_DEVICE", "")
        self.assertEqual(
            store.setup(
                os.environ.get("MOONCAKE_COMPAT_HOSTNAME", "127.0.0.1:0"),
                "P2PHANDSHAKE",
                64 * 1024**2,
                16 * 1024**2,
                protocol,
                device,
                os.environ["MOONCAKE_MASTER"],
                None,  # Old SGLang passes its optional shared engine here.
            ),
            0,
        )
        keys = ["glm53-compat-" + uuid4().hex for _ in range(2)]
        source = ctypes.create_string_buffer(b"abcdefghijklmnop", 16)
        destination = (ctypes.c_ubyte * 16)()
        source_ptr = ctypes.addressof(source)
        destination_ptr = ctypes.addressof(destination)
        registered = []
        session_attempted = False
        try:
            for address, size in ((source_ptr, 16), (destination_ptr, 16)):
                self.assertEqual(store.register_buffer(address, size), 0)
                registered.append(address)
            config = ReplicateConfig()
            self.assertEqual(
                list(
                    store.batch_put_from_multi_buffers(
                        keys,
                        [
                            [source_ptr, source_ptr + 4],
                            [source_ptr + 8, source_ptr + 12],
                        ],
                        [[4, 4], [4, 4]],
                        config,
                    )
                ),
                [0, 0],
            )
            self.assertEqual(list(store.batch_is_exist(keys)), [1, 1])
            session_attempted = True
            self.assertEqual(list(store.batch_get_session_start(keys)), [0, 0])
            for layer in range(2):
                self.assertEqual(
                    list(
                        store.batch_get_into_multi_buffer_ranges(
                            keys,
                            [
                                [destination_ptr + layer * 8],
                                [destination_ptr + layer * 8 + 4],
                            ],
                            [[4], [4]],
                            [[layer * 4], [layer * 4]],
                        )
                    ),
                    [4, 4],
                )
            self.assertEqual(bytes(destination), b"abcdijklefghmnop")
        finally:
            if session_attempted:
                store.batch_get_session_end(keys)
            for address in reversed(registered):
                store.unregister_buffer(address)
            for key in keys:
                store.remove(key)
            store.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
