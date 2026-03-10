"""
Tests for embedded SeekDB via ObClient/ObVecClient (path= or pyseekdb_client=).

Requires optional dependency: pip install pyobvector[pyseekdb]
Tests are skipped when pyseekdb is not installed or when pylibseekdb (embedded runtime) is not available.
"""

import tempfile
import unittest
from pathlib import Path

try:
    import pyseekdb  # noqa: F401

    PYSEEKDB_AVAILABLE = True
except ImportError:
    PYSEEKDB_AVAILABLE = False

try:
    import pylibseekdb  # noqa: F401

    PYLIBSEEKDB_AVAILABLE = True
except ImportError:
    PYLIBSEEKDB_AVAILABLE = False


def _skip_if_no_embedded():
    """Skip if pyseekdb or pylibseekdb not available (no embedded SeekDB)."""
    if not PYSEEKDB_AVAILABLE:
        raise unittest.SkipTest(
            "pyseekdb not installed; run: pip install pyobvector[pyseekdb]"
        )
    if not PYLIBSEEKDB_AVAILABLE:
        raise unittest.SkipTest("pylibseekdb not available (embedded SeekDB runtime)")


@unittest.skipIf(
    not PYSEEKDB_AVAILABLE,
    "pyseekdb not installed; run: pip install pyobvector[pyseekdb]",
)
class TestSeekdbEmbeddedConnection(unittest.TestCase):
    """Test ObClient/ObVecClient with embedded SeekDB (path= or pyseekdb_client=)."""

    def setUp(self) -> None:
        _skip_if_no_embedded()
        self.tmpdir = tempfile.mkdtemp(prefix="pyobvector_seekdb_")
        self.db_path = str(Path(self.tmpdir) / "seekdb_data")
        Path(self.db_path).mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        import shutil

        if hasattr(self, "tmpdir") and Path(self.tmpdir).exists():
            try:
                shutil.rmtree(self.tmpdir, ignore_errors=True)
            except Exception:
                pass

    def test_seekdb_remote_client_path_returns_ob_vec_client(self):
        from pyobvector import SeekdbRemoteClient, ObVecClient
        from pyobvector.client.ob_client import ObClient

        client = SeekdbRemoteClient(path=self.db_path, database="test")
        self.assertIsInstance(client, ObVecClient)
        self.assertIsInstance(client, ObClient)
        self.assertIsNotNone(client.engine)
        self.assertIsNotNone(client.ob_version)

    def test_ob_vec_client_path(self):
        from pyobvector import ObVecClient

        client = ObVecClient(path=self.db_path, db_name="test")
        self.assertIsInstance(client, ObVecClient)
        self.assertIsNotNone(client.engine)
        self.assertIsNotNone(client.ob_version)

    def test_ob_vec_client_pyseekdb_client(self):
        import pyseekdb
        from pyobvector import ObVecClient

        pyseekdb_client = pyseekdb.Client(path=self.db_path, database="test")
        client = ObVecClient(pyseekdb_client=pyseekdb_client)
        self.assertIsInstance(client, ObVecClient)
        self.assertIsNotNone(client.engine)

    def test_create_table_insert_drop(self):
        """Test create_table, insert, and drop_table_if_exist via ObClient API."""
        from sqlalchemy import Column, Integer, VARCHAR

        from pyobvector import SeekdbRemoteClient, ObVecClient
        from pyobvector.client.ob_client import ObClient

        client = SeekdbRemoteClient(path=self.db_path, database="test")
        self.assertIsInstance(client, ObVecClient)
        self.assertIsInstance(client, ObClient)

        table_name = "embed_api_table"
        client.drop_table_if_exist(table_name)
        self.assertFalse(client.check_table_exists(table_name))

        client.create_table(
            table_name=table_name,
            columns=[
                Column("id", Integer, primary_key=True),
                Column("name", VARCHAR(64)),
            ],
        )
        self.assertTrue(client.check_table_exists(table_name))

        client.insert(
            table_name,
            data=[
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob"},
            ],
        )

        from sqlalchemy import text

        with client.engine.connect() as conn:
            with conn.begin():
                res = conn.execute(
                    text(f"SELECT id, name FROM `{table_name}` ORDER BY id")
                )
                rows = res.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0], (1, "alice"))
        self.assertEqual(rows[1], (2, "bob"))

        client.drop_table_if_exist(table_name)
        self.assertFalse(client.check_table_exists(table_name))

    def test_vector_table_and_ann_search(self):
        """Test create table with vector index, insert, and ann_search."""
        from sqlalchemy import Column, Integer, VARCHAR

        from pyobvector import (
            SeekdbRemoteClient,
            VECTOR,
            VectorIndex,
            l2_distance,
        )

        client = SeekdbRemoteClient(path=self.db_path, database="test")
        table_name = "embed_vec_table"
        client.drop_table_if_exist(table_name)

        client.create_table(
            table_name=table_name,
            columns=[
                Column("id", Integer, primary_key=True),
                Column("title", VARCHAR(255)),
                Column("vec", VECTOR(3)),
            ],
            indexes=[
                VectorIndex(
                    "vec_idx", "vec", params="distance=l2, type=hnsw, lib=vsag"
                ),
            ],
            mysql_organization="heap",
        )
        client.insert(
            table_name,
            data=[
                {"id": 1, "title": "doc A", "vec": [1.0, 1.0, 1.0]},
                {"id": 2, "title": "doc B", "vec": [1.0, 2.0, 3.0]},
                {"id": 3, "title": "doc C", "vec": [3.0, 2.0, 1.0]},
            ],
        )

        res = client.ann_search(
            table_name=table_name,
            vec_data=[1.0, 2.0, 3.0],
            vec_column_name="vec",
            distance_func=l2_distance,
            with_dist=True,
            topk=3,
            output_column_names=["id", "title"],
        )
        rows = res.fetchall()
        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 3)  # id, title, distance

        client.drop_table_if_exist(table_name)


class TestSeekdbEmbeddedWithoutPyseekdb(unittest.TestCase):
    """Test that using path= without pyseekdb raises a clear ImportError."""

    def test_path_raises_without_pyseekdb(self):
        from pyobvector import SeekdbRemoteClient

        if PYSEEKDB_AVAILABLE:
            self.skipTest("pyseekdb is installed; cannot test ImportError")

        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(ImportError) as ctx:
                SeekdbRemoteClient(path=tmpdir, database="test")
            self.assertIn("pyseekdb", str(ctx.exception).lower())
            self.assertIn("pyobvector[pyseekdb]", str(ctx.exception))
