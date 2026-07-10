from unittest import TestCase
from unittest.mock import MagicMock, patch

from pyobvector.client.ob_client import ObClient


class TestObClient(TestCase):
    def test_upsert_refreshes_embedded_seekdb_index(self) -> None:
        client = ObClient.__new__(ObClient)
        client.engine = MagicMock()
        client.metadata_obj = MagicMock()
        client._flush_seekdb_index = MagicMock()

        table = MagicMock()
        upsert_statement = MagicMock()
        upsert_statement.values.return_value = upsert_statement

        with (
            patch("pyobvector.client.ob_client.Table", return_value=table),
            patch(
                "pyobvector.client.ob_client.ReplaceStmt",
                return_value=upsert_statement,
            ),
        ):
            client.upsert("test_table", [{"id": "doc-1"}])

        client._flush_seekdb_index.assert_called_once_with()
