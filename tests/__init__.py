import unittest
from pyobvector.client import ObVecClient, VecIndexType, IndexParam
from pyobvector.schema import VECTOR, VectorIndex
from sqlalchemy import Column, Integer, Table
from sqlalchemy.sql import func
from sqlalchemy.exc import NoSuchTableError


class ObVecClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ObVecClient(echo=True)


if __name__ == "__main__":
    unittest.main()
