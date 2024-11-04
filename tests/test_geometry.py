# from geoalchemy2 import Geometry
# from geoalchemy2.shape import from_shape
import unittest
from pyobvector import *
from sqlalchemy import Column, Integer, Index, JSON, String, text
from sqlalchemy import func
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class ObVecGeoTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ObVecClient()

    def test_point(self):
        test_collection_name = "ob_point"
        self.client.drop_table_if_exist(test_collection_name)

        cols = [
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("name", String(64)),
            Column("geo", POINT(srid=4326), nullable=False)
        ]

        geo_index = [
            Index("gidx", "geo")
        ]

        self.client.create_table(
            table_name=test_collection_name,
            columns=cols,
            indexes=geo_index
        )

        data = [
            {"name": "A", "geo": func.ST_GeomFromText(POINT.to_db((39.9289, 116.3883)), 4326)},
            {"name": "B", "geo": func.ST_GeomFromText(POINT.to_db((39.9145, 116.4002)), 4326)},
            {"name": "C", "geo": func.ST_GeomFromText(POINT.to_db((39.9040, 116.4053)), 4326)},
        ]
        self.client.insert(test_collection_name, data=data)