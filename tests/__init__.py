import unittest
from pyobvector.client import ObVecClient


class ObVecClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = ObVecClient(echo=True)


if __name__ == "__main__":
    unittest.main()
