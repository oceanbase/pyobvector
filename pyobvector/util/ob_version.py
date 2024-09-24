"""OceanBase cluster version module."""
import copy
from typing import List


class ObVersion:
    """The class to describe OceanBase cluster version.

    Attributes:
    version_nums (List[int]) : version number of OceanBase cluster. For example, '4.3.3.0'
    """
    def __init__(self, version_nums: List[int]):
        self.version_nums = copy.deepcopy(version_nums)

    @classmethod
    def from_db_version_string(cls, version: str):
        """Construct ObVersion with a version string.

        Args:
            version: a string of 4 numbers separated by '.'
        """
        return cls([int(version_num) for version_num in version.split(".")])

    @classmethod
    def from_db_version_nums(
        cls, main_ver, sub_ver1: int, sub_ver2: int, sub_ver3: int
    ):
        """Construct ObVersion with 4 version numbers.

        Args:
            main_ver: main version
            sub_ver1: first subversion
            sub_ver2: second subversion
            sub_ver3: third subversion
        """
        return cls([main_ver, sub_ver1, sub_ver2, sub_ver3])

    def __lt__(self, other):
        if len(self.version_nums) != len(other.version_nums):
            raise ValueError("version num list length is not equal")
        idx, ilen = 0, len(self.version_nums)
        while idx < ilen:
            if self.version_nums[idx] < other.version_nums[idx]:
                return True
            if self.version_nums[idx] > other.version_nums[idx]:
                return False
            idx += 1
        return False
