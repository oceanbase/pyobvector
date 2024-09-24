"""A module to specify vector index parameters for MilvusLikeClient"""
from enum import Enum
from typing import Union

class VecIndexType(Enum):
    """Vector index algorithm type"""
    HNSW = 0
    # IVFFLAT = 1


class IndexParam:
    """Vector index parameters.
    
    Attributes:
    index_name (string) : vector index name
    field_name (string) : vector index built on which field
    index_type (VecIndexType) :
        vector index algorithms (Only HNSW supported)
    kwargs : 
        vector index parameters for different algorithms
    """
    HNSW_DEFAULT_M = 16
    HNSW_DEFAULT_EF_CONSTRUCTION = 200
    HNSW_DEFAULT_EF_SEARCH = 40
    OCEANBASE_DEFAULT_ALGO_LIB = 'vsag'

    def __init__(
        self, index_name: str, field_name: str, index_type: Union[VecIndexType, str], **kwargs
    ):
        self.index_name = index_name
        self.field_name = field_name
        self.index_type = index_type
        self.index_type = self._get_vector_index_type_str()
        self.kwargs = kwargs

    def _get_vector_index_type_str(self):
        """Parse vector index type to string."""
        if isinstance(self.index_type, VecIndexType):
            if self.index_type == VecIndexType.HNSW:
                return "hnsw"
            # elif self.index_type == VecIndexType.IVFFLAT:
            #     return "ivfflat"
            raise ValueError(f"unsupported vector index type: {self.index_type}")
        assert isinstance(self.index_type, str)
        if self.index_type.lower() == "hnsw":
            return "hnsw"
        raise ValueError(f"unsupported vector index type: {self.index_type}")

    def _parse_kwargs(self):
        ob_params = {}
        # handle metric_type
        if 'metric_type' in self.kwargs:
            ob_params['distance'] = self.kwargs['metric_type']
        elif self.index_type == "hnsw":
            ob_params['distance'] = 'l2'
        else:
            raise ValueError(f"unsupported vector index type: {self.index_type}")
        # handle param
        if 'params' in self.kwargs:
            for k, v in self.kwargs['params'].items():
                if k == 'M':
                    ob_params['m'] = v
                elif k == 'efConstruction':
                    ob_params['ef_construction'] = v
                elif k == 'efSearch':
                    ob_params['ef_search'] = v
                else:
                    ob_params[k] = v
        elif self.index_type == "hnsw":
            ob_params['m'] = IndexParam.HNSW_DEFAULT_M
            ob_params['ef_construction'] = IndexParam.HNSW_DEFAULT_EF_CONSTRUCTION
            ob_params['ef_search'] = IndexParam.HNSW_DEFAULT_EF_SEARCH
        else:
            raise ValueError(f"unsupported vector index type: {self.index_type}")
        # Append OceanBase parameters.
        ob_params['lib'] = IndexParam.OCEANBASE_DEFAULT_ALGO_LIB
        return ob_params

    def param_str(self):
        """Parse vector index parameters to string."""
        ob_param = self._parse_kwargs()
        partial_str = ",".join([f"{k}={v}" for k, v in ob_param.items()])
        if len(partial_str) > 0:
            partial_str += ","
        partial_str += f"type={self.index_type}"
        return partial_str

    def __iter__(self):
        yield "field_name", self.field_name
        if self.index_type:
            yield "index_type", self.index_type
        yield "index_name", self.index_name
        yield from self.kwargs.items()

    def __str__(self):
        return str(dict(self))

    def __eq__(self, other: None):
        if isinstance(other, self.__class__):
            return dict(self) == dict(other)

        if isinstance(other, dict):
            return dict(self) == other
        return False


class IndexParams:
    """Vector index parameters for MilvusLikeClient"""
    def __init__(self):
        self._indexes = {}

    def add_index(
        self, field_name: str, index_type: VecIndexType, index_name: str, **kwargs
    ):
        """Add `IndexParam` to `IndexParams`
        
        Args:
        :param field_name (string) : vector index built on which field
        :param index_type (VecIndexType) :
                vector index algorithms (Only HNSW supported)
        :param index_name (string) : vector index name
        """
        index_param = IndexParam(index_name, field_name, index_type, **kwargs)
        pair_key = (field_name, index_name)
        self._indexes[pair_key] = index_param

    def __iter__(self):
        yield from self._indexes.values()

    def __str__(self):
        return str(list(self))
