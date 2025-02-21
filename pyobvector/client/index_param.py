"""A module to specify vector index parameters for MilvusLikeClient"""
from enum import Enum
from typing import Union

class VecIndexType(Enum):
    """Vector index algorithm type"""
    HNSW = 0
    HNSW_SQ = 1
    IVFFLAT = 2
    IVFSQ = 3
    IVFPQ = 4


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
    HNSW_ALGO_NAME = "hnsw"
    HNSW_SQ_ALGO_NAME = "hnsw_sq"
    IVFFLAT_ALGO_NAME = "ivf_flat"
    IVFSQ_ALGO_NAME = "ivf_sq8"
    IVFPQ_ALGO_NAME = "ivf_pq"

    def __init__(
        self, index_name: str, field_name: str, index_type: Union[VecIndexType, str], **kwargs
    ):
        self.index_name = index_name
        self.field_name = field_name
        self.index_type = index_type
        self.index_type = self._get_vector_index_type_str()
        self.kwargs = kwargs

    def is_index_type_hnsw_serial(self):
        return self.index_type in [
            IndexParam.HNSW_ALGO_NAME, IndexParam.HNSW_SQ_ALGO_NAME
        ]
    
    def is_index_type_ivf_serial(self):
        return self.index_type in [
            IndexParam.IVFFLAT_ALGO_NAME,
            IndexParam.IVFSQ_ALGO_NAME,
            IndexParam.IVFPQ_ALGO_NAME,
        ]
    
    def is_index_type_product_quantization(self):
        return self.index_type in [
            IndexParam.IVFPQ_ALGO_NAME,
        ]

    def _get_vector_index_type_str(self):
        """Parse vector index type to string."""
        if isinstance(self.index_type, VecIndexType):
            if self.index_type == VecIndexType.HNSW:
                return IndexParam.HNSW_ALGO_NAME
            elif self.index_type == VecIndexType.HNSW_SQ:
                return IndexParam.HNSW_SQ_ALGO_NAME
            elif self.index_type == VecIndexType.IVFFLAT:
                return IndexParam.IVFFLAT_ALGO_NAME
            elif self.index_type == VecIndexType.IVFSQ:
                return IndexParam.IVFSQ_ALGO_NAME
            elif self.index_type == VecIndexType.IVFPQ:
                return IndexParam.IVFPQ_ALGO_NAME
            raise ValueError(f"unsupported vector index type: {self.index_type}")
        assert isinstance(self.index_type, str)
        index_type = self.index_type.lower()
        if index_type not in [
            IndexParam.HNSW_ALGO_NAME,
            IndexParam.HNSW_SQ_ALGO_NAME,
            IndexParam.IVFFLAT_ALGO_NAME,
            IndexParam.IVFSQ_ALGO_NAME,
            IndexParam.IVFPQ_ALGO_NAME,
        ]:
            raise ValueError(f"unsupported vector index type: {self.index_type}")
        return index_type

    def _parse_kwargs(self):
        ob_params = {}
        # handle lib
        if self.is_index_type_hnsw_serial():
            ob_params['lib'] = 'vsag'
        else:
            ob_params['lib'] = 'OB'
        # handle metric_type
        ob_params['distance'] = "l2"
        if 'metric_type' in self.kwargs:
            ob_params['distance'] = self.kwargs['metric_type']
        # handle param
        if self.is_index_type_ivf_serial():
            if (self.is_index_type_product_quantization() and
                'params' not in self.kwargs):
                raise ValueError('params must be configured for IVF index type')
            
            if 'params' not in self.kwargs:
                params = {}
            else:
                params = self.kwargs['params']
            
            if self.is_index_type_product_quantization():
                if 'm' not in params:
                    raise ValueError('m must be configured for IVFSQ or IVFPQ')
                ob_params['m'] = params['m']
            if 'nlist' in params:
                ob_params['nlist'] = params['nlist']
            if 'samples_per_nlist' in params:
                ob_params['samples_per_nlist'] = params['samples_per_nlist']

        if self.is_index_type_hnsw_serial():
            if 'params' in self.kwargs:
                params = self.kwargs['params']
                if 'M' in params:
                    ob_params['m'] = params['M']
                if 'efConstruction' in params:
                    ob_params['ef_construction'] = params['efConstruction']
                if 'efSearch' in params:
                    ob_params['ef_search'] = params['efSearch']
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
