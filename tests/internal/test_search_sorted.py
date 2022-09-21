import numpy as np
import pyarrow as pa

from daft.internal.kernels.search_sorted import search_sorted_chunked_array


def test_int_array() -> None:
    keys = np.random.randint(0, 100, 1000)
    data = np.arange(100)
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    pa_keys = pa.chunked_array([keys])
    pa_result = search_sorted_chunked_array(pa_data, pa_keys)
    assert np.all(result == pa_result.to_numpy())


def test_int_array_with_nulls() -> None:
    keys = np.random.randint(0, 100, 1000)
    data = np.arange(100)
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([data])
    pa_keys = pa.chunked_array([pa.chunked_array([keys] + [[None] * 10] + [keys]).combine_chunks()])
    pa_result = search_sorted_chunked_array(pa_data, pa_keys)
    assert np.all(result == pa_result[:1000].to_numpy())
    assert np.all(result == pa_result[1000 + 10 :].to_numpy())
    assert pa_result[1000 : 1000 + 10].null_count == 10


def test_string_array() -> None:
    keys = np.array([str(i) for i in range(100)])
    data = np.array([str(i) for i in range(10)])
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([[str(i) for i in range(10)]])
    pa_keys = pa.chunked_array([[str(i) for i in range(100)]])
    pa_result = search_sorted_chunked_array(pa_data, pa_keys)
    assert np.all(result == pa_result.to_numpy())


def test_string_array_with_nulls() -> None:
    keys = np.array([str(i) for i in range(100)])
    data = np.array([str(i) for i in range(10)])
    result = np.searchsorted(data, keys)
    pa_data = pa.chunked_array([[str(i) for i in range(10)]])
    pa_keys = pa.chunked_array([[str(i) for i in range(100)] + [None] * 10 + [str(i) for i in range(100)]])
    pa_result = search_sorted_chunked_array(pa_data, pa_keys)
    assert np.all(result == pa_result[:100].to_numpy())
    assert np.all(result == pa_result[100 + 10 :].to_numpy())
    assert pa_result[100 : 100 + 10].null_count == 10
