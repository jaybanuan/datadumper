import pandas as pd
import numpy as np

import datadumper as dd


save = dd.DataDump(
    output_dir='./output/save',
    prefix=dd.counter_prefix()
)


@save(filename='セーブ')
def DataFrameのテストデータの出力(flag: bool = True) -> pd.DataFrame | None:
    return pd.DataFrame(np.arange(12).reshape(3, 4)) if flag else None


@save(filename='保存.CSV')
def JSONのテストデータの出力(flag: bool = True) -> dict | None:
    data = {
        'one': 1,
        'two': 2,
        'array': [10, 11, 12],
        'dict' : {
            '赤': 'レッド',
            '青': 'ブルー'
        }
    }

    return data if flag else None


def test_datadump() -> None:
    DataFrameのテストデータの出力(True)
    JSONのテストデータの出力(True)
    DataFrameのテストデータの出力(False)
    JSONのテストデータの出力(False)
