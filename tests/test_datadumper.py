import pandas as pd
import numpy as np
import threading

from typing import Callable

import datadumper as dd


def __start_and_join_thread(funcs: list[Callable]) -> None:
    threads = [threading.Thread(target=func) for func in funcs]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()


def test_json_decorator() -> None:
    dd.datadump.set_context(dd.DataDumpContext(
        namespace=test_json_decorator.__name__,
        data_dumper_factory=dd.create_json_dumper_factory(output_base_dir='./tmp'),
        counter=dd.ThreadSafeCounter()
    ))

    @dd.datadump()
    def func1() -> dict:
        return {
            'aaa': [1, 2, 3],
            'xxx': {
                'yyy': 'zzz'
            }
        }

    @dd.datadump(indent=2)
    def func2() -> list:
        return [10, 20, 30]
    
    __start_and_join_thread([func1, func2])


def test_dataframe_decorator() -> None:
    dd.datadump.set_context(dd.DataDumpContext(
        namespace=test_dataframe_decorator.__name__,
        data_dumper_factory=dd.create_dataframe_dumper_factory(output_base_dir='./tmp'),
        counter=dd.ThreadSafeCounter()
    ))

    @dd.datadump()
    def func1() -> pd.DataFrame:
        return pd.DataFrame(np.arange(3 * 4).reshape(3, 4))

    @dd.datadump(index=True)
    def func2() -> pd.DataFrame:
        return pd.DataFrame(np.arange(4 * 5).reshape(4, 5))
    
    __start_and_join_thread([func1, func2])
