import inspect
import os
import pathlib

from typing import *


class DumpFunc(Protocol):
    def __call__(self, output_dir: str | os.PathLike[str] , file_name_stem: str, data: Any) -> None: ...


class DataDumper:
    def __init__(self, output_dir: str | os.PathLike[str], dump_func: DumpFunc, append_source: bool = True) -> None:
        self.__counter = 0

        dirs = [output_dir]
        if append_source:
            dirs.append(pathlib.Path(inspect.stack()[1].frame.f_code.co_filename).stem)

        self.__output_dir = pathlib.Path(*dirs)

        self.__dump_func = dump_func


    def dump(self, data: Any, file_name_hint: str = None) -> None:
        try:
            self.__output_dir.mkdir(parents=True, exist_ok=True)

            if file_name_hint is None:
                caller_name = inspect.stack()[1].function
                file_name_hint = caller_name if caller_name != '<module>' else ''

            file_name_stem = f'{self.__counter:02}{"_" if file_name_hint else ""}{file_name_hint}'
                
            self.__dump_func(output_dir=self.__output_dir, file_name_stem=file_name_stem, data=data)
        finally:
            self.__counter += 1


class JsonDumper(DataDumper):
    def __init__(self, output_dir: str | os.PathLike[str], append_source: bool = True) -> None:
        import json

        def __dump_func(output_dir: str | os.PathLike[str] , file_name_stem: str, data: list | dict) -> None:
            with open(pathlib.Path(output_dir, f'{file_name_stem}.json'), 'w') as f:
                json.dump(obj=data, fp=f, ensure_ascii=False, indent=4)

        super().__init__(
            output_dir=output_dir,
            dump_func=__dump_func,
            append_source=append_source
        )


class DataFrameDumper(DataDumper):
    def __init__(self, output_dir: str | os.PathLike[str], append_source: bool = True) -> None:
        import pandas as pd

        def __dump_func(output_dir: str | os.PathLike[str] , file_name_stem: str, data: pd.DataFrame) -> None:
            data.to_csv(
                pathlib.Path(output_dir, f'{file_name_stem}.csv'),
                encoding='cp932'
            )

        super().__init__(
            output_dir=output_dir,
            dump_func=__dump_func,
            append_source=append_source
        )
