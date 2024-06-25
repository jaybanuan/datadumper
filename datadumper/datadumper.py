import collections.abc
import functools
import json
import os
import pathlib
import threading

from typing import Callable, Protocol, Any, Self

import pandas as pd


class DumpFunc(Protocol):
    def __call__(self, data: Any, filepath: str | os.PathLike[str]) -> None: ...


class PrefixFunc(Protocol):
    def __call__(self) -> str: ...


def dump_none(data: None, filepath: str | os.PathLike[str]) -> None:
    with open(filepath, 'w') as f:
        pass


def dump_data_frame(data: pd.DataFrame, filepath: str | os.PathLike[str]) -> None:
    data.to_csv(
        filepath,
        encoding='utf-8_sig'
    )


def dump_json(data: dict, filepath: str | os.PathLike[str]) -> None:
    with open(filepath, 'w') as f:
        json.dump(
            obj=data,
            fp=f,
            ensure_ascii=False,
            indent=4
        )


class DataDumper(DumpFunc):
    def __init__(self, data_type: type, dump_func: DumpFunc, ext: str | list[str]) -> None:
        self.__data_type = data_type
        self.__dump_func = dump_func
        
        if isinstance(ext, list):
            if not ext:
                raise Exception(f'The argument "extensions" is empty list.')

            self.__ext = ext[0]
            self.__ext_for_check = [extention.lower() for extention in ext]
        elif isinstance(ext, str):
            self.__ext = ext
            self.__ext_for_check = [ext.lower()]
        else:
            raise Exception(f'Unknown type: {type(ext)}')
    
    @property
    def data_type(self) -> type:
        return self.__data_type


    @property
    def dump_func(self) -> DumpFunc:
        return self.__dump_func


    @property
    def ext_for_check(self) -> list[str]:
        return self.__ext_for_check


    def __ensure_filepath(self, filepath: str | os.PathLike[str]) -> str:
        # Ensure libpath.Path instance
        filepath = filepath if isinstance(filepath, pathlib.Path) else pathlib.Path(filepath)

        # Ensure file extension
        suffix = filepath.suffix
        if (not suffix) or (suffix.lower() not in self.__ext_for_check):
            filepath = pathlib.Path(filepath.parent, f'{filepath.stem}{self.__ext}')
        else:
            suffix = ''

        # Ensure parent directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)

        return filepath


    def __call__(self, data: Any, filepath: str | os.PathLike[str]) -> None:
        filepath = self.__ensure_filepath(filepath)

        self.__dump_func(
            data=data,
            filepath=filepath
        )


class DataDump:
    def __init__(self) -> None:
        self.__initialized = False

        self.__rlock = threading.RLock()
        self.__dumpers: dict[type, DataDumper] = {}


    def init(self, output_dir: str | os.PathLike[str], prefix: str | PrefixFunc = None) -> Self:
        if self.__initialized:
            raise Exception('Already initialized.')

        self.__output_dir = output_dir if isinstance(output_dir, pathlib.Path) else pathlib.Path(output_dir)
        self.__prefix = prefix

        self.add_dump_func(type(None),              dump_none,       '.txt')
        self.add_dump_func(pd.DataFrame,            dump_data_frame, '.csv')
        self.add_dump_func(collections.abc.Mapping, dump_json,       '.json')

        self.__initialized = True

        return self


    def add_dump_func(self, data_type: type, dump_func: DumpFunc, ext: str | list[str]) -> None:
        self.__dumpers[data_type] = DataDumper(data_type, dump_func, ext)


    def __call_dumper[T](self, data: T, filepath: pathlib.Path) -> None:
        for data_type, dumper in self.__dumpers.items():
            if isinstance(data, data_type):
                dumper(data, filepath)
                break


    def __call__[**P, R](self, filename: str | None = None) -> Callable[[Callable[P, R]], Callable[P, R]]:
        if not self.__initialized:
            raise Exception('Not initialized yet.')

        def middle_wrapper(f: Callable[P, R]) -> Callable[P, R]:
            @functools.wraps(f)
            def inner_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                data = f(*args, **kwargs)

                with self.__rlock:
                    filepath = pathlib.Path(
                        self.__output_dir,
                        filename if filename is not None else f.__name__
                    )
    
                    if self.__prefix is not None:
                        prefix = ''
                        if isinstance(self.__prefix, str):
                            prefix = self.__prefix
                        elif isinstance(self.__prefix, Callable):
                            prefix = self.__prefix()
    
                        filepath = pathlib.Path(filepath.parent, f'{prefix}{filepath.name}')
    
                    self.__call_dumper(
                        data=data,
                        filepath=filepath
                    )
    
                    return data
    
            return inner_wrapper
        
        return middle_wrapper


def counter_prefix() -> Callable[[], str]:
    counter = 0

    def func() -> str:
        nonlocal counter

        prefix = f'{counter:02}-'
        counter = counter + 1

        return prefix
    
    return func
