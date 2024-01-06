import inspect
import os
import pathlib
import functools

from typing import Callable, TypeVar, ParamSpec, Protocol, Any


class DumpFunc(Protocol):
    def __call__(self, output_dir: str | os.PathLike[str] , file_name_stem: str, data: Any) -> None: ...


class DataDumper:
    @classmethod
    def __get_caller_stack_frame(cls) -> inspect.FrameInfo | None:
        result = None

        stack_frames = inspect.stack()

        top = stack_frames[0]
        for stack_frame in stack_frames[1:]:
            if stack_frame.frame.f_code.co_filename != top.frame.f_code.co_filename:
                result = stack_frame
                break
        
        return result


    def __init__(self, output_dir: str | os.PathLike[str], dump_func: DumpFunc, by_module: bool = True) -> None:
        self.__output_dir = pathlib.Path(output_dir)
        self.__dump_func = dump_func
        self.__by_module = by_module
        self.__counter = 0


    def dump(self, data: Any, file_name_hint: str | None = None) -> None:
        try:
            stack_frame = DataDumper.__get_caller_stack_frame()

            output_dir = self.__output_dir
            if self.__by_module and stack_frame:
                output_dir = pathlib.Path(
                    output_dir,
                    pathlib.Path(stack_frame.frame.f_code.co_filename).stem     # module name
                )

            output_dir.mkdir(parents=True, exist_ok=True)

            if file_name_hint is None:
                if stack_frame and stack_frame.function != '<module>':
                    file_name_hint = stack_frame.function
                else:
                    file_name_hint = ''

            file_name_stem = f'{self.__counter:02}{"_" if file_name_hint else ""}{file_name_hint}'
                
            self.__dump_func(output_dir=output_dir, file_name_stem=file_name_stem, data=data)
        finally:
            self.__counter += 1


class JsonDumper(DataDumper):
    def __init__(self, output_dir: str | os.PathLike[str], by_module: bool = True) -> None:
        import json

        def __dump_func(output_dir: str | os.PathLike[str] , file_name_stem: str, data: list | dict) -> None:
            with open(pathlib.Path(output_dir, f'{file_name_stem}.json'), 'w') as f:
                json.dump(obj=data, fp=f, ensure_ascii=False, indent=4)

        super().__init__(
            output_dir=output_dir,
            dump_func=__dump_func,
            by_module=by_module
        )


class DataFrameDumper(DataDumper):
    def __init__(self, output_dir: str | os.PathLike[str], by_module: bool = True) -> None:
        import pandas as pd

        def __dump_func(output_dir: str | os.PathLike[str] , file_name_stem: str, data: pd.DataFrame) -> None:
            data.to_csv(
                pathlib.Path(output_dir, f'{file_name_stem}.csv'),
                encoding='cp932'
            )

        super().__init__(
            output_dir=output_dir,
            dump_func=__dump_func,
            by_module=by_module
        )


R = TypeVar('R')
P = ParamSpec('P')


# デコレータ
def datadump(dumper: DataDumper) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def datadump_wrapper(f: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(f)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            data = f(*args, **kwargs)
            dumper.dump(data, f.__name__)
        
            return data

        return wrapper
    
    return datadump_wrapper
