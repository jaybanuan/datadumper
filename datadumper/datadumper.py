import functools
import inspect
import os
import pathlib
import threading

from typing import Callable, TypeVar, ParamSpec, Protocol, Any, Self


class DumpFunc(Protocol):
    def __call__(self, output_dir: str | os.PathLike[str] , file_name_stem: str, data: Any, dumper_args: dict[str, Any]) -> None: ...


class Counter(Protocol):
    def __call__(self) -> int: ...


class ThreadSafeCounter(Counter):
    def __init__(self) -> None:
        self.__counter = -1
        self.__lock = threading.Lock()
    

    def __call__(self) -> int:
        with self.__lock:
            self.__counter += 1
            return self.__counter


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


    def __init__(self,
        dump_func: DumpFunc,
        counter: Counter,
        output_base_dir: str | os.PathLike[str],
        namespace: str | None = None
    ) -> None:
        path_elements = [output_base_dir]
        if namespace is not None:
            path_elements.append(namespace)
        
        self.__dump_func = dump_func
        self.__counter = counter
        self.__output_dir = pathlib.Path(*path_elements)


    def dump(self, data: Any, file_name_hint: str | None = None, args: dict[str, Any] = {}) -> None:
        self.__output_dir.mkdir(parents=True, exist_ok=True)

        if file_name_hint is None:
            if (stack_frame := DataDumper.__get_caller_stack_frame()) and stack_frame.function != '<module>':
                file_name_hint = stack_frame.function
            else:
                file_name_hint = ''

        file_name_stem = f'{self.__counter():02}{"_" if file_name_hint else ""}{file_name_hint}'

        self.__dump_func(output_dir=self.__output_dir, file_name_stem=file_name_stem, data=data, dumper_args=args)


class DataDumperFactory(Protocol):
    def __call__(self, namespace: str, counter: Counter) -> DataDumper: ...


def create_json_dumper_factory(
    output_base_dir: str | os.PathLike[str],
    default_dumper_args: dict[str, Any] = {'ensure_ascii': False, 'indent': 4}
) -> DataDumperFactory:
    import json

    def dumper_factory(namespace: str, counter: Counter) -> DataDumper:
        def dump_func(output_dir: str | os.PathLike[str] , file_name_stem: str, data: list | dict, dumper_args: dict[str, Any]) -> None:
            with open(pathlib.Path(output_dir, f'{file_name_stem}.json'), 'w') as f:
                json.dump(
                    obj=data,
                    fp=f,
                    **(default_dumper_args | dumper_args)
                )

        return DataDumper(
            dump_func=dump_func,
            counter=counter,
            output_base_dir=output_base_dir,
            namespace=namespace
        )

    return dumper_factory


def create_dataframe_dumper_factory(
    output_base_dir: str | os.PathLike[str],
    default_dumper_args: dict[str, Any] = {'index': False, 'encoding': 'utf-8_sig'}
) -> DataDumperFactory:
    import pandas as pd

    def dumper_factory(namespace: str, counter: Counter) -> DataDumper:
        def dump_func(output_dir: str | os.PathLike[str], file_name_stem: str, data: pd.DataFrame, dumper_args: dict[str, Any]) -> None:
            data.to_csv(
                pathlib.Path(output_dir, f'{file_name_stem}.csv'),
                **(default_dumper_args | dumper_args)
            )

        return DataDumper(
            dump_func=dump_func,
            counter=counter,
            output_base_dir=pathlib.Path(output_base_dir),
            namespace=namespace
        )

    return dumper_factory


R = TypeVar('R')
P = ParamSpec('P')


class DataDumpContext:
    def __init__(self, namespace: str, data_dumper_factory: DataDumperFactory, counter: Counter) -> None:
        self.__namespace = namespace
        self.__data_dumper_factory = data_dumper_factory
        self.__counter = counter
        self.__thread_local = threading.local()


    @property
    def namespace(self) -> str:
        return self.__namespace


    @property
    def data_dumper_factory(self) -> DataDumperFactory:
        return self.__data_dumper_factory


    @property
    def counter(self) -> Counter:
        return self.__counter


    @property
    def dumper(self) -> DataDumper:
        if (not hasattr(self.__thread_local, 'dumper')):
            self.__thread_local.dumper = self.__data_dumper_factory(self.__namespace, self.__counter)
        
        return self.__thread_local.dumper


# decorator class
class DataDump:
    def __init__(self) -> None:
        pass


    def set_context(self, context: DataDumpContext) -> None:
        self.__context = context


    def __call__(self, **dumper_args) -> Callable[[Callable[P, R]], Callable[P, R]]:
        def middle_wrapper(f: Callable[P, R]) -> Callable[P, R]:
            @functools.wraps(f)
            def inner_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                data = f(*args, **kwargs)
                self.__context.dumper.dump(data, f.__name__, dumper_args)
            
                return data
    
            return inner_wrapper
        
        return middle_wrapper


datadump = DataDump()