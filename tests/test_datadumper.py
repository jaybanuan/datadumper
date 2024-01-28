import datadumper as dd

def test_dataframe():
    data_dumper = dd.DataFrameDumper(output_dir='tmp/df')

    import pandas as pd
    import numpy as np

    data = pd.DataFrame(np.arange(12).reshape(3, 4))
    data_dumper.dump(data)
    data_dumper.dump(data)


def test_json():
    data_dumper = dd.JsonDumper(output_dir='tmp/json')

    data = {
        'a': 'A',
        'b': 'B'
    }

    data_dumper.dump(data)
    data_dumper.dump(data)



def test_decorator_class():
    dd.datadump.set_dumper(dict, dd.JsonDumper(output_dir='tmp/json'))

    @dd.datadump
    def func(message: str) -> dict:
        return {'message': message}

    func('hello')
