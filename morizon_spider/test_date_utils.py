import pytest
from morizon_spider.date_utils import save_obj, load_obj

@pytest.mark.parametrize('data_dict, filename, expected_line1, expected_line2',
    [(
        {'morizon_sale': ['21-02-2019','25-02-2019'],
         'morizon_rent': ['25-05-1999']},
        'tests/test_date_file.txt',
        'morizon_sale:21-02-2019,25-02-2019',
        'morizon_rent:25-05-1999'
    )])
def test_save_obj(data_dict, filename, expected_line1, expected_line2):
    save_obj(data_dict, filename)
    with open(filename, 'rb') as f:
        for line_number, line in enumerate(f.read().split('\n')[:-1]):
            if line.startswith(expected_line1[:12]):
                assert line == expected_line1
            elif line.startswith(expected_line2[:12]):
                assert line == expected_line2
            else: assert False


@pytest.mark.parametrize('filename, expected',
    [(
        'tests/test_load_obj.txt',
        {'morizon_sale': ['21-02-2019','25-02-2019'],
         'morizon_rent': ['25-05-1999']}
    )])
def test_load_obj(filename, expected):
    obj = load_obj(filename)
    assert obj == expected

