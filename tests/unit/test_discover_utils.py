import pytest

from tap_mysql.discover_utils import Column, schema_for_column


def _json_column():
    return Column(
        table_schema='db',
        table_name='t',
        column_name='val',
        data_type='json',
        character_maximum_length=None,
        numeric_precision=None,
        numeric_scale=None,
        column_type='json',
        column_key='')


def test_json_column_defaults_to_object_type():
    schema = schema_for_column(_json_column())
    assert schema.type == ['null', 'object']


def test_json_column_honors_json_as_type_object():
    schema = schema_for_column(_json_column(), json_as_type='object')
    assert schema.type == ['null', 'object']


def test_json_column_honors_json_as_type_string():
    schema = schema_for_column(_json_column(), json_as_type='string')
    assert schema.type == ['null', 'string']


def test_json_column_rejects_invalid_json_as_type():
    with pytest.raises(ValueError):
        schema_for_column(_json_column(), json_as_type='array')
