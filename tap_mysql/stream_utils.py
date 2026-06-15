# pylint: disable=missing-docstring,too-many-locals
import sys
from decimal import Decimal

import orjson
import singer
from singer import metadata


def _orjson_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f'Object of type {type(obj)} is not JSON serializable')


def write_message(message):
    sys.stdout.write(orjson.dumps(message.asdict(), default=_orjson_default).decode() + '\n')
    sys.stdout.flush()


def write_schema_message(catalog_entry, bookmark_properties=None):
    if bookmark_properties is None:
        bookmark_properties = []

    key_properties = get_key_properties(catalog_entry)

    write_message(singer.SchemaMessage(
        stream=catalog_entry.stream,
        schema=catalog_entry.schema.to_dict(),
        key_properties=key_properties,
        bookmark_properties=bookmark_properties
    ))


def get_key_properties(catalog_entry):
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    is_view = get_is_view(catalog_entry)

    if is_view:
        key_properties = stream_metadata.get('view-key-properties', [])
    else:
        key_properties = stream_metadata.get('table-key-properties', [])

    return key_properties


def get_is_view(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('is-view')
