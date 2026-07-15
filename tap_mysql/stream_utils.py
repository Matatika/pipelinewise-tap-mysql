# pylint: disable=missing-docstring,too-many-locals
import sys
from decimal import Decimal

import orjson
import singer


def orjson_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f'Object of type {type(obj)} is not JSON serializable')


def write_message(message: singer.Message):
    sys.stdout.write(orjson.dumps(message.to_dict(), default=orjson_default).decode() + '\n')
    sys.stdout.flush()


def write_schema_message(catalog_entry: singer.CatalogEntry, bookmark_properties: list[str] | None = None):
    if bookmark_properties is None:
        bookmark_properties = []

    key_properties = get_key_properties(catalog_entry)

    write_message(
        singer.SchemaMessage(
            stream=catalog_entry.tap_stream_id,
            schema=catalog_entry.schema.to_dict(),
            key_properties=key_properties,
            bookmark_properties=bookmark_properties,
        )
    )


def get_key_properties(catalog_entry: singer.CatalogEntry):
    stream_metadata = catalog_entry.metadata.root
    return stream_metadata.view_key_properties if stream_metadata.is_view else stream_metadata.table_key_properties
