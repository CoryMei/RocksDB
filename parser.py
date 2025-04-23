#!/usr/bin/env python3
"""
RocksDB SST file parser for format_version=5.
"""

import sys

from helpers import BLOCK_TRAILER_SIZE
from impl import parse_footer, parse_index, parse_data


def parse_file(filename: str, key: str) -> str | None:
    # Read the file as bytes.
    with open(filename, "rb") as f:
        data = f.read()

    meta_handle, index_handle = parse_footer(data)
    print("Metaindex:", meta_handle)
    print("Index:", index_handle)

    index_block = data[
        index_handle.offset : index_handle.offset
        + index_handle.size
        + BLOCK_TRAILER_SIZE
    ]
    data_handle = parse_index(index_block, key)

    # Key not found
    if data_handle is None:
        return None

    print("Data block:", data_handle)

    data_block = data[
        data_handle.offset : data_handle.offset + data_handle.size + BLOCK_TRAILER_SIZE
    ]

    return parse_data(data_block, key)


def main():
    if len(sys.argv) != 3:
        print("Usage: parser.py <filename> <key>")
        sys.exit(1)

    filename = sys.argv[1]
    key = sys.argv[2]
    value = parse_file(filename, key)
    if value:
        print(f"Found key '{key}': {value}")
    else:
        print(f"Key '{key}' not found.")


if __name__ == "__main__":
    main()
