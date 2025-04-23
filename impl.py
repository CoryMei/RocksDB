from helpers import (
    BlockHandle,
    BLOCK_TRAILER_SIZE,
    decode_varint32,
    decode_varint64,
    decode_varsignedint64,
    parse_block_footer,
    RocksDBFormatError,
    MAGIC_NUMBER,
    FORMAT_VERSION,
)


def parse_footer_handles(data: bytes) -> tuple[BlockHandle, BlockHandle]:
    """
    Takes in a byte array and returns the metaindex and index block handles.

    A block handle is structured as follows:
        Offset: varint64 (1-10 bytes)
        Size: varint64 (1-10 bytes)
        ---------------------
        Total: 2-20 bytes
    """

    # TODO
    pass


def parse_footer(data: bytes) -> tuple[BlockHandle, BlockHandle]:
    """
    Parse the footer of the SST file and extract the index block offset.
    Format (Version 1-5):
        Checksum type: 1 byte
        Block handles (metaindex + index): 40 bytes (some may be padding)
        Version: 4 bytes
        Magic number: 8 bytes
        ---------------------
        Total: 53 bytes

    We assume that all numbers are little-endian.
    Returns the metaindex and index block handles.
    """

    # 1. Check the magic number

    # 2. Check the version

    # 3. Parse the block handles -- call `parse_footer_handles()` and return the
    # result

    # TODO

    return None, None


def parse_index(data: bytes, key: str) -> BlockHandle | None:
    """
    Parse the index block and find the block handle for the data block the given
    key should be in.

    Index values are block handles to data blocks. For format_version >= 4,
    index values are delta compressed. When shared_size == 0, the value is the
    block handle. Otherwise, the value is a varsignedint64 representing the
    difference in size between this data block and the previous data block.
    This can be used to compute the block handle to this data block.

    Returns None if no corresponding data block is found.

    We assume that `index_value_is_delta_encoded`, `index_type = kBinarySearch`,
    and `index_key_is_user_key` are all true.
    """

    # TODO: Linear scan over the index block.
    return None


def parse_data(data: bytes, key: str) -> str | None:
    """
    Parse the data block and extract the value for the given key.
    Returns None if the key is not found.

    Key-value pairs are stored in the data block in the following format:
        shared_size (varint32), non_shared_size (varint32), value_len (varint32)
        key: non_shared_size bytes
        value_type and seq_no (8 bytes)
        value (value_len bytes)
    """

    # TODO: Linear scan over data block
    return None
