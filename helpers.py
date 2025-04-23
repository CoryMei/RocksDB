from dataclasses import dataclass

# Constants

# b'\x88\xe2\x41\xb7\x85\xf4\xcf\xf7' (need to reverse order)
MAGIC_NUMBER = 0x88E241B785F4CFF7

FORMAT_VERSION = 5

VARINT_SHIFT = 7
VARINT_MSB = 1 << VARINT_SHIFT

"""
The block trailer is a 5-byte structure that is present at the end of every
block. It is not included in the block size that is encoded in the block handle.
It contains the following fields:
    Footer type: 1 byte
    Block checksum: 4 bytes
"""
BLOCK_TRAILER_SIZE = 5

"""
The block footer contains 4 bytes that encode the block index type and the
number of restart entries in the block. We can obtain the number of restart
entries by masking out the highest bit.
"""
NUM_RESTART_MASK = (1 << 31) - 1


class RocksDBFormatError(Exception):
    """
    Exception raised for errors when parsing a RocksDB file.
    """

    pass


@dataclass
class BlockHandle:
    offset: int
    size: int


def decode_varint(data: bytes, max_shift: int) -> tuple[int, int]:
    """
    Decode a varint from the given byte array.

    Varints allow encoding unsigned integers with a variable number of bytes.
    The encoding uses the least significant 7 bits of each byte to store the
    number and the most significant bit to indicate if there are more bytes to
    read. Read more: https://protobuf.dev/programming-guides/encoding/#varints

    Returns the decoded value and the number of bytes read. If the value is not
    a valid varint (based on max_shift), raises a RocksDBFormatError.
    """
    value = 0
    shift = 0
    num_bytes = 0

    while shift <= max_shift:
        byte = data[num_bytes]
        num_bytes += 1
        value |= (byte & 0x7F) << shift
        shift += 7

        if not byte & 0x80:
            return value, num_bytes

    raise RocksDBFormatError("Varint is too long.")


def decode_varint32(data: bytes) -> tuple[int, int]:
    """
    Decode a 32-bit varint from the given byte array.
    Returns the decoded value and the number of bytes read. If the value is not
    a valid varint, raises a RocksDBFormatError.
    """
    return decode_varint(data, 27)


def decode_varint64(data: bytes) -> tuple[int, int]:
    """
    Decode a 64-bit varint from the given byte array.
    Returns the decoded value and the number of bytes read. If the value is not
    a valid varint, raises a RocksDBFormatError.
    """
    return decode_varint(data, 63)


def decode_varsignedint64(data: bytes) -> tuple[int, int]:
    """
    Decode a 64-bit varsignedint from the given byte array.
    Uses Zigzag encoding.
    Returns the decoded value and the number of bytes read. If the value is not
    a valid varint, raises a RocksDBFormatError.
    """
    value, num_bytes = decode_varint64(data)
    return (value >> 1) ^ -(value & 1), num_bytes


def parse_block_footer(data: bytes) -> tuple[int, int]:
    """
    Parse the block footer and return the index type and the number of restarts.
    """
    num_restarts = int.from_bytes(data[:4], byteorder="little") & NUM_RESTART_MASK
    # TODO: Parse the index type
    return 0, num_restarts
