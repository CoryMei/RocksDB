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
    # Read metaindex block handle
    meta_offset, bytes_read = decode_varint64(data)
    meta_size, more_bytes = decode_varint64(data[bytes_read:])
    bytes_read += more_bytes
    
    # Read index block handle
    index_offset, more_bytes = decode_varint64(data[bytes_read:])
    bytes_read += more_bytes
    index_size, _ = decode_varint64(data[bytes_read:])
    
    # Create and return block handles
    meta_handle = BlockHandle(offset=meta_offset, size=meta_size)
    index_handle = BlockHandle(offset=index_offset, size=index_size)
    
    return meta_handle, index_handle


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
    # Footer is the last 53 bytes of the file
    footer_data = data[-53:]
    
    # 1. Check the magic number (last 8 bytes)
    magic_number = int.from_bytes(footer_data[-8:], byteorder="little")
    if magic_number != MAGIC_NUMBER:
        raise RocksDBFormatError("Magic number does not match")
    
    # 2. Check the version (4 bytes before magic number)
    version = int.from_bytes(footer_data[-12:-8], byteorder="little")
    if version != FORMAT_VERSION:
        raise RocksDBFormatError("Format version does not match")
    
    # 3. Parse the block handles - starting after the checksum type (first byte)
    return parse_footer_handles(footer_data[1:41])


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
    # Get the number of restart entries
    _, num_restarts = parse_block_footer(data[-9:-5])
    
    # Calculate where the entries end
    restart_array_offset = len(data) - BLOCK_TRAILER_SIZE - 4 - (4 * num_restarts)
    
    # Initialize variables for tracking the previous key and block handle
    prev_key = ""
    prev_handle = None
    
    # Start iterating through entries
    offset = 0
    while offset < restart_array_offset:
        # Parse the entry header (shared_size, non_shared_size)
        shared_size, bytes_read = decode_varint32(data[offset:])
        offset += bytes_read
        
        non_shared_size, bytes_read = decode_varint32(data[offset:])
        offset += bytes_read
        
        # Reconstruct the key
        if shared_size == 0:
            # The entire key is stored in this entry
            entry_key = data[offset:offset+non_shared_size].decode("utf-8")
            offset += non_shared_size
            
            # Parse the block handle (not delta-compressed)
            block_offset, bytes_read = decode_varint64(data[offset:])
            offset += bytes_read
            block_size, bytes_read = decode_varint64(data[offset:])
            offset += bytes_read
            
            current_handle = BlockHandle(offset=block_offset, size=block_size)
        else:
            # Key is delta-compressed
            entry_key = prev_key[:shared_size] + data[offset:offset+non_shared_size].decode("utf-8")
            offset += non_shared_size
            
            # Value is delta-compressed
            delta_size, bytes_read = decode_varsignedint64(data[offset:])
            offset += bytes_read
            
            # Compute the new block handle
            block_offset = prev_handle.offset + prev_handle.size + BLOCK_TRAILER_SIZE
            block_size = prev_handle.size + delta_size
            
            current_handle = BlockHandle(offset=block_offset, size=block_size)
        
        # If we find a key that's greater than or equal to our search key,
        # return the current handle
        if entry_key >= key:
            return current_handle
        
        # Update previous key and handle
        prev_key = entry_key
        prev_handle = current_handle
    
    # If we got here and found no larger key, the last entry is our target
    return prev_handle


def parse_data(data: bytes, key: str) -> str | None:
    """
    Parse the data block and extract the value for the given key.
    Returns None if the key is not found.

    Key-value pairs are stored in the data block in the following format:
        shared_size (varint32), non_shared_size (varint32), value_len (varint32)
        key: non_shared_size bytes (includes 8-byte internal footer)
        value (value_len bytes)
    """
    # Get the number of restart entries
    _, num_restarts = parse_block_footer(data[-9:-5])
    
    # Calculate where the entries end (before restart array)
    restart_array_offset = len(data) - BLOCK_TRAILER_SIZE - 4 - (4 * num_restarts)
    
    # Initialize variable for tracking the previous key
    prev_key = ""
    
    # Start iterating through entries
    offset = 0
    while offset < restart_array_offset:
        entry_start = offset
        
        # Parse entry header
        shared_size, bytes_read = decode_varint32(data[offset:])
        offset += bytes_read
        
        non_shared_size, bytes_read = decode_varint32(data[offset:])
        offset += bytes_read
        
        value_length, bytes_read = decode_varint32(data[offset:])
        offset += bytes_read
        
        # The key has an 8-byte internal footer (value type + seq #)
        # Exclude these 8 bytes when reconstructing the actual key
        actual_key_size = non_shared_size - 8
        
        # Reconstruct the key
        if shared_size == 0:
            # No sharing with previous key
            entry_key = data[offset:offset+actual_key_size].decode("utf-8")
        else:
            # Key is delta-compressed
            entry_key = prev_key[:shared_size] + data[offset:offset+actual_key_size].decode("utf-8")
        
        # Skip past key and internal footer (8 bytes)
        offset += non_shared_size
        
        # Check if this is our key
        if entry_key == key:
            # Found our key, extract the value
            value = data[offset:offset+value_length].decode("utf-8")
            return value
        
        # Skip past value
        offset += value_length
        
        # Update previous key for next iteration
        prev_key = entry_key
    
    # Key not found
    return None