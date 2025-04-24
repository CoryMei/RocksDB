# RocksDB SST File Parser

This is a Python implementation of a parser for RocksDB Sorted String Table (SST) files with format_version=5. The parser can locate and retrieve values associated with specific keys within an SST file.

## Overview

RocksDB is an open-source key-value store built by Facebook, based on Google's LevelDB. SST files are a core component of RocksDB, storing sorted key-value pairs in a structured format optimized for both flash storage and range queries.

This parser implements the logic for:
- Parsing the file footer to locate index blocks
- Parsing index blocks to find data blocks
- Parsing data blocks to extract values for given keys

## File Structure

The project consists of the following files:

- `helpers.py`: Contains utility functions and constants for decoding varints, handling block footers, etc.
- `impl.py`: Core implementation of the parser logic
- `parser.py`: Driver code to execute the parser on SST files

## How It Works

The parser follows the RocksDB SST file format:

1. **Footer Parsing**: Reads the last 53 bytes of the file to verify the magic number and version, then extracts handles to the metaindex and index blocks.

2. **Index Block Parsing**: Scans through the index block to find the data block that should contain the requested key. Index blocks may use delta compression for both keys and values.

3. **Data Block Parsing**: Scans through the data block to find the exact key-value pair. Data blocks also use delta compression for keys and include internal footers.

## Usage

To use the parser, run:

```
python3 parser.py <sst_file> <key>
```

Example:
```
python3 parser.py basic.sst 00000000
```

## Testing

Two test files are provided:
- `basic.sst`: Contains 3000 key-value pairs (00000000 to 00002999) without delta compression in the index block
- `delta.sst`: Contains the same data but uses delta compression in the index block

Expected output for key "00000000" in basic.sst:
```
Metaindex: BlockHandle(offset=63876, size=34)
Index: BlockHandle(offset=62643, size=301)
Data block: BlockHandle(offset=0, size=4085)
Found key '00000000': 00000000
```

## Implementation Details

### Key Features

- **Varint Decoding**: Uses variable-width integer encoding to efficiently store values
- **Delta Compression**: Optimizes storage by only storing differences between consecutive keys
- **Block Structure**: Handles the complex structure of SST files including block trailers, footers, and restart arrays

### Important Notes

- The data block keys include an 8-byte internal footer (value type + sequence number) that must be excluded when comparing keys
- Index blocks use entry keys that are >= the last key in the corresponding data block and < the first key in the next block
- SST files are designed for efficient range queries and optimized for flash storage

## References

This implementation is based on the RocksDB file format specification and class materials on Log-structured Merge Trees (LSM-Trees).