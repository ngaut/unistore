
# File format

The file format is different from RocksDB because we need to achieve two goals:

- Separate new values from old values.

  So in most cases, we don't need to touch the old block, this improves the performance when there are many old versions.
  

- Make cached blocks survive compaction.
      
    In RocksDB, a compaction rewrite the SST files, all the cached blocks are invalidated, but it's very common
that a top table's block does not overlap the bottom table, or a bottom table's block does not overlap the top 
table. In this case, the cached blocks don't need to be invalidated. This would save both CPU and I/O resource.
  
## L0 file

```
[cf_file]
[cf_file]
...
[cf_file]
cf_offset(4)
cf_offset(4)
...
number_cf(4)
magic(4)
```

## L1+ file

```
cf_file
```

## cf_file

```
[data_section]
[old_data_section]
[index_section]
[old_index_section]
[properties_section]
[footer](24)
```

## footer
```
old_data_section_offset(4)
index_section_offset(4)
old_index_section_offset(4)
properties_section_offset(4)
compression_type(1)
checksum_type(1)
table_format_version(2)
magic(4)
```

## compression_type
```
no_compression = 0
snappy = 1
zstd = 2
```

## checksum_type
```
no_checksum = 0
crc32Castagnoli = 1
```

## table_format_version
```
1
```

## data_section & old_data_section

```
checksum(4) + [compressed_block]
checksum(4) + [compressed_block]
...
```

## uncompressed_block

```
num_entries(4)
entry_offset(4)
entry_offset(4)
...
common_prefix_len(2)
common_prefix
entries_data
```

## entry
```
key_suffix_len(2)
key_suffix
meta(1)        // bitmap for delete/hasOld
version(8)
old_version(8) // optional if has_old is true
user_meta_len(1)
user_meta
value
```

## index_section & old_index_section
The index_section contains block index and optional auxiliary indices includes bloom filter and hash index.   

```
checksum(4)
num_blocks(4)
block_key_offset(4)
block_key_offset(4)
...
[block_address](16)
[block_address](16)
...
common_prefix_len(2)
common_prefix
block_keys_len(4)
block_keys_data
aux_index_type(2) + aux_index_len(4) + aux_index_data
aux_index_type(2) + aux_index_len(4) + aux_index_data
...
```

## block_address
The `original_fid` and `original_offset` is used as the key of the block cache entry.

```
original_fid(8)
original_offset(4)
current_offset(4)
```

## properties_section
```
checksum(4)
[property_entry]
[property_entry]
...
```

## property_entry
Property can be any key value pairs, already defined properties includes:
- smallest_key
- biggest_key

```
key_len(2)
val_len(4)
entry_data
```
