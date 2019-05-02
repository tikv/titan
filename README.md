# Titan: A RocksDB Plugin to Reduce Write Amplification

Titan is RocksDB Plugin for key-value separation, inspired by 
[WiscKey](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf).
For introduction and design details, see our
[blog post](https://pingcap.com/blog/titan-storage-engine-design-and-implementation/).

## Build and Test
Titan rely on RocksDB source code to build. You need to checkout RocksDB source code locally,
and provide the path to Titan build script.
```
# To build:
mkdir -p build
cd build
cmake .. -DROCKSDB_DIR=<rocksdb_source_dir>
make -j<n>

# Build static lib (i.e. libtitan.a) only:
make titan -j<n>

# Release build:
cmake .. -DROCKSDB_DIR=<rocksdb_source_dir> -DCMAKE_BUILD_TYPE=Release

# Building with sanitizer (e.g. ASAN):
cmake .. -DROCKSDB_DIR=<rocksdb_source_dir> -DWITH_ASAN=ON

# Building with compression libraries (e.g. snappy):
cmake .. -DROCKSDB_DIR=<rocksdb_source_dir> -DWITH_SNAPPY=ON

# Run tests after build. You need to filter tests by "titan" prefix.
ctest -R titan
```
