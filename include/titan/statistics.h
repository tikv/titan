#pragma once

#include "rocksdb/statistics.h"

namespace rocksdb {
namespace titandb {

std::shared_ptr<Statistics> CreateDBStatistics();

enum TickerType : uint32_t {
  TITAN_NUM_GET = TICKER_ENUM_MAX,
  TITAN_NUM_SEEK,
  TITAN_NUM_NEXT,
  TITAN_NUM_PREV,

  TITAN_BLOB_FILE_NUM_KEYS_WRITTEN,
  TITAN_BLOB_FILE_NUM_KEYS_READ,
  TITAN_BLOB_FILE_BYTES_WRITTEN,
  TITAN_BLOB_FILE_BYTES_READ,
  TITAN_BLOB_FILE_SYNCED,

  TITAN_GC_NUM_FILES,
  TITAN_GC_NUM_NEW_FILES,
  TITAN_GC_NUM_KEYS_OVERWRITTEN,
  TITAN_GC_NUM_KEYS_RELOCATED,
  TITAN_GC_BYTES_OVERWRITTEN,
  TITAN_GC_BYTES_RELOCATED,
  TITAN_GC_BYTES_WRITTEN,
  TITAN_GC_BYTES_READ,

  TITAN_BLOB_CACHE_HIT,
  TITAN_BLOB_CACHE_MISS,

  TITAN_GC_NO_NEED,
  TITAN_GC_REMAIN,

  TITAN_GC_DISCARDABLE,
  TITAN_GC_SAMPLE,
  TITAN_GC_SMALL_FILE,

  TITAN_GC_FAILURE,
  TITAN_GC_SUCCESS,
  TITAN_GC_TRIGGER_NEXT,

  TITAN_TICKER_ENUM_MAX,
};

// The order of items listed in Tickers should be the same as
// the order listed in TickersNameMap
const std::vector<std::pair<TickerType, std::string>> TitanTickersNameMap = {
    {TITAN_NUM_GET, "titandb.num.get"},
    {TITAN_NUM_SEEK, "titandb.num.seek"},
    {TITAN_NUM_NEXT, "titandb.num.next"},
    {TITAN_NUM_PREV, "titandb.num.prev"},
    {TITAN_BLOB_FILE_NUM_KEYS_WRITTEN, "titandb.blob.file.num.keys.written"},
    {TITAN_BLOB_FILE_NUM_KEYS_READ, "titandb.blob.file.num.keys.read"},
    {TITAN_BLOB_FILE_BYTES_WRITTEN, "titandb.blob.file.bytes.written"},
    {TITAN_BLOB_FILE_BYTES_READ, "titandb.blob.file.bytes.read"},
    {TITAN_BLOB_FILE_SYNCED, "titandb.blob.file.synced"},
    {TITAN_GC_NUM_FILES, "titandb.gc.num.files"},
    {TITAN_GC_NUM_NEW_FILES, "titandb.gc.num.new.files"},
    {TITAN_GC_NUM_KEYS_OVERWRITTEN, "titandb.gc.num.keys.overwritten"},
    {TITAN_GC_NUM_KEYS_RELOCATED, "titandb.gc.num.keys.relocated"},
    {TITAN_GC_BYTES_OVERWRITTEN, "titandb.gc.bytes.overwritten"},
    {TITAN_GC_BYTES_RELOCATED, "titandb.gc.bytes.relocated"},
    {TITAN_GC_BYTES_WRITTEN, "titandb.gc.bytes.written"},
    {TITAN_GC_BYTES_READ, "titandb.gc.bytes.read"},
    {TITAN_BLOB_CACHE_HIT, "titandb.blob.cache.hit"},
    {TITAN_BLOB_CACHE_MISS, "titandb.blob.cache.miss"},
    {TITAN_GC_NO_NEED, "titandb.gc.no.need"},
    {TITAN_GC_REMAIN, "titandb.gc.remain"},
    {TITAN_GC_DISCARDABLE, "titandb.gc.discardable"},
    {TITAN_GC_SAMPLE, "titandb.gc.sample"},
    {TITAN_GC_SMALL_FILE, "titandb.gc.small.file"},
    {TITAN_GC_FAILURE, "titandb.gc.failure"},
    {TITAN_GC_SUCCESS, "titandb.gc.success"},
    {TITAN_GC_TRIGGER_NEXT, "titandb.gc.trigger.next"},
};

enum HistogramType : uint32_t {
  TITAN_KEY_SIZE = HISTOGRAM_ENUM_MAX,
  TITAN_VALUE_SIZE,

  TITAN_GET_MICROS,
  TITAN_SEEK_MICROS,
  TITAN_NEXT_MICROS,
  TITAN_PREV_MICROS,

  TITAN_BLOB_FILE_WRITE_MICROS,
  TITAN_BLOB_FILE_READ_MICROS,
  TITAN_BLOB_FILE_SYNC_MICROS,
  TITAN_MANIFEST_FILE_SYNC_MICROS,

  TITAN_GC_MICROS,
  TITAN_GC_INPUT_FILE_SIZE,
  TITAN_GC_OUTPUT_FILE_SIZE,

  TITAN_ITER_TOUCH_BLOB_FILE_COUNT,

  TITAN_HISTOGRAM_ENUM_MAX,
};

const std::vector<std::pair<HistogramType, std::string>>
    TitanHistogramsNameMap = {
        {TITAN_KEY_SIZE, "titandb.key.size"},
        {TITAN_VALUE_SIZE, "titandb.value.size"},
        {TITAN_GET_MICROS, "titandb.get.micros"},
        {TITAN_SEEK_MICROS, "titandb.seek.micros"},
        {TITAN_NEXT_MICROS, "titandb.next.micros"},
        {TITAN_PREV_MICROS, "titandb.prev.micros"},
        {TITAN_BLOB_FILE_WRITE_MICROS, "titandb.blob.file.write.micros"},
        {TITAN_BLOB_FILE_READ_MICROS, "titandb.blob.file.read.micros"},
        {TITAN_BLOB_FILE_SYNC_MICROS, "titandb.blob.file.sync.micros"},
        {TITAN_MANIFEST_FILE_SYNC_MICROS, "titandb.manifest.file.sync.micros"},

        {TITAN_GC_MICROS, "titandb.gc.micros"},
        {TITAN_GC_INPUT_FILE_SIZE, "titandb.gc.input.file.size"},
        {TITAN_GC_OUTPUT_FILE_SIZE, "titandb.gc.output.file.size"},
        {TITAN_ITER_TOUCH_BLOB_FILE_COUNT,
         "titandb.iter.touch.blob.file.count"},
};

}  // namespace titandb
}  // namespace rocksdb
