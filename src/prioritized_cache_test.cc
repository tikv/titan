#include "titan/prioritized_cache.h"
#include "test_util/testharness.h"
#include "util.h"

namespace rocksdb {
namespace titandb {

class PrioritizedCacheTest : public testing::Test {
public:
  PrioritizedCacheTest() {}
  ~PrioritizedCacheTest() {}
};

TEST_F(PrioritizedCacheTest, PriorityTest) {
  Slice high_cache_key = "high_test_key";
  Slice cacahe_value = "test_value";
  Slice low_cache_key = "low_test_key";

  LRUCacheOptions options;
  options.capacity = high_cache_key.size() + cacahe_value.size();
  options.high_pri_pool_ratio = 1;

  std::shared_ptr<Cache> cache = NewLRUCache(options);
  cache->SetCapacity(high_cache_key.size() + cacahe_value.size());
  PrioritizedCache high_cache(cache, Cache::Priority::HIGH);
  PrioritizedCache low_cache(cache, Cache::Priority::LOW);
  Cache::Handle* cache_handle = cache->Lookup(high_cache_key);

  // here we set insert Priority to high, to check if it will 
  // insert to low in fact
  auto lo_ok = low_cache.Insert(low_cache_key, (void*)&cacahe_value, cache->GetCapacity(), 
      &DeleteCacheValue<Slice>, &cache_handle, Cache::Priority::LOW);
  ASSERT_TRUE(lo_ok.ok());

  // here we set insert Priority to low, to check if it will 
  // insert to high in fact
  auto hi_ok = high_cache.Insert(high_cache_key, (void*)&cacahe_value, cache->GetCapacity(), 
      &DeleteCacheValue<Slice>, &cache_handle, Cache::Priority::LOW);
  ASSERT_TRUE(hi_ok.ok());

  auto high_handle = cache->Lookup(high_cache_key);
  if (high_handle) {
    auto v = reinterpret_cast<Slice*>(cache->Value(high_handle));
    ASSERT_EQ(cacahe_value.data(), v->data());
  }

  auto low_handle = cache->Lookup(low_cache_key);
  if (low_handle) {
    auto v = reinterpret_cast<Slice*>(cache->Value(low_handle));
    ASSERT_EQ(cacahe_value.data(), v->data());
  }

}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
