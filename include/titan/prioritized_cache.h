#pragma once
#include "rocksdb/cache.h"

namespace rocksdb {
namespace titandb {

class PrioritizedCache : public Cache {
 public:
  // Constructs a PrioritizedCache, which is a wrapper
  // of rocksdb Cache
  PrioritizedCache(std::shared_ptr<Cache> cache, Cache::Priority priority);
  ~PrioritizedCache();

  // Get the Cache ptr
  std::shared_ptr<Cache> GetCache();

  // always insert into the cache with the priority when init,
  // regardless of user provided option
  Status Insert(const Slice& key, void* value, size_t charge,
                void (*deleter)(const Slice& key, void* value),
                Cache::Handle** handle, Cache::Priority /*priority*/) override;

  // The type of the Cache
  const char* Name() const override;

  // If the cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  // If stats is not nullptr, relative tickers could be used inside the
  // function.
  Cache::Handle* Lookup(const Slice& key, Statistics* stats = nullptr) override;

  // Increments the reference count for the handle if it refers to an entry in
  // the cache. Returns true if refcount was incremented; otherwise, returns
  // false.
  // REQUIRES: handle must have been returned by a method on *this.
  bool Ref(Cache::Handle* handle) override;

  /**
   * Release a mapping returned by a previous Lookup(). A released entry might
   * still  remain in cache in case it is later looked up by others. If
   * force_erase is set then it also erase it from the cache if there is no
   * other reference to  it. Erasing it should call the deleter function that
   * was provided when the
   * entry was inserted.
   *
   * Returns true if the entry was also erased.
   */
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  bool Release(Cache::Handle* handle, bool force_erase = false) override;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  void* Value(Cache::Handle* handle) override;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  void Erase(const Slice& key) override;

  // Return a new numeric id.  May be used by multiple clients who are
  // sharding the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  uint64_t NewId() override;

  // sets the maximum configured capacity of the cache. When the new
  // capacity is less than the old capacity and the existing usage is
  // greater than new capacity, the implementation will do its best job to
  // purge the released entries from the cache in order to lower the usage
  void SetCapacity(size_t capacity) override;

  // Set whether to return error on insertion when cache reaches its full
  // capacity.
  void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Get the flag whether to return error on insertion when cache reaches its
  // full capacity.
  bool HasStrictCapacityLimit() const override;

  // returns the maximum configured capacity of the cache
  size_t GetCapacity() const override;

  // returns the memory size for the entries residing in the cache.
  size_t GetUsage() const override;

  // returns the memory size for a specific entry in the cache.
  size_t GetUsage(Cache::Handle* handle) const override;

  // returns the memory size for the entries in use by the system
  size_t GetPinnedUsage() const override;

  // returns the charge for the specific entry in the cache.
  size_t GetCharge(Cache::Handle* handle) const override;

  // Apply callback to all entries in the cache
  // If thread_safe is true, it will also lock the accesses. Otherwise, it will
  // access the cache without the lock held
  void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                              bool thread_safe) override;

  // Remove all entries.
  // Prerequisite: no entry is referenced.
  void EraseUnRefEntries() override;

 private:
  std::shared_ptr<Cache> cache_;
  Cache::Priority priority_;
};

}  // namespace titandb
}  // namespace rocksdb