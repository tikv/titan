#include "titan/prioritized_cache.h"

namespace rocksdb {
namespace titandb {

PrioritizedCache::PrioritizedCache(std::shared_ptr<Cache> cache,
                                   Cache::Priority priority)
    : cache_(cache), priority_(priority) {}

PrioritizedCache::~PrioritizedCache() {}

std::shared_ptr<Cache> PrioritizedCache::GetCache() { return cache_; }

Status PrioritizedCache::Insert(const Slice& key, void* value, size_t charge,
                                void (*deleter)(const Slice& key, void* value),
                                Cache::Handle** handle,
                                Cache::Priority /*priority*/) {
  return cache_->Insert(key, value, charge, deleter, handle, priority_);
}

const char* PrioritizedCache::Name() const { return "PrioritizedCache"; }

Cache::Handle* PrioritizedCache::Lookup(const Slice& key,
                                 Statistics* stats) {
  return cache_->Lookup(key, stats);
}

bool PrioritizedCache::Ref(Cache::Handle* handle) { return cache_->Ref(handle); }

bool PrioritizedCache::Release(Cache::Handle* handle, bool force_erase) {
  return cache_->Release(handle, force_erase);
}

void* PrioritizedCache::Value(Cache::Handle* handle) { return cache_->Value(handle); }

void PrioritizedCache::Erase(const Slice& key) { cache_->Erase(key); }

uint64_t PrioritizedCache::NewId() { return cache_->NewId(); }

void PrioritizedCache::SetCapacity(size_t capacity) {
  cache_->SetCapacity(capacity);
}

void PrioritizedCache::SetStrictCapacityLimit(bool strict_capacity_limit) {
  cache_->SetStrictCapacityLimit(strict_capacity_limit);
}

bool PrioritizedCache::HasStrictCapacityLimit() const {
  return cache_->HasStrictCapacityLimit();
}

size_t PrioritizedCache::GetCapacity() const { return cache_->GetCapacity(); }

size_t PrioritizedCache::GetUsage() const { return cache_->GetUsage(); }

size_t PrioritizedCache::GetUsage(Cache::Handle* handle) const {
  return cache_->GetUsage();
}

size_t PrioritizedCache::GetPinnedUsage() const {
  return cache_->GetPinnedUsage();
}

size_t PrioritizedCache::GetCharge(Cache::Handle* handle) const {
  return cache_->GetCharge(handle);
}

void PrioritizedCache::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                              bool thread_safe) {
  return cache_->ApplyToAllCacheEntries(callback, thread_safe);
}

void PrioritizedCache::EraseUnRefEntries() {
  return cache_->EraseUnRefEntries();
}

}  // namespace titandb
}  // namespace rocksdb