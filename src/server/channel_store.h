// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>
#include <parallel_hashmap/phmap.h>

#include <shared_mutex>
#include <string_view>

#include "facade/connection_ref.h"
#include "facade/facade_types.h"
#include "util/fibers/synchronization.h"

// Specialize phmap::LockableImpl for util::fb2::SharedMutex.
namespace phmap {
template <> class LockableImpl<::util::fb2::SharedMutex> : public ::util::fb2::SharedMutex {
 public:
  using mutex_type = ::util::fb2::SharedMutex;
  using Base = LockableBaseImpl<::util::fb2::SharedMutex>;
  using SharedLock = std::shared_lock<mutex_type>;
  using ReadWriteLock = typename Base::ReadWriteLock;
  using UniqueLock = std::unique_lock<mutex_type>;
  using SharedLocks = typename Base::ReadLocks;
  using UniqueLocks = typename Base::WriteLocks;
};
}  // namespace phmap

namespace dfly {

class ConnectionContext;

namespace cluster {
class SlotSet;
}

// ChannelStore manages PUB/SUB subscriptions.
//
// A single global instance is shared across all threads. Concurrency is
// provided by phmap::parallel_flat_hash_map, which shards the map into
// N submaps each protected by fb2::SharedMutex. Concurrent readers
// (e.g. PUBLISH) hold individual submap locks only while accessing that
// submap; writers (SUBSCRIBE/UNSUBSCRIBE) hold the relevant submap lock
// for the duration of the insert/erase.
class ChannelStore {
 public:
  struct Subscriber : public facade::ConnectionRef {
    Subscriber(ConnectionRef ref, const std::string& pattern)
        : facade::ConnectionRef(std::move(ref)), pattern(pattern) {
    }

    // Sort by thread-id. Subscriber without owner comes first.
    static bool ByThread(const Subscriber& lhs, const Subscriber& rhs);
    static bool ByThreadId(const Subscriber& lhs, const unsigned thread);

    std::string pattern;  // non-empty if registered via psubscribe
  };

  ChannelStore() {
  }

  void Add(std::string_view channel, ConnectionContext* cntx, uint32_t thread_id, bool pattern);

  void Remove(std::string_view channel, ConnectionContext* cntx, bool pattern);

  // Send messages to channel, block on connection backpressure
  unsigned SendMessages(std::string_view channel, facade::ArgRange messages, bool sharded) const;

  // Fetch all subscribers for channel, including matching patterns.
  std::vector<Subscriber> FetchSubscribers(std::string_view channel) const;

  std::vector<std::string> ListChannels(const std::string_view pattern) const;

  size_t PatternCount() const;

  void UnsubscribeAfterClusterSlotMigration(const cluster::SlotSet& sdeleted_slots);

 private:
  using ThreadId = unsigned;

  // Subscribers for a single channel/pattern.
  using SubscribeMap = phmap::flat_hash_map<ConnectionContext*, ThreadId>;

  // Wraps absl::Hash<string_view> with is_transparent so phmap enables
  // heterogeneous overloads (if_contains, try_emplace_l, etc.).
  struct StringViewHash {
    using is_transparent = void;
    size_t operator()(std::string_view sv) const {
      return absl::Hash<std::string_view>{}(sv);
    }
  };

  using ChannelMap = phmap::parallel_flat_hash_map<
      std::string, SubscribeMap, StringViewHash, std::equal_to<>,
      phmap::priv::Allocator<std::pair<const std::string, SubscribeMap>>, 4,
      util::fb2::SharedMutex>;

  using ChannelsSubMap = absl::flat_hash_map<std::string, std::vector<ChannelStore::Subscriber>>;

  void UnsubscribeConnectionsFromDeletedSlots(const ChannelsSubMap& sub_map, uint32_t idx);

  static void Fill(const SubscribeMap& src, const std::string& pattern,
                   std::vector<Subscriber>* out);

  ChannelMap channels_;
  ChannelMap patterns_;
};

extern ChannelStore* channel_store;

}  // namespace dfly
