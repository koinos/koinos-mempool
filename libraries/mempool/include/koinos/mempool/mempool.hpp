#pragma once

#include <chrono>
#include <memory>
#include <utility>
#include <vector>

#include <koinos/crypto/multihash.hpp>
#include <koinos/exception.hpp>
#include <koinos/state_db/state_db.hpp>

#include <koinos/broadcast/broadcast.pb.h>
#include <koinos/protocol/protocol.pb.h>
#include <koinos/rpc/mempool/mempool_rpc.pb.h>

namespace koinos::mempool {

namespace constants { constexpr uint64_t max_pending_transaction_request = 2000; }

using transaction_id_type = std::string;
using account_type = std::string;
using nonce_type = uint64_t;
using block_height_type = uint64_t;

KOINOS_DECLARE_EXCEPTION( pending_transaction_insertion_failure );
KOINOS_DECLARE_EXCEPTION( pending_transaction_exceeds_resources );
KOINOS_DECLARE_EXCEPTION( pending_transaction_request_overflow );
KOINOS_DECLARE_EXCEPTION( pending_transaction_unlinkable_block );
KOINOS_DECLARE_EXCEPTION( pending_transaction_unknown_block );

namespace detail { class mempool_impl; }

class mempool final
{
private:
   std::unique_ptr< detail::mempool_impl > _my;

public:
   mempool( state_db::fork_resolution_algorithm algo = state_db::fork_resolution_algorithm::fifo );
   virtual ~mempool();

   bool check_pending_account_resources(
      const account_type& payer,
      uint64_t max_payer_resources,
      uint64_t trx_resource_limit,
      std::optional< crypto::multihash > block_id = {} ) const;

   uint64_t add_pending_transaction(
      const protocol::transaction& transaction,
      std::chrono::system_clock::time_point time,
      uint64_t max_payer_rc,
      uint64_t disk_storaged_used,
      uint64_t network_bandwidth_used,
      uint64_t compute_bandwidth_used );

   bool has_pending_transaction(
      const transaction_id_type& id,
      std::optional< crypto::multihash > block_id = {} ) const;

   std::vector< rpc::mempool::pending_transaction > get_pending_transactions(
      uint64_t limit = constants::max_pending_transaction_request,
      std::optional< crypto::multihash > block_id = {} );

   uint64_t remove_pending_transactions( const std::vector< transaction_id_type >& ids );

   uint64_t prune(
      std::chrono::seconds expiration,
      std::chrono::system_clock::time_point now = std::chrono::system_clock::now() );

   void handle_block( const koinos::broadcast::block_accepted& bam );
   void handle_irreversibility( const koinos::broadcast::block_irreversible& bi );
};

} // koinos::mempool
