#pragma once

#include <chrono>
#include <memory>
#include <utility>
#include <vector>

#include <koinos/crypto/multihash.hpp>
#include <koinos/exception.hpp>
#include <koinos/protocol/protocol.pb.h>
#include <koinos/rpc/mempool/mempool_rpc.pb.h>

#define MAX_PENDING_TRANSACTION_REQUEST 2000

namespace koinos::mempool {

using transaction_id_type = std::string;
using account_type = std::string;
using nonce_type = uint64_t;
using block_height_type = uint64_t;

KOINOS_DECLARE_EXCEPTION( pending_transaction_insertion_failure );
KOINOS_DECLARE_EXCEPTION( pending_transaction_exceeds_resources );
KOINOS_DECLARE_EXCEPTION( pending_transaction_request_overflow );

enum class fork_resolution_algorithm
{
   fifo,
   block_time,
   pob
};

namespace detail { class mempool_impl; }

class mempool final
{
private:
   std::unique_ptr< detail::mempool_impl > _my;

public:
   mempool();
   virtual ~mempool();

   bool check_pending_account_resources(
      const account_type& payer,
      uint64_t max_payer_rc,
      uint64_t rc_limit )const;

   uint64_t add_pending_transaction(
      const protocol::transaction& transaction,
      std::chrono::system_clock::time_point time,
      uint64_t max_payer_rc,
      uint64_t disk_storaged_used,
      uint64_t network_bandwidth_used,
      uint64_t compute_bandwidth_used );

   bool has_pending_transaction( const transaction_id_type& id )const;
   std::vector< rpc::mempool::pending_transaction > get_pending_transactions( std::size_t limit = MAX_PENDING_TRANSACTION_REQUEST );
   std::pair< uint64_t, uint64_t > remove_pending_transactions( const std::vector< transaction_id_type >& ids );
   uint64_t prune( std::chrono::seconds expiration, std::chrono::system_clock::time_point now = std::chrono::system_clock::now() );
};

} // koinos::mempool
