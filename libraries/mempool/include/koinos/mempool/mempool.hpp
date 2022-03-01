#pragma once

#include <memory>
#include <vector>

#include <koinos/crypto/multihash.hpp>
#include <koinos/exception.hpp>
#include <koinos/protocol/protocol.pb.h>
#include <koinos/rpc/mempool/mempool_rpc.pb.h>

#define MAX_PENDING_TRANSACTION_REQUEST 500

namespace koinos::mempool {

using account_type = std::string;
using block_height_type = uint64_t;

KOINOS_DECLARE_EXCEPTION( pending_transaction_insertion_failure );
KOINOS_DECLARE_EXCEPTION( pending_transaction_exceeds_resources );
KOINOS_DECLARE_EXCEPTION( pending_transaction_request_overflow );

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

   void add_pending_transaction(
      const protocol::transaction& transaction,
      block_height_type height,
      const account_type& payer,
      uint64_t max_payer_rc,
      uint64_t rc_limit,
      uint64_t disk_storaged_used,
      uint64_t network_bandwidth_used,
      uint64_t compute_bandwidth_used );

   bool has_pending_transaction( const crypto::multihash& id )const;
   std::vector< rpc::mempool::pending_transaction > get_pending_transactions( std::size_t limit = MAX_PENDING_TRANSACTION_REQUEST );
   void remove_pending_transactions( const std::vector< crypto::multihash >& ids );
   void prune( block_height_type h );
   std::size_t payer_entries_size()const;
};

} // koinos::mempool
