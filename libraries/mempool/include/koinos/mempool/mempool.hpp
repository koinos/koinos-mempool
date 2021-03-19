#pragma once

#include <memory>
#include <vector>

#include <koinos/exception.hpp>
#include <koinos/pack/classes.hpp>

#define MAX_PENDING_TRANSACTION_REQUEST 100

namespace koinos::mempool {

using account_type = variable_blob;

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
      const uint128& max_payer_resources,
      const uint128& trx_resource_limit
   ) const;

   void add_pending_transaction(
      const multihash& id,
      const protocol::transaction& t,
      block_height_type h,
      const account_type& payer,
      const uint128& max_payer_resources,
      const uint128& trx_resource_limit );

   bool has_pending_transaction( const multihash& id );
   std::vector< protocol::transaction > get_pending_transactions( const multihash& start = multihash(), std::size_t limit = 100 );
   void remove_pending_transaction( const multihash& id );
   void prune( block_height_type h );
   std::size_t payer_entries_size();
};

} // koinos::mempool
