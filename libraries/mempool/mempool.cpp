#include <koinos/mempool/mempool.hpp>
#include <koinos/mempool/state.hpp>

#include <chrono>
#include <functional>
#include <tuple>

#include <boost/multiprecision/cpp_int.hpp>

#include <koinos/chain/value.pb.h>
#include <koinos/protocol/protocol.pb.h>
#include <koinos/mempool/mempool.pb.h>
#include <koinos/state_db/state_db.hpp>

#include <koinos/util/base58.hpp>
#include <koinos/util/conversion.hpp>
#include <koinos/util/hex.hpp>

namespace koinos::mempool {

namespace detail {

using int128_t = boost::multiprecision::int128_t;

class mempool_impl final
{
private:
   state_db::database _db;

   crypto::multihash tmp_id( crypto::multihash id ) const;
   state_db::state_node_ptr relevant_node( std::optional< crypto::multihash > id, state_db::shared_lock_ptr lock ) const;

   void cleanup_account_resources( state_db::state_node_ptr node, const pending_transaction_record& pending_trx );

   uint64_t remove_pending_transactions( state_db::state_node_ptr node, const std::vector< transaction_id_type >& ids );

   bool check_pending_account_resources(
      state_db::abstract_state_node_ptr node,
      const account_type& payer,
      uint64_t max_payer_resources,
      uint64_t trx_resource_limit ) const;

   uint64_t add_pending_transaction_to_fork(
      state_db::anonymous_state_node_ptr node,
      const protocol::transaction& transaction,
      std::chrono::system_clock::time_point time,
      uint64_t max_payer_rc,
      uint64_t disk_storaged_used,
      uint64_t network_bandwidth_used,
      uint64_t compute_bandwidth_used );

public:
   mempool_impl( fork_resolution_algorithm algo );
   virtual ~mempool_impl();

   bool has_pending_transaction( const transaction_id_type& id, std::optional< crypto::multihash > block_id ) const;

   std::vector< rpc::mempool::pending_transaction > get_pending_transactions( uint64_t limit, std::optional< crypto::multihash > block_id );

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

   uint64_t remove_pending_transactions( const std::vector< transaction_id_type >& ids );

   uint64_t prune( std::chrono::seconds expiration, std::chrono::system_clock::time_point now );

   void handle_block( const koinos::broadcast::block_accepted& bam );
   void handle_irreversibility( const koinos::broadcast::block_irreversible& bi );
};

mempool_impl::mempool_impl( fork_resolution_algorithm algo )
{
   state_db::state_node_comparator_function comp;

   switch ( algo )
   {
      case fork_resolution_algorithm::block_time:
         comp = &state_db::block_time_comparator;
         break;
      case fork_resolution_algorithm::pob:
         comp = &state_db::pob_comparator;
         break;
      case fork_resolution_algorithm::fifo:
         [[fallthrough]];
      default:
         comp = &state_db::fifo_comparator;
   }


   _db.open( {}, []( state_db::state_node_ptr ){}, comp, _db.get_unique_lock() );

   auto lock =_db.get_shared_lock();
   auto node_id = _db.get_head( lock )->id();
   [[maybe_unused]] auto node = _db.create_writable_node( node_id, tmp_id( node_id ), protocol::block_header(), lock );
   assert( node );
}

mempool_impl::~mempool_impl() = default;

crypto::multihash mempool_impl::tmp_id( crypto::multihash id ) const
{
   return crypto::hash( crypto::multicodec::sha2_256, id );
}

state_db::state_node_ptr mempool_impl::relevant_node( std::optional< crypto::multihash > id, state_db::shared_lock_ptr lock ) const
{
   state_db::state_node_ptr node;

   if ( id )
   {
      auto block_node = _db.get_node( *id, lock );
      node = _db.get_node( tmp_id( block_node->id() ), lock );
      assert( node );
   }
   else
   {
      auto block_node = _db.get_head( lock );
      node = _db.get_node( tmp_id( block_node->id() ), lock );
      assert( node );
   }

   return node;
}

void mempool_impl::handle_block( const koinos::broadcast::block_accepted& bam )
{
   auto block_id = util::converter::to< crypto::multihash >( bam.block().id() );
   auto previous_id = util::converter::to< crypto::multihash >( bam.block().header().previous() );
   auto lock = _db.get_shared_lock();

   LOG(debug) << "Handling block - Height: " << bam.block().header().height() <<  ", ID: " << block_id;

   crypto::multihash node_id;

   try
   {
      auto root_id = _db.get_root( lock )->id();

      // We are handling the genesis case
      if ( root_id != _db.get_head( lock )->id() )
         node_id = tmp_id( previous_id );
      else
         node_id = tmp_id( root_id );

      auto node = _db.get_node( node_id, lock );
      assert( node );

      std::vector< transaction_id_type > ids;

      for ( int i = 0; i < bam.block().transactions_size(); ++i )
         ids.emplace_back( bam.block().transactions( i ).id() );

      const auto removed_count = remove_pending_transactions( node, ids );

      _db.update_node( node_id, block_id, bam.block().header(), lock );
      node_id = block_id;

      if ( bam.live() && removed_count )
         LOG(info) << "Removed " << removed_count << " included transaction(s) via block - Height: "
            << bam.block().header().height() << ", ID: " << util::to_hex( bam.block().id() );
   }
   catch ( ... )
   {
      _db.discard_node( node_id, lock );
      LOG(error) << "A fatal error has occurred while processing block broadcast";
      assert( false );
      throw;
   }

   _db.finalize_node( node_id, lock );
   [[maybe_unused]] auto node = _db.create_writable_node( node_id, tmp_id( node_id ), protocol::block_header(), lock );
   assert( node );
}

void mempool_impl::handle_irreversibility( const koinos::broadcast::block_irreversible& bi )
{
   auto block_id = util::converter::to< crypto::multihash >( bi.topology().id() );
   auto lock = _db.get_unique_lock();
   if ( auto lib = _db.get_node( block_id, lock ); lib )
      _db.commit_node( block_id, lock );
}

bool mempool_impl::has_pending_transaction( const transaction_id_type& id, std::optional< crypto::multihash > block_id ) const
{
   auto lock = _db.get_shared_lock();

   if ( auto node = relevant_node( block_id, lock ); node )
      return node->get_object( space::transaction_index(), id ) != nullptr;

   return false;
}

std::vector< rpc::mempool::pending_transaction > mempool_impl::get_pending_transactions( uint64_t limit, std::optional< crypto::multihash > block_id )
{
   KOINOS_ASSERT(
      limit <= constants::max_pending_transaction_request,
      pending_transaction_request_overflow,
      "requested too many pending transactions. max: ${max}", ("max", constants::max_pending_transaction_request)
   );

   auto lock = _db.get_shared_lock();

   auto node = relevant_node( block_id, lock );

   std::vector< rpc::mempool::pending_transaction > pending_transactions;
   pending_transactions.reserve( limit );

   state_db::object_key next = state_db::object_key();

   while ( pending_transactions.size() < limit )
   {
      auto [ value, key ] = node->get_next_object( space::pending_transaction(), next );

      if ( !value )
         break;

      next = key;

      auto pending_tx = util::converter::to< pending_transaction_record >( *value );
      uint64_t seq_no = util::converter::to< uint64_t >( key );

      rpc::mempool::pending_transaction ptx;
      *ptx.mutable_transaction() = pending_tx.transaction();
      ptx.set_disk_storage_used( pending_tx.disk_storage_used() );
      ptx.set_network_bandwidth_used( pending_tx.network_bandwidth_used() );
      ptx.set_compute_bandwidth_used( pending_tx.compute_bandwidth_used() );

      pending_transactions.push_back( ptx );
   }

   return pending_transactions;
}

bool mempool_impl::check_pending_account_resources(
   const account_type& payer,
   uint64_t max_payer_resources,
   uint64_t trx_resource_limit,
   std::optional< crypto::multihash > block_id ) const
{
   auto lock = _db.get_shared_lock();

   state_db::state_node_ptr node;

   node = relevant_node( block_id, lock );

   return check_pending_account_resources( node, payer, max_payer_resources, trx_resource_limit );
}

bool mempool_impl::check_pending_account_resources(
   state_db::abstract_state_node_ptr node,
   const account_type& payer,
   uint64_t max_payer_resources,
   uint64_t trx_resource_limit ) const
{
   if ( auto obj = node->get_object( space::address_resources(), payer ); obj )
   {
      auto arr = util::converter::to< address_resource_record >( *obj );

      int128_t max_resource_delta = int128_t( max_payer_resources ) - int128_t( arr.max_rc() );
      int128_t new_resources = int128_t( arr.current_rc() ) + max_resource_delta - int128_t( trx_resource_limit );
      return new_resources >= 0;
   }

   return trx_resource_limit <= max_payer_resources;
}

uint64_t mempool_impl::add_pending_transaction_to_fork(
   state_db::anonymous_state_node_ptr node,
   const protocol::transaction& transaction,
   std::chrono::system_clock::time_point time,
   uint64_t max_payer_rc,
   uint64_t disk_storaged_used,
   uint64_t network_bandwidth_used,
   uint64_t compute_bandwidth_used )
{
   uint64_t rc_used = 0;

   KOINOS_ASSERT(
      check_pending_account_resources( node, transaction.header().payer(), max_payer_rc, transaction.header().rc_limit() ),
      pending_transaction_exceeds_resources,
      "transaction would exceed maximum resources for account: ${a}", ("a", util::encode_base58( util::converter::as< std::vector< std::byte > >( transaction.header().payer() )) )
   );

   KOINOS_ASSERT(
      node->get_object( space::transaction_index(), transaction.id() ) == nullptr,
      pending_transaction_insertion_failure,
      "cannot insert duplicate transaction"
   );

   // Grab the latest metadata object if it exists
   mempool_metadata metadata;
   if ( auto obj = node->get_object( space::mempool_metadata(), std::string{} ); obj )
      metadata = util::converter::to< mempool_metadata >( *obj );
   else
      metadata.set_seq_num( 1 );

   uint64_t tx_id = metadata.seq_num();

   metadata.set_seq_num( metadata.seq_num() + 1 );

   // Update metadata
   if ( auto obj = util::converter::as< std::string >( metadata ); !obj.empty() )
      node->put_object( space::mempool_metadata(), std::string{}, &obj );

   pending_transaction_record pending_tx;
   *pending_tx.mutable_transaction() = transaction;
   pending_tx.set_timestamp( std::chrono::duration_cast< std::chrono::milliseconds >( time.time_since_epoch() ).count() );
   pending_tx.set_disk_storage_used( disk_storaged_used );
   pending_tx.set_network_bandwidth_used( network_bandwidth_used );
   pending_tx.set_compute_bandwidth_used( compute_bandwidth_used );

   std::string transaction_index_bytes = util::converter::as< std::string >( tx_id );
   auto pending_transaction_bytes = util::converter::as< std::string >( pending_tx );

   node->put_object( space::pending_transaction(), transaction_index_bytes, &pending_transaction_bytes );
   node->put_object( space::transaction_index(), transaction.id(), &transaction_index_bytes );

   address_resource_record arr;
   if ( auto obj = node->get_object( space::address_resources(), transaction.header().payer() ); obj )
   {
      arr = util::converter::to< address_resource_record >( *obj );

      int128_t max_rc_delta = int128_t( max_payer_rc ) - int128_t( arr.max_rc() );
      int128_t new_rc = int128_t( arr.current_rc() ) + max_rc_delta - int128_t( transaction.header().rc_limit() );

      arr.set_max_rc( max_payer_rc );
      arr.set_current_rc( new_rc.convert_to< uint64_t >() );

      rc_used = max_payer_rc - arr.current_rc();
   }
   else
   {
      arr.set_max_rc( max_payer_rc );
      arr.set_current_rc( max_payer_rc - transaction.header().rc_limit() );

      rc_used = transaction.header().rc_limit();
   }

   auto address_resource_bytes = util::converter::as< std::string >( arr );
   node->put_object( space::address_resources(), transaction.header().payer(), &address_resource_bytes );

   return rc_used;
}

uint64_t mempool_impl::add_pending_transaction(
   const protocol::transaction& transaction,
   std::chrono::system_clock::time_point time,
   uint64_t max_payer_rc,
   uint64_t disk_storaged_used,
   uint64_t network_bandwidth_used,
   uint64_t compute_bandwidth_used )
{
   uint64_t rc_used = 0;

   auto lock = _db.get_shared_lock();
   auto nodes = _db.get_fork_heads( lock );

   try
   {
      std::vector< state_db::anonymous_state_node_ptr > anonymous_state_nodes;

      for ( auto block_node : nodes )
      {
         auto pending_node = relevant_node( block_node->id(), lock );
         assert( pending_node );
         auto node = pending_node->create_anonymous_node();
         anonymous_state_nodes.push_back( node );

         auto rc = add_pending_transaction_to_fork( node, transaction, time, max_payer_rc, disk_storaged_used, network_bandwidth_used, compute_bandwidth_used );

         // We're only returning the RC used as it pertains to head
         if ( block_node->id() == _db.get_head( lock )->id() )
            rc_used = rc;
      }

      for ( auto anonymous_state_node : anonymous_state_nodes )
         anonymous_state_node->commit();
   }
   catch ( const std::exception& e )
   {
      LOG(debug) << "Failed to apply pending transaction " << util::to_hex( transaction.id() ) << " with: " << e.what();
      throw;
   }
   catch ( ... )
   {
      LOG(debug) << "Failed to apply pending transaction " << util::to_hex( transaction.id() );
      throw;
   }

   LOG(debug) << "Transaction added to mempool: " << util::to_hex( transaction.id() );

   return rc_used;
}

uint64_t mempool_impl::remove_pending_transactions( const std::vector< transaction_id_type >& ids )
{
   uint64_t count = 0;
   auto lock = _db.get_shared_lock();
   auto nodes = _db.get_fork_heads( lock );

   for ( auto block_node : nodes )
   {
      auto node = relevant_node( block_node->id(), lock );

      auto num_removed = remove_pending_transactions( node, ids );

      // We're only returning the number of transactions removed as it pertains to head
      if ( block_node->id() == _db.get_head( lock )->id() )
         count = num_removed;
   }

   return count;
}

uint64_t mempool_impl::remove_pending_transactions( state_db::state_node_ptr node, const std::vector< transaction_id_type >& ids )
{
   uint64_t count = 0;

   for ( const auto& id : ids )
   {
      auto seq_obj = node->get_object( space::transaction_index(), id );
      if ( !seq_obj )
         continue;

      auto ptx_obj = node->get_object( space::pending_transaction(), *seq_obj );
      assert( ptx_obj );

      auto pending_tx = util::converter::to< pending_transaction_record >( *ptx_obj );

      cleanup_account_resources( node, pending_tx );
      node->remove_object( space::pending_transaction(), *seq_obj );
      node->remove_object( space::transaction_index(), id );

      count++;
   }

   return count;
}

uint64_t mempool_impl::prune( std::chrono::seconds expiration, std::chrono::system_clock::time_point now )
{
   uint64_t count = 0;
   auto lock = _db.get_shared_lock();
   auto nodes = _db.get_fork_heads( lock );

   for ( auto block_node : nodes )
   {
      auto node = relevant_node( block_node->id(), lock );

      bool is_head = block_node->id() == _db.get_head( lock )->id();

      for (;;)
      {
         auto [ ptx_obj, key ] = node->get_next_object( space::pending_transaction(), std::string{} );
         if ( !ptx_obj )
            break;

         auto pending_tx = util::converter::to< pending_transaction_record >( *ptx_obj );

         std::chrono::system_clock::time_point time { std::chrono::milliseconds { pending_tx.timestamp() } };
         if ( time + expiration > now )
            break;

         auto tx_idx_obj = node->get_object( space::transaction_index(), pending_tx.transaction().id() );
         assert( tx_idx_obj );

         cleanup_account_resources( node, pending_tx );
         node->remove_object( space::pending_transaction(), key );
         node->remove_object( space::transaction_index(), pending_tx.transaction().id() );

         // Only consider pruned transactions on the fork considered head
         if ( is_head )
            count++;
      }
   }

   return count;
}

void mempool_impl::cleanup_account_resources( state_db::state_node_ptr node, const pending_transaction_record& pending_trx )
{
   auto obj = node->get_object( space::address_resources(), pending_trx.transaction().header().payer() );
   assert( obj );

   auto arr = util::converter::to< address_resource_record >( *obj );
   if ( arr.current_rc() + pending_trx.transaction().header().rc_limit() >= arr.max_rc() )
   {
      node->remove_object( space::address_resources(), pending_trx.transaction().header().payer() );
   }
   else
   {
      arr.set_current_rc( arr.current_rc() + pending_trx.transaction().header().rc_limit() );
      auto arr_obj = util::converter::as< std::string >( arr );
      node->put_object( space::address_resources(), pending_trx.transaction().header().payer(), &arr_obj );
   }
}

} // detail

mempool::mempool( fork_resolution_algorithm algo ) : _my( std::make_unique< detail::mempool_impl >( algo ) ) {}
mempool::~mempool() = default;

bool mempool::has_pending_transaction( const transaction_id_type& id, std::optional< crypto::multihash > block_id ) const
{
   return _my->has_pending_transaction( id, block_id );
}

std::vector< rpc::mempool::pending_transaction > mempool::get_pending_transactions( uint64_t limit, std::optional< crypto::multihash > block_id )
{
   return _my->get_pending_transactions( limit, block_id );
}

bool mempool::check_pending_account_resources(
   const account_type& payer,
   uint64_t max_payer_resources,
   uint64_t trx_resource_limit,
   std::optional< crypto::multihash > block_id ) const
{
   return _my->check_pending_account_resources( payer, max_payer_resources, trx_resource_limit, block_id );
}

uint64_t mempool::add_pending_transaction(
   const protocol::transaction& transaction,
   std::chrono::system_clock::time_point time,
   uint64_t max_payer_rc,
   uint64_t disk_storaged_used,
   uint64_t network_bandwidth_used,
   uint64_t compute_bandwidth_used )
{
   return _my->add_pending_transaction( transaction, time, max_payer_rc, disk_storaged_used, network_bandwidth_used, compute_bandwidth_used );
}

uint64_t mempool::remove_pending_transactions( const std::vector< transaction_id_type >& ids )
{
   return _my->remove_pending_transactions( ids );
}

uint64_t mempool::prune( std::chrono::seconds expiration, std::chrono::system_clock::time_point now )
{
   return _my->prune( expiration, now );
}

void mempool::handle_block( const koinos::broadcast::block_accepted& bam )
{
   _my->handle_block( bam );
}

void mempool::handle_irreversibility( const koinos::broadcast::block_irreversible& bi )
{
   _my->handle_irreversibility( bi );
}

} // koinos::mempool
