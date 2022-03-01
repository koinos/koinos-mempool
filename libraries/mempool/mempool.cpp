#include <koinos/mempool/mempool.hpp>

#include <functional>
#include <tuple>

#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/composite_key.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>

#include <koinos/util/base58.hpp>
#include <koinos/util/conversion.hpp>
#include <koinos/util/hex.hpp>

namespace koinos::mempool {

namespace detail {

using namespace boost;
using int128_t = boost::multiprecision::int128_t;

struct pending_transaction_object
{
   crypto::multihash     id;
   protocol::transaction transaction;
   block_height_type     last_update;            //< Chain height at the time of submission
   account_type          payer;                  //< Payer at the time of submission
   uint64_t              rc_limit;               //< Max resources at the time of submission
   uint64_t              disk_storage_used;
   uint64_t              network_bandwidth_used;
   uint64_t              compute_bandwidth_used;
};

struct by_id;
struct by_height;

using pending_transaction_index = multi_index_container<
   pending_transaction_object,
   multi_index::indexed_by<
      multi_index::sequenced<>,
      multi_index::ordered_unique< multi_index::tag< by_id >,
         multi_index::member< pending_transaction_object, crypto::multihash, &pending_transaction_object::id >
      >,
      multi_index::ordered_non_unique< multi_index::tag< by_height >,
         multi_index::member< pending_transaction_object, block_height_type, &pending_transaction_object::last_update >
      >
   >
>;

struct account_resources_object
{
   account_type      account;
   uint64_t          resources;
   uint64_t          max_resources;
   block_height_type last_update;
};

struct by_account;

using account_resources_index = multi_index_container<
   account_resources_object,
   multi_index::indexed_by<
      multi_index::ordered_unique< multi_index::tag< by_account >,
         multi_index::member< account_resources_object, account_type, &account_resources_object::account >
      >
   >
>;

class mempool_impl final
{
private:
   account_resources_index          _account_resources_idx;
   mutable std::mutex               _account_resources_mutex;

   pending_transaction_index        _pending_transaction_idx;
   mutable std::mutex               _pending_transaction_mutex;

public:
   mempool_impl();
   virtual ~mempool_impl();

   bool has_pending_transaction( const crypto::multihash& id ) const;
   std::vector< rpc::mempool::pending_transaction > get_pending_transactions( std::size_t limit );
   bool check_pending_account_resources(
      const account_type& payer,
      uint64_t max_payer_resources,
      uint64_t trx_resource_limit ) const;
   void add_pending_transaction(
      const protocol::transaction& transaction,
      block_height_type height,
      const account_type& payer,
      uint64_t max_payer_rc,
      uint64_t rc_limit,
      uint64_t disk_storaged_used,
      uint64_t network_bandwidth_used,
      uint64_t compute_bandwidth_used );
   void remove_pending_transactions( const std::vector< crypto::multihash >& ids );
   void prune( block_height_type h );
   std::size_t payer_entries_size() const;
   void cleanup_account_resources( const pending_transaction_object& pending_trx );

private:
   bool check_pending_account_resources_lockfree(
         const account_type& payer,
         uint64_t max_payer_rc,
         uint64_t rc_limit
      )const;
};

mempool_impl::mempool_impl() {}
mempool_impl::~mempool_impl() = default;

bool mempool_impl::has_pending_transaction( const crypto::multihash& id ) const
{
   std::lock_guard< std::mutex > guard( _pending_transaction_mutex );
   auto& id_idx = _pending_transaction_idx.get< by_id >();

   auto it = id_idx.find( id );

   return it != id_idx.end();
}

std::vector< rpc::mempool::pending_transaction > mempool_impl::get_pending_transactions( std::size_t limit )
{
   KOINOS_ASSERT( limit <= MAX_PENDING_TRANSACTION_REQUEST, pending_transaction_request_overflow, "Requested too many pending transactions. Max: ${max}", ("max", MAX_PENDING_TRANSACTION_REQUEST) );

   std::lock_guard< std::mutex > guard( _pending_transaction_mutex );

   std::vector< rpc::mempool::pending_transaction > pending_transactions;
   pending_transactions.reserve(limit);

   auto itr = _pending_transaction_idx.begin();

   while ( itr != _pending_transaction_idx.end() && pending_transactions.size() < limit )
   {
      rpc::mempool::pending_transaction ptx;
      *ptx.mutable_transaction() = itr->transaction;
      ptx.set_disk_storage_used( itr->disk_storage_used );
      ptx.set_network_bandwidth_used( itr->network_bandwidth_used );
      ptx.set_compute_bandwidth_used( itr->compute_bandwidth_used );

      pending_transactions.push_back( ptx );
      ++itr;
   }

   return pending_transactions;
}

bool mempool_impl::check_pending_account_resources_lockfree(
   const account_type& payer,
   uint64_t max_payer_resources,
   uint64_t trx_resource_limit ) const
{
   auto& account_idx = _account_resources_idx.get< by_account >();
   auto it = account_idx.find( payer );

   if ( it == account_idx.end() )
   {
      return trx_resource_limit <= max_payer_resources;
   }

   int128_t max_resource_delta = int128_t( max_payer_resources ) - int128_t( it->max_resources );
   int128_t new_resources = int128_t( it->resources ) + max_resource_delta - int128_t( trx_resource_limit );
   return new_resources >= 0;
}

bool mempool_impl::check_pending_account_resources(
   const account_type& payer,
   uint64_t max_payer_rc,
   uint64_t rc_limit ) const
{
   std::lock_guard< std::mutex > guard( _account_resources_mutex );
   return check_pending_account_resources_lockfree( payer, max_payer_rc, rc_limit );
}

void mempool_impl::add_pending_transaction(
   const protocol::transaction& transaction,
   block_height_type height,
   const account_type& payer,
   uint64_t max_payer_rc,
   uint64_t rc_limit,
   uint64_t disk_storaged_used,
   uint64_t network_bandwidth_used,
   uint64_t compute_bandwidth_used )
{
   {
      std::lock_guard< std::mutex > guard( _account_resources_mutex );

      KOINOS_ASSERT(
         check_pending_account_resources_lockfree( payer, max_payer_rc, rc_limit ),
         pending_transaction_exceeds_resources,
         "transaction would exceed maximum resources for account: ${a}", ("a", util::encode_base58( util::converter::as< std::vector< std::byte > >( payer )))
      );

      {
         auto id = util::converter::to< crypto::multihash >( transaction.id() );

         std::lock_guard< std::mutex > guard( _pending_transaction_mutex );

         auto rval = _pending_transaction_idx.emplace_back( pending_transaction_object {
            .id                     = id,
            .transaction            = transaction,
            .last_update            = height,
            .payer                  = payer,
            .rc_limit               = rc_limit,
            .disk_storage_used      = disk_storaged_used,
            .network_bandwidth_used = network_bandwidth_used,
            .compute_bandwidth_used = compute_bandwidth_used
         } );

         KOINOS_ASSERT( rval.second, pending_transaction_insertion_failure, "failed to insert transaction with id: ${id}", ("id", id) );
      }

      auto& account_idx = _account_resources_idx.get< by_account >();
      auto it = account_idx.find( payer );

      if ( it == account_idx.end() )
      {
         _account_resources_idx.insert( account_resources_object {
            .account       = payer,
            .resources     = max_payer_rc - rc_limit,
            .max_resources = max_payer_rc,
            .last_update   = height
         } );
      }
      else
      {
         int128_t max_resource_delta = int128_t( max_payer_rc ) - int128_t( it->max_resources );
         int128_t new_resources = int128_t( it->resources ) + max_resource_delta - int128_t( rc_limit );

         account_idx.modify( it, [&]( account_resources_object& aro )
         {
            aro.max_resources = max_payer_rc;
            aro.resources = new_resources.convert_to< uint64_t >();
            aro.last_update = height;
         } );
      }
   }

   LOG(info) << "Transaction added to mempool: " << util::to_hex( transaction.id() );
}

void mempool_impl::remove_pending_transactions( const std::vector< crypto::multihash >& ids )
{
   std::lock_guard< std::mutex > account_guard( _account_resources_mutex );
   std::lock_guard< std::mutex > trx_guard( _pending_transaction_mutex );

   for ( const auto& id : ids )
   {
      auto& id_idx = _pending_transaction_idx.get< by_id >();

      auto it = id_idx.find( id );
      if ( it != id_idx.end() )
      {
         cleanup_account_resources( *it );
         LOG(info) << "Removing included transaction from mempool: " << util::to_hex( it->transaction.id() );
         id_idx.erase( it );
      }
   }
}

void mempool_impl::prune( block_height_type h )
{
   std::lock_guard< std::mutex > account_guard( _account_resources_mutex );
   std::lock_guard< std::mutex > trx_guard( _pending_transaction_mutex );

   auto& by_block_idx = _pending_transaction_idx.get< by_height >();
   auto itr = by_block_idx.begin();

   while( itr != by_block_idx.end() && itr->last_update <= h )
   {
      cleanup_account_resources( *itr );
      LOG(info) << "Pruning transaction from mempool: " << util::to_hex( itr->transaction.id() );
      itr = by_block_idx.erase( itr );
   }
}

std::size_t mempool_impl::payer_entries_size() const
{
   std::lock_guard< std::mutex > guard( _account_resources_mutex );
   return _account_resources_idx.size();
}

void mempool_impl::cleanup_account_resources( const pending_transaction_object& pending_trx )
{
   auto itr = _account_resources_idx.find( pending_trx.payer );
   if ( itr != _account_resources_idx.end() )
   {
      if ( itr->resources + pending_trx.rc_limit >= itr->max_resources )
      {
         _account_resources_idx.erase( itr );
      }
      else
      {
         _account_resources_idx.modify( itr, [&]( account_resources_object& aro )
         {
            aro.resources += pending_trx.rc_limit;
         } );
      }
   }
}

} // detail

mempool::mempool() : _my( std::make_unique< detail::mempool_impl >() ) {}
mempool::~mempool() = default;

bool mempool::has_pending_transaction( const crypto::multihash& id ) const
{
   return _my->has_pending_transaction( id );
}

std::vector< rpc::mempool::pending_transaction > mempool::get_pending_transactions( std::size_t limit )
{
   return _my->get_pending_transactions( limit );
}

bool mempool::check_pending_account_resources(
   const account_type& payer,
   uint64_t max_payer_resources,
   uint64_t trx_resource_limit ) const
{
   return _my->check_pending_account_resources( payer, max_payer_resources, trx_resource_limit );
}

void mempool::add_pending_transaction(
   const protocol::transaction& transaction,
   block_height_type height,
   const account_type& payer,
   uint64_t max_payer_rc,
   uint64_t rc_limit,
   uint64_t disk_storaged_used,
   uint64_t network_bandwidth_used,
   uint64_t compute_bandwidth_used )
{
   _my->add_pending_transaction( transaction, height, payer, max_payer_rc, rc_limit, disk_storaged_used, network_bandwidth_used, compute_bandwidth_used );
}

void mempool::remove_pending_transactions( const std::vector< crypto::multihash >& ids )
{
   _my->remove_pending_transactions( ids );
}

void mempool::prune( block_height_type h )
{
   _my->prune( h );
}

std::size_t mempool::payer_entries_size() const
{
   return _my->payer_entries_size();
}

} // koinos::mempool
