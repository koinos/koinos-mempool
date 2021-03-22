#include <boost/test/unit_test.hpp>

#include <koinos/mempool/mempool.hpp>

#include <koinos/crypto/elliptic.hpp>
#include <koinos/crypto/multihash.hpp>

#include <koinos/pack/classes.hpp>

#include <memory>

using namespace koinos;

struct mempool_fixture
{
   mempool_fixture()
   {
      std::string seed1 = "alpha bravo charlie delta";
      _key1 = crypto::private_key::regenerate( crypto::hash( CRYPTO_SHA2_256_ID, seed1 ) );

      std::string seed2 = "echo foxtrot golf hotel";
      _key2 = crypto::private_key::regenerate( crypto::hash( CRYPTO_SHA2_256_ID, seed2 ) );

      std::string seed3 = "india juliet kilo lima";
      _key3 = crypto::private_key::regenerate( crypto::hash( CRYPTO_SHA2_256_ID, seed3 ) );
   }

   multihash sign( crypto::private_key& key, protocol::transaction& t )
   {
      auto digest = crypto::hash( CRYPTO_SHA2_256_ID, t.active_data.get_const_native() );
      auto signature = key.sign_compact( digest );
      t.signature_data = variable_blob( signature.begin(), signature.end() );
      return digest;
   }

   crypto::private_key                     _key1;
   crypto::private_key                     _key2;
   crypto::private_key                     _key3;
};

BOOST_FIXTURE_TEST_SUITE( mempool_tests, mempool_fixture )

BOOST_AUTO_TEST_CASE( mempool_basic_test )
{
   mempool::mempool mempool;

   protocol::transaction t1;
   t1.active_data->resource_limit = 10;
   auto t1_id = sign( _key1, t1 );

   BOOST_TEST_MESSAGE( "adding pending transaction" );
   auto payer = pack::to_variable_blob( _key1.get_public_key().to_address() );
   auto max_payer_resources = koinos::uint128( 1000000000000 );
   auto trx_resource_limit = t1.active_data->resource_limit;
   mempool.add_pending_transaction( t1_id, t1, block_height_type{ 1 }, payer, max_payer_resources, trx_resource_limit );

   BOOST_TEST_MESSAGE( "adding duplicate pending transaction" );
   BOOST_REQUIRE_THROW( mempool.add_pending_transaction( t1_id, t1, block_height_type{ 2 }, payer, max_payer_resources, trx_resource_limit ), mempool::pending_transaction_insertion_failure );

   BOOST_TEST_MESSAGE( "checking pending transaction list" );
   {
      auto pending_txs = mempool.get_pending_transactions();
      BOOST_TEST_MESSAGE( "checking pending transactions size" );
      BOOST_REQUIRE_EQUAL( pending_txs.size(), 1 );
      BOOST_TEST_MESSAGE( "checking pending transaction id" );
      BOOST_REQUIRE_EQUAL( crypto::hash( CRYPTO_SHA2_256_ID, pending_txs[0].active_data.get_const_native() ), t1_id );
   }

   BOOST_TEST_MESSAGE( "pending transaction existence check" );
   BOOST_REQUIRE_EQUAL( mempool.has_pending_transaction( t1_id ), true );

   protocol::transaction t2;
   t2.active_data->resource_limit = 1000000000000;
   auto t2_id = sign( _key1, t2 );

   BOOST_TEST_MESSAGE( "adding pending transaction that exceeds accout resources" );
   payer = pack::to_variable_blob( _key1.get_public_key().to_address() );
   max_payer_resources = 1000000000000;
   trx_resource_limit = t2.active_data->resource_limit;
   BOOST_REQUIRE_THROW( mempool.add_pending_transaction( t2_id, t2, block_height_type{ 3 }, payer, max_payer_resources, trx_resource_limit ), mempool::pending_transaction_exceeds_resources );

   BOOST_TEST_MESSAGE( "removing pending transaction" );
   mempool.remove_pending_transaction( t1_id );

   BOOST_TEST_MESSAGE( "checking pending transaction list" );
   {
      auto pending_txs = mempool.get_pending_transactions();
      BOOST_TEST_MESSAGE( "checking pending transactions size" );
      BOOST_REQUIRE_EQUAL( pending_txs.size(), 0 );
   }

   BOOST_TEST_MESSAGE( "pending transaction existence check" );
   BOOST_REQUIRE_EQUAL( mempool.has_pending_transaction( t1_id ), false );
}

BOOST_AUTO_TEST_CASE( pending_transaction_pagination )
{
   mempool::mempool mempool;
   protocol::transaction trx;
   multihash trx_id;
   mempool::account_type payer;
   uint128 max_payer_resources;
   uint128 trx_resource_limit;

   for( uint64_t i = 0; i < MAX_PENDING_TRANSACTION_REQUEST * 2 + 1; i++ )
   {
      trx.active_data.make_mutable();
      trx.active_data->resource_limit = 10 * i;
      trx_id = sign( _key1, trx );

      payer = pack::to_variable_blob( _key1.get_public_key().to_address() );
      max_payer_resources = 1000000000000;
      trx_resource_limit = trx_resource_limit = trx.active_data->resource_limit;
      mempool.add_pending_transaction( trx_id, trx, block_height_type( i ), payer, max_payer_resources, trx_resource_limit );
   }

   BOOST_REQUIRE_THROW( mempool.get_pending_transactions( multihash(), MAX_PENDING_TRANSACTION_REQUEST + 1 ), mempool::pending_transaction_request_overflow );

   auto pending_trxs = mempool.get_pending_transactions();
   BOOST_REQUIRE( pending_trxs.size() == MAX_PENDING_TRANSACTION_REQUEST );
   for( uint64_t i = 0; i < MAX_PENDING_TRANSACTION_REQUEST; i++ )
   {
      BOOST_CHECK_EQUAL( pending_trxs[i].active_data.get_const_native().resource_limit, 10 * i );
   }

   auto last_id = crypto::hash( CRYPTO_SHA2_256_ID, pending_trxs.rbegin()->active_data.get_const_native() );
   pending_trxs = mempool.get_pending_transactions( last_id, MAX_PENDING_TRANSACTION_REQUEST / 2 );
   BOOST_REQUIRE( pending_trxs.size() == MAX_PENDING_TRANSACTION_REQUEST / 2 );
   for( uint64_t i = 0; i < MAX_PENDING_TRANSACTION_REQUEST / 2; i++ )
   {
      BOOST_CHECK_EQUAL( pending_trxs[i].active_data.get_const_native().resource_limit, 10 * (i + MAX_PENDING_TRANSACTION_REQUEST) );
   }

   last_id = crypto::hash( CRYPTO_SHA2_256_ID, pending_trxs.rbegin()->active_data.get_const_native() );
   pending_trxs = mempool.get_pending_transactions( last_id );
   BOOST_REQUIRE( pending_trxs.size() == (MAX_PENDING_TRANSACTION_REQUEST + 1) / 2 + 1 );
   for( uint64_t i = 0; i < MAX_PENDING_TRANSACTION_REQUEST / 2; i++ )
   {
      BOOST_CHECK_EQUAL( pending_trxs[i].active_data.get_const_native().resource_limit, 10 * (i + MAX_PENDING_TRANSACTION_REQUEST + (MAX_PENDING_TRANSACTION_REQUEST + 1) / 2) );
   }
}

BOOST_AUTO_TEST_CASE( pending_transaction_pruning )
{
   // Add payerA transactions to blocks 1 and 2
   // Add payerB transaction to block 1
   // Add payerC transaction to block 2
   // Prune 1, payerA trx2 and payerC trx exist
   // Prune 2, no trx exist
   mempool::mempool mempool;
   protocol::transaction trx;
   multihash trx_id;
   mempool::account_type payer;
   uint128 max_payer_resources;
   uint128 trx_resource_limit;

   trx.active_data.make_mutable();
   trx.active_data->resource_limit = 1;
   trx_id = sign( _key1, trx );
   payer = pack::to_variable_blob( _key1.get_public_key().to_address() );
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx_resource_limit = trx.active_data->resource_limit;
   mempool.add_pending_transaction( trx_id, trx, block_height_type( 1 ), payer, max_payer_resources, trx_resource_limit );

   trx.active_data.make_mutable();
   trx.active_data->resource_limit = 2;
   trx_id = sign( _key2, trx );
   payer = pack::to_variable_blob( _key2.get_public_key().to_address() );
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx_resource_limit = trx.active_data->resource_limit;
   mempool.add_pending_transaction( trx_id, trx, block_height_type( 1 ), payer, max_payer_resources, trx_resource_limit );

   trx.active_data.make_mutable();
   trx.active_data->resource_limit = 3;
   trx_id = sign( _key1, trx );
   payer = pack::to_variable_blob( _key1.get_public_key().to_address() );
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx_resource_limit = trx.active_data->resource_limit;
   mempool.add_pending_transaction( trx_id, trx, block_height_type( 2 ), payer, max_payer_resources, trx_resource_limit );

   trx.active_data.make_mutable();
   trx.active_data->resource_limit = 4;
   trx_id = sign( _key3, trx );
   payer = pack::to_variable_blob( _key3.get_public_key().to_address() );
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx_resource_limit = trx.active_data->resource_limit;
   mempool.add_pending_transaction( trx_id, trx, block_height_type( 2 ), payer, max_payer_resources, trx_resource_limit );

   auto pending_trxs = mempool.get_pending_transactions();
   BOOST_CHECK_EQUAL( mempool.payer_entries_size(), 3 );
   BOOST_REQUIRE_EQUAL( pending_trxs.size(), 4 );
   for( size_t i = 0; i < pending_trxs.size(); i++ )
      BOOST_REQUIRE_EQUAL( pending_trxs[i].active_data.get_const_native().resource_limit, i + 1 );

   mempool.prune( block_height_type( 1 ) );
   pending_trxs = mempool.get_pending_transactions();
   BOOST_CHECK_EQUAL( mempool.payer_entries_size(), 2 );
   BOOST_REQUIRE_EQUAL( pending_trxs.size(), 2 );
   for( size_t i = 0; i < pending_trxs.size(); i++ )
      BOOST_REQUIRE_EQUAL( pending_trxs[i].active_data.get_const_native().resource_limit, i + 3 );

   mempool.prune( block_height_type( 2 ) );
   pending_trxs = mempool.get_pending_transactions();
   BOOST_CHECK_EQUAL( mempool.payer_entries_size(), 0 );
   BOOST_REQUIRE_EQUAL( pending_trxs.size(), 0 );
}

BOOST_AUTO_TEST_CASE( pending_transaction_dynamic_max_resources )
{
   mempool::mempool mempool;
   protocol::transaction trx;
   multihash trx_id;
   mempool::account_type payer;
   uint128 max_payer_resources;
   uint128 trx_resource_limit;

   BOOST_TEST_MESSAGE( "gain max account resources" );

   trx.active_data.make_mutable();
   trx.active_data->resource_limit = 1000000000000;
   trx_id = sign( _key1, trx );

   payer = pack::to_variable_blob( _key1.get_public_key().to_address() );
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx_resource_limit = trx.active_data->resource_limit;

   mempool.add_pending_transaction( trx_id, trx, block_height_type( 1 ), payer, max_payer_resources, trx_resource_limit );

   for ( unsigned int i = 2; i < 10; i++ )
   {
      max_payer_resources = max_payer_resources + i * 10;
      trx_resource_limit = i * 10;

      trx.active_data->resource_limit = trx_resource_limit;
      trx_id = sign( _key1, trx );

      mempool.add_pending_transaction( trx_id, trx, block_height_type( i ), payer, max_payer_resources, trx_resource_limit );
   }

   max_payer_resources = max_payer_resources + 99;
   trx_resource_limit = 100;

   trx.active_data->resource_limit = trx_resource_limit;
   trx_id = sign( _key1, trx );

   BOOST_REQUIRE_THROW(
      mempool.add_pending_transaction( trx_id, trx, block_height_type( 10 ), payer, max_payer_resources, trx_resource_limit ),
      mempool::pending_transaction_exceeds_resources
   );

   BOOST_TEST_MESSAGE( "lose max account resources" );

   trx.active_data.make_mutable();
   trx.active_data->resource_limit = 999999999980;
   trx_id = sign( _key2, trx );

   payer = pack::to_variable_blob( _key2.get_public_key().to_address() );
   max_payer_resources = 1000000000000;
   trx_resource_limit = trx_resource_limit = trx.active_data->resource_limit;

   mempool.add_pending_transaction( trx_id, trx, block_height_type( 1 ), payer, max_payer_resources, trx_resource_limit );

   max_payer_resources = 999999999990;
   trx_resource_limit = 10;

   trx.active_data->resource_limit = trx_resource_limit;
   trx_id = sign( _key2, trx );

   mempool.add_pending_transaction( trx_id, trx, block_height_type( 2 ), payer, max_payer_resources, trx_resource_limit );

   trx_resource_limit = 1;

   trx.active_data->resource_limit = trx_resource_limit;
   trx_id = sign( _key2, trx );

   BOOST_REQUIRE_THROW(
      mempool.add_pending_transaction( trx_id, trx, block_height_type( 3 ), payer, max_payer_resources, trx_resource_limit ),
      mempool::pending_transaction_exceeds_resources
   );
}

BOOST_AUTO_TEST_SUITE_END()
