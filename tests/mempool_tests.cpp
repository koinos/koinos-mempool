#include <boost/test/unit_test.hpp>

#include <koinos/mempool/mempool.hpp>

#include <koinos/chain/value.pb.h>
#include <koinos/crypto/elliptic.hpp>
#include <koinos/crypto/multihash.hpp>
#include <koinos/protocol/protocol.pb.h>
#include <koinos/util/conversion.hpp>
#include <koinos/util/hex.hpp>

#include <chrono>
#include <memory>

using namespace koinos;
using namespace std::chrono_literals;
using namespace std::string_literals;

struct mempool_fixture
{
  mempool_fixture()
  {
    initialize_logging( "mempool_tests", {}, "info" );

    std::string seed1 = "alpha bravo charlie delta";
    _key1             = crypto::private_key::regenerate( crypto::hash( crypto::multicodec::sha2_256, seed1 ) );

    std::string seed2 = "echo foxtrot golf hotel";
    _key2             = crypto::private_key::regenerate( crypto::hash( crypto::multicodec::sha2_256, seed2 ) );

    std::string seed3 = "india juliet kilo lima";
    _key3             = crypto::private_key::regenerate( crypto::hash( crypto::multicodec::sha2_256, seed3 ) );

    std::string seed4 = "mike november oscar papa";
    _key4             = crypto::private_key::regenerate( crypto::hash( crypto::multicodec::sha2_256, seed4 ) );
  }

  virtual ~mempool_fixture()
  {
    boost::log::core::get()->remove_all_sinks();
  }

  mempool::transaction_id_type sign( crypto::private_key& key, protocol::transaction& t )
  {
    auto digest    = crypto::hash( crypto::multicodec::sha2_256, t.header() );
    auto signature = key.sign_compact( digest );
    std::string sig_str;
    sig_str.resize( signature.size() );
    std::transform( signature.begin(),
                    signature.end(),
                    sig_str.begin(),
                    []( std::byte b )
                    {
                      return char( b );
                    } );
    t.add_signatures( std::move( sig_str ) );
    return util::converter::as< std::string >( digest );
  }

  crypto::private_key _key1;
  crypto::private_key _key2;
  crypto::private_key _key3;
  crypto::private_key _key4;
};

mempool::pending_transaction make_pending_transaction( const protocol::transaction& trx,
                                                       uint64_t disk_storage_used,
                                                       uint64_t network_bandwidth_used,
                                                       uint64_t compute_bandwidth_used )
{
  mempool::pending_transaction pending_trx;
  *pending_trx.mutable_transaction() = trx;
  pending_trx.set_disk_storage_used( disk_storage_used );
  pending_trx.set_network_bandwidth_used( network_bandwidth_used );
  pending_trx.set_compute_bandwidth_used( compute_bandwidth_used );

  return pending_trx;
}

BOOST_FIXTURE_TEST_SUITE( mempool_tests, mempool_fixture )

BOOST_AUTO_TEST_CASE( mempool_basic_test )
{
  mempool::mempool mempool;

  auto payer                   = _key1.get_public_key().to_address_bytes();
  uint64_t max_payer_resources = 1'000'000'000'000ull;
  chain::value_type nonce_value;
  nonce_value.set_uint64_value( 1 );

  protocol::transaction t1;
  t1.mutable_header()->set_rc_limit( 10 );
  t1.mutable_header()->set_payer( payer );
  t1.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  t1.set_id( sign( _key1, t1 ) );

  auto missing_tid = util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, "missing"s ) );

  BOOST_TEST_MESSAGE( "adding pending transaction" );
  auto trx_resource_limit = t1.header().rc_limit();
  auto rc_used            = mempool.add_pending_transaction( make_pending_transaction( t1, 1, 2, 3 ),
                                                  std::chrono::system_clock::now(),
                                                  max_payer_resources );
  BOOST_CHECK_EQUAL( t1.header().rc_limit(), rc_used );

  BOOST_CHECK_EQUAL( mempool.get_reserved_account_rc( payer ), rc_used );

  BOOST_TEST_MESSAGE( "adding duplicate pending transaction" );
  BOOST_REQUIRE_THROW( mempool.add_pending_transaction( make_pending_transaction( t1, 1, 2, 3 ),
                                                        std::chrono::system_clock::now(),
                                                        max_payer_resources ),
                       mempool::pending_transaction_insertion_failure );

  BOOST_TEST_MESSAGE( "checking payer was not charged for failed pending transaction" );
  BOOST_REQUIRE(
    mempool.check_pending_account_resources( payer, max_payer_resources, max_payer_resources - trx_resource_limit ) );

  BOOST_TEST_MESSAGE( "checking pending transaction list" );
  {
    auto pending_txs = mempool.get_pending_transactions();
    BOOST_TEST_MESSAGE( "checking pending transactions size" );
    BOOST_REQUIRE_EQUAL( pending_txs.size(), 1 );
    BOOST_TEST_MESSAGE( "checking pending transaction id" );
    BOOST_REQUIRE_EQUAL( util::converter::as< std::string >(
                           crypto::hash( crypto::multicodec::sha2_256, pending_txs[ 0 ].transaction().header() ) ),
                         t1.id() );
    BOOST_REQUIRE_EQUAL( pending_txs[ 0 ].disk_storage_used(), 1 );
    BOOST_REQUIRE_EQUAL( pending_txs[ 0 ].network_bandwidth_used(), 2 );
    BOOST_REQUIRE_EQUAL( pending_txs[ 0 ].compute_bandwidth_used(), 3 );
  }
  {
    auto pending_txs = mempool.get_pending_transactions( { t1.id(), missing_tid } );
    BOOST_TEST_MESSAGE( "checking pending transactions size" );
    BOOST_REQUIRE_EQUAL( pending_txs.size(), 1 );
    BOOST_TEST_MESSAGE( "checking pending transaction id" );
    BOOST_REQUIRE_EQUAL( pending_txs[ 0 ].transaction().id(), t1.id() );
  }

  BOOST_TEST_MESSAGE( "pending transaction existence check" );
  BOOST_REQUIRE( mempool.has_pending_transaction( t1.id() ) );

  payer = _key1.get_public_key().to_address_bytes();
  nonce_value.set_uint64_value( 2 );

  protocol::transaction t2;
  t2.mutable_header()->set_rc_limit( 1'000'000'000'000 );
  t2.mutable_header()->set_payer( payer );
  t2.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  t2.set_id( sign( _key1, t2 ) );

  BOOST_TEST_MESSAGE( "adding pending transaction that exceeds accout resources" );
  max_payer_resources = 1'000'000'000'000;

  BOOST_REQUIRE_THROW( mempool.add_pending_transaction( make_pending_transaction( t2, 1, 1, 1 ),
                                                        std::chrono::system_clock::now(),
                                                        max_payer_resources ),
                       mempool::pending_transaction_exceeds_resources );

  BOOST_TEST_MESSAGE( "removing pending transactions" );
  mempool.remove_pending_transactions( std::vector< mempool::transaction_id_type >{ t1.id() } );

  BOOST_CHECK_EQUAL( mempool.get_reserved_account_rc( payer ), 0 );

  BOOST_TEST_MESSAGE( "checking pending transaction list" );
  {
    auto pending_txs = mempool.get_pending_transactions();
    BOOST_TEST_MESSAGE( "checking pending transactions size" );
    BOOST_REQUIRE_EQUAL( pending_txs.size(), 0 );
  }
  {
    auto pending_txs = mempool.get_pending_transactions( { t1.id(), missing_tid } );
    BOOST_TEST_MESSAGE( "checking pending transactions size" );
    BOOST_REQUIRE_EQUAL( pending_txs.size(), 0 );
  }

  BOOST_TEST_MESSAGE( "pending transaction existence check" );
  BOOST_REQUIRE( !mempool.has_pending_transaction( t1.id() ) );
}

BOOST_AUTO_TEST_CASE( pending_transaction_pagination )
{
  mempool::mempool mempool;
  mempool::account_type payer = _key1.get_public_key().to_address_bytes();
  ;
  uint64_t max_payer_resources = 1'000'000'000'000;
  uint64_t trx_resource_limit;
  chain::value_type nonce_value;
  uint64_t rc_used = 0;

  for( uint64_t i = 0; i < mempool::constants::max_request_limit + 1; i++ )
  {
    protocol::transaction trx;
    nonce_value.set_uint64_value( i + 1 );

    trx.mutable_header()->set_rc_limit( 10 * i );
    trx.mutable_header()->set_payer( payer );
    trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
    trx.set_id( sign( _key1, trx ) );

    rc_used += trx.header().rc_limit();

    auto rc = mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                               std::chrono::system_clock::now(),
                                               max_payer_resources );
    BOOST_CHECK_EQUAL( rc, rc_used );
  }

  BOOST_REQUIRE_THROW( mempool.get_pending_transactions( mempool::constants::max_request_limit + 1 ),
                       mempool::pending_transaction_request_overflow );

  auto pending_trxs = mempool.get_pending_transactions( mempool::constants::max_request_limit );
  BOOST_REQUIRE( pending_trxs.size() == mempool::constants::max_request_limit );
  for( uint64_t i = 0; i < pending_trxs.size(); i++ )
  {
    BOOST_CHECK_EQUAL( pending_trxs[ i ].transaction().header().rc_limit(), 10 * i );
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
  mempool::account_type payer;
  uint64_t max_payer_resources;
  uint64_t trx_resource_limit;
  chain::value_type nonce_value;

  auto now = std::chrono::system_clock::now();

  payer               = _key1.get_public_key().to_address_bytes();
  max_payer_resources = 1'000'000'000'000;
  nonce_value.set_uint64_value( 1 );
  trx.mutable_header()->set_rc_limit( 1 );
  trx.mutable_header()->set_payer( payer );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key1, trx ) );

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ), now, max_payer_resources );

  payer               = _key2.get_public_key().to_address_bytes();
  max_payer_resources = 1'000'000'000'000;
  nonce_value.set_uint64_value( 2 );
  trx.mutable_header()->set_rc_limit( 2 );
  trx.mutable_header()->set_payer( payer );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key2, trx ) );

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ), now, max_payer_resources );

  payer               = _key1.get_public_key().to_address_bytes();
  max_payer_resources = 1'000'000'000'000;
  nonce_value.set_uint64_value( 3 );
  trx.mutable_header()->set_rc_limit( 3 );
  trx.mutable_header()->set_payer( payer );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key1, trx ) );

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ), now + 1s, max_payer_resources );
  payer               = _key3.get_public_key().to_address_bytes();
  max_payer_resources = 1'000'000'000'000;
  nonce_value.set_uint64_value( 4 );
  trx.mutable_header()->set_rc_limit( 4 );
  trx.mutable_header()->set_payer( payer );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key3, trx ) );

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ), now + 1s, max_payer_resources );

  auto pending_trxs = mempool.get_pending_transactions();
  BOOST_REQUIRE_EQUAL( pending_trxs.size(), 4 );
  for( size_t i = 0; i < pending_trxs.size(); i++ )
  {
    BOOST_REQUIRE_EQUAL( pending_trxs[ i ].transaction().header().rc_limit(), i + 1 );
  }

  mempool.prune( 1s, now + 1s );
  pending_trxs = mempool.get_pending_transactions();
  BOOST_REQUIRE_EQUAL( pending_trxs.size(), 2 );
  for( size_t i = 0; i < pending_trxs.size(); i++ )
  {
    BOOST_REQUIRE_EQUAL( pending_trxs[ i ].transaction().header().rc_limit(), i + 3 );
  }

  mempool.prune( 1s, now + 2s );
  pending_trxs = mempool.get_pending_transactions();
  BOOST_REQUIRE_EQUAL( pending_trxs.size(), 0 );
}

BOOST_AUTO_TEST_CASE( pending_transaction_dynamic_max_resources )
{
  mempool::mempool mempool;
  protocol::transaction trx;
  mempool::account_type payer;
  uint64_t max_payer_resources, rc_pending;
  uint64_t trx_resource_limit;
  chain::value_type nonce_value;

  BOOST_TEST_MESSAGE( "gain max account resources" );

  payer               = _key1.get_public_key().to_address_bytes();
  max_payer_resources = 1'000'000'000'000;
  rc_pending          = 1'000'000'000'000;
  nonce_value.set_uint64_value( 1 );
  trx.mutable_header()->set_rc_limit( rc_pending );
  trx.mutable_header()->set_payer( payer );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key1, trx ) );

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  BOOST_CHECK_EQUAL( mempool.get_reserved_account_rc( payer ), 1'000'000'000'000 );

  for( unsigned int i = 2; i < 10; i++ )
  {
    max_payer_resources = max_payer_resources + i * 10;
    trx_resource_limit  = i * 10;
    nonce_value.set_uint64_value( i );

    trx.mutable_header()->set_rc_limit( trx_resource_limit );
    trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
    trx.set_id( sign( _key1, trx ) );

    mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                     std::chrono::system_clock::now(),
                                     max_payer_resources );

    rc_pending += trx_resource_limit;
    BOOST_CHECK_EQUAL( mempool.get_reserved_account_rc( payer ), rc_pending );
  }

  max_payer_resources = max_payer_resources + 99;
  trx_resource_limit  = 100;
  nonce_value.set_uint64_value( 10 );

  trx.mutable_header()->set_rc_limit( trx_resource_limit );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key1, trx ) );

  BOOST_REQUIRE_THROW( mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                                        std::chrono::system_clock::now(),
                                                        max_payer_resources ),
                       mempool::pending_transaction_exceeds_resources );

  BOOST_CHECK_EQUAL( mempool.get_reserved_account_rc( payer ), rc_pending );

  BOOST_TEST_MESSAGE( "lose max account resources" );

  payer               = _key2.get_public_key().to_address_bytes();
  max_payer_resources = 1'000'000'000'000;
  rc_pending          = 999'999'999'980;
  nonce_value.set_uint64_value( 11 );

  trx.mutable_header()->set_rc_limit( rc_pending );
  trx.mutable_header()->set_payer( payer );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key2, trx ) );

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  BOOST_CHECK_EQUAL( mempool.get_reserved_account_rc( payer ), rc_pending );

  max_payer_resources  = 999'999'999'990;
  trx_resource_limit   = 10;
  rc_pending          += trx_resource_limit;
  nonce_value.set_uint64_value( 12 );

  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.mutable_header()->set_rc_limit( trx_resource_limit );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key2, trx ) );

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  BOOST_CHECK_EQUAL( mempool.get_reserved_account_rc( payer ), rc_pending );

  trx_resource_limit = 1;
  nonce_value.set_uint64_value( 12 );

  trx.mutable_header()->set_rc_limit( trx_resource_limit );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key2, trx ) );

  BOOST_REQUIRE_THROW( mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                                        std::chrono::system_clock::now(),
                                                        max_payer_resources ),
                       mempool::pending_transaction_exceeds_resources );

  BOOST_CHECK_EQUAL( mempool.get_reserved_account_rc( payer ), rc_pending );
}

BOOST_AUTO_TEST_CASE( fork_test )
{
  mempool::mempool mempool( state_db::fork_resolution_algorithm::block_time );
  broadcast::block_accepted bam;

  auto payer1           = _key1.get_public_key().to_address_bytes();
  auto payer2           = _key2.get_public_key().to_address_bytes();
  auto payer3           = _key3.get_public_key().to_address_bytes();
  auto payer4           = _key4.get_public_key().to_address_bytes();
  uint64_t max_payer_rc = 1'000'000'000'000ull;
  chain::value_type nonce_value;
  nonce_value.set_uint64_value( 1 );

  protocol::transaction t1;
  t1.mutable_header()->set_rc_limit( 10 );
  t1.mutable_header()->set_payer( payer1 );
  t1.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  t1.set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, t1.header() ) ) );

  protocol::transaction t2;
  t2.mutable_header()->set_rc_limit( 10 );
  t2.mutable_header()->set_payer( payer2 );
  t2.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  t2.set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, t2.header() ) ) );

  protocol::transaction t3;
  t3.mutable_header()->set_rc_limit( 10 );
  t3.mutable_header()->set_payer( payer3 );
  t3.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  t3.set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, t3.header() ) ) );

  protocol::transaction t4;
  t4.mutable_header()->set_rc_limit( 10 );
  t4.mutable_header()->set_payer( payer4 );
  t4.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  t4.set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, t4.header() ) ) );

  protocol::block b1;
  b1.mutable_header()->set_height( 1 );
  b1.mutable_header()->set_timestamp( 1 );
  b1.mutable_header()->set_signer( payer1 );
  b1.mutable_header()->set_previous(
    util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );
  b1.set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, b1.header() ) ) );

  protocol::block b2;
  b2.mutable_header()->set_height( 1 );
  b2.mutable_header()->set_timestamp( 2 );
  b2.mutable_header()->set_signer( payer2 );
  b2.mutable_header()->set_previous(
    util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );
  b2.set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, b2.header() ) ) );

  protocol::block b3;
  b3.mutable_header()->set_height( 2 );
  b3.mutable_header()->set_timestamp( 3 );
  b3.mutable_header()->set_signer( payer3 );
  b3.mutable_header()->set_previous( b1.id() );
  b3.set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, b3.header() ) ) );

  protocol::block b4;
  b4.mutable_header()->set_height( 2 );
  b4.mutable_header()->set_timestamp( 4 );
  b4.mutable_header()->set_signer( payer4 );
  b4.mutable_header()->set_previous( b1.id() );
  b4.set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, b4.header() ) ) );

  auto now = std::chrono::system_clock::now();

  mempool.add_pending_transaction( make_pending_transaction( t1, 1, 1, 1 ), now, max_payer_rc );
  BOOST_REQUIRE_EQUAL( 1, mempool.get_pending_transactions().size() );
  BOOST_REQUIRE_EQUAL( t1.id(), mempool.get_pending_transactions()[ 0 ].transaction().id() );

  *b2.add_transactions() = t1;
  *bam.mutable_block()   = b2;
  mempool.handle_block( bam );

  BOOST_REQUIRE_EQUAL( 0, mempool.get_pending_transactions().size() );

  *bam.mutable_block() = b1;
  mempool.handle_block( bam );

  BOOST_REQUIRE_EQUAL( 1, mempool.get_pending_transactions().size() );

  mempool.add_pending_transaction( make_pending_transaction( t2, 1, 1, 1 ), now + 1s, max_payer_rc );
  BOOST_REQUIRE_EQUAL( 2, mempool.get_pending_transactions().size() );
  BOOST_REQUIRE_EQUAL( t1.id(), mempool.get_pending_transactions()[ 0 ].transaction().id() );
  BOOST_REQUIRE_EQUAL( t2.id(), mempool.get_pending_transactions()[ 1 ].transaction().id() );

  mempool.add_pending_transaction( make_pending_transaction( t3, 1, 1, 1 ), now + 2s, max_payer_rc );
  BOOST_REQUIRE_EQUAL( 3, mempool.get_pending_transactions().size() );
  BOOST_REQUIRE_EQUAL( t1.id(), mempool.get_pending_transactions()[ 0 ].transaction().id() );
  BOOST_REQUIRE_EQUAL( t2.id(), mempool.get_pending_transactions()[ 1 ].transaction().id() );
  BOOST_REQUIRE_EQUAL( t3.id(), mempool.get_pending_transactions()[ 2 ].transaction().id() );

  *b4.add_transactions() = t2;
  *bam.mutable_block()   = b4;
  mempool.handle_block( bam );

  BOOST_REQUIRE_EQUAL( 2, mempool.get_pending_transactions().size() );
  BOOST_REQUIRE_EQUAL( t1.id(), mempool.get_pending_transactions()[ 0 ].transaction().id() );
  BOOST_REQUIRE_EQUAL( t3.id(), mempool.get_pending_transactions()[ 1 ].transaction().id() );

  mempool.add_pending_transaction( make_pending_transaction( t4, 1, 1, 1 ), now + 3s, max_payer_rc );
  BOOST_REQUIRE_EQUAL( 3, mempool.get_pending_transactions().size() );
  BOOST_REQUIRE_EQUAL( t1.id(), mempool.get_pending_transactions()[ 0 ].transaction().id() );
  BOOST_REQUIRE_EQUAL( t3.id(), mempool.get_pending_transactions()[ 1 ].transaction().id() );
  BOOST_REQUIRE_EQUAL( t4.id(), mempool.get_pending_transactions()[ 2 ].transaction().id() );

  *bam.mutable_block() = b3;
  mempool.handle_block( bam );

  BOOST_REQUIRE_EQUAL( 4, mempool.get_pending_transactions().size() );
  BOOST_REQUIRE_EQUAL( t1.id(), mempool.get_pending_transactions()[ 0 ].transaction().id() );
  BOOST_REQUIRE_EQUAL( t2.id(), mempool.get_pending_transactions()[ 1 ].transaction().id() );
  BOOST_REQUIRE_EQUAL( t3.id(), mempool.get_pending_transactions()[ 2 ].transaction().id() );
  BOOST_REQUIRE_EQUAL( t4.id(), mempool.get_pending_transactions()[ 3 ].transaction().id() );
}

BOOST_AUTO_TEST_CASE( nonce_basic_test )
{
  // This test adds two transactions to the mempool,
  // one with the payee set and one without,
  // and then removes them, ensuring add_pending_transaction
  // and check_account_nonce work correctly
  mempool::mempool mempool;
  protocol::transaction trx;
  mempool::account_type payer;
  mempool::account_type payee;
  uint64_t max_payer_resources;
  uint64_t trx_resource_limit;
  chain::value_type nonce_value;

  payer               = _key1.get_public_key().to_address_bytes();
  payee               = _key2.get_public_key().to_address_bytes();
  max_payer_resources = 1'000'000'000'000;
  nonce_value.set_uint64_value( 1 );
  trx.mutable_header()->set_rc_limit( 10 );
  trx.mutable_header()->set_payer( payer );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key1, trx ) );
  auto trx1_id = trx.id();

  BOOST_REQUIRE( mempool.check_account_nonce( payer, trx.header().nonce() ) );
  BOOST_REQUIRE( mempool.check_account_nonce( payee, trx.header().nonce() ) );

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  BOOST_REQUIRE( !mempool.check_account_nonce( payer, trx.header().nonce() ) );
  BOOST_REQUIRE( mempool.check_account_nonce( payee, trx.header().nonce() ) );

  trx.mutable_header()->set_rc_limit( 20 );
  trx.set_id( sign( _key1, trx ) );

  BOOST_REQUIRE_THROW( mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                                        std::chrono::system_clock::now(),
                                                        max_payer_resources ),
                       mempool::pending_transaction_nonce_conflict );

  trx.mutable_header()->set_rc_limit( 10 );
  trx.mutable_header()->set_payee( payee );
  trx.set_id( sign( _key2, trx ) );
  auto trx2_id = trx.id();

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  BOOST_REQUIRE( !mempool.check_account_nonce( payer, trx.header().nonce() ) );
  BOOST_REQUIRE( !mempool.check_account_nonce( payee, trx.header().nonce() ) );

  trx.mutable_header()->set_rc_limit( 20 );
  trx.set_id( sign( _key2, trx ) );

  BOOST_REQUIRE_THROW( mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                                        std::chrono::system_clock::now(),
                                                        max_payer_resources ),
                       mempool::pending_transaction_nonce_conflict );

  mempool.remove_pending_transactions( std::vector< mempool::transaction_id_type >{ trx1_id, trx2_id } );

  BOOST_REQUIRE( mempool.check_account_nonce( payer, trx.header().nonce() ) );
  BOOST_REQUIRE( mempool.check_account_nonce( payee, trx.header().nonce() ) );
}

BOOST_AUTO_TEST_CASE( nonce_fork_test )
{
  // This test tests nonce tracking on different forks
  //
  // A transaction (t1) with nonce 1 will be pushed with blocks, A1, and B1
  // Pushing a second transaction (t2) with the same nonce should fail
  // A new block, A2 will be pushed, which contains t1 and will remove nonce 1 from A2

  mempool::mempool mempool;
  protocol::transaction t1, t2;
  mempool::account_type payer, producer1, producer2, producer3;
  uint64_t max_payer_resources;
  uint64_t trx_resource_limit;
  chain::value_type nonce_value;

  broadcast::block_accepted bam;
  producer1 = _key1.get_public_key().to_address_bytes();
  producer2 = _key2.get_public_key().to_address_bytes();
  producer3 = _key3.get_public_key().to_address_bytes();

  payer               = _key1.get_public_key().to_address_bytes();
  max_payer_resources = 1'000'000'000'000;
  nonce_value.set_uint64_value( 1 );
  t1.mutable_header()->set_rc_limit( 10 );
  t1.mutable_header()->set_payer( payer );
  t1.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  t1.set_id( sign( _key1, t1 ) );

  protocol::block a1;
  a1.mutable_header()->set_height( 1 );
  a1.mutable_header()->set_timestamp( 1 );
  a1.mutable_header()->set_signer( producer1 );
  a1.mutable_header()->set_previous(
    util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );
  auto a1_id = crypto::hash( crypto::multicodec::sha2_256, a1.header() );
  a1.set_id( util::converter::as< std::string >( a1_id ) );

  protocol::block b1;
  b1.mutable_header()->set_height( 1 );
  b1.mutable_header()->set_timestamp( 1 );
  b1.mutable_header()->set_signer( producer2 );
  b1.mutable_header()->set_previous(
    util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );
  auto b1_id = crypto::hash( crypto::multicodec::sha2_256, b1.header() );
  b1.set_id( util::converter::as< std::string >( b1_id ) );

  *bam.mutable_block() = a1;
  mempool.handle_block( bam );

  *bam.mutable_block() = b1;
  mempool.handle_block( bam );

  mempool.add_pending_transaction( make_pending_transaction( t1, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  BOOST_REQUIRE( !mempool.check_account_nonce( payer, t1.header().nonce(), a1_id ) );
  BOOST_REQUIRE( !mempool.check_account_nonce( payer, t1.header().nonce(), b1_id ) );
  BOOST_REQUIRE( mempool.has_pending_transaction( t1.id(), a1_id ) );
  BOOST_REQUIRE( mempool.has_pending_transaction( t1.id(), b1_id ) );

  t2.mutable_header()->set_rc_limit( 20 );
  t2.mutable_header()->set_payer( payer );
  t2.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  t2.set_id( sign( _key1, t2 ) );

  BOOST_REQUIRE_THROW( mempool.add_pending_transaction( make_pending_transaction( t2, 1, 1, 1 ),
                                                        std::chrono::system_clock::now(),
                                                        max_payer_resources ),
                       mempool::pending_transaction_nonce_conflict );

  protocol::block a2;
  a2.mutable_header()->set_height( 2 );
  a2.mutable_header()->set_timestamp( 2 );
  a2.mutable_header()->set_signer( producer1 );
  a2.mutable_header()->set_previous( a1.id() );
  *a2.add_transactions() = t1;
  auto a2_id             = crypto::hash( crypto::multicodec::sha2_256, a2.header() );
  a2.set_id( util::converter::as< std::string >( a2_id ) );

  *bam.mutable_block() = a2;
  bam.set_live( true );
  mempool.handle_block( bam );

  BOOST_REQUIRE( mempool.check_account_nonce( payer, t1.header().nonce(), a2_id ) );
}

BOOST_AUTO_TEST_CASE( pending_rc_fork_test )
{
  // This test tests pending rc tracking on different forks
  // The method should return the highest pending rc on all forks

  mempool::mempool mempool;
  protocol::transaction t1, t2;
  mempool::account_type payer, producer1, producer2, producer3;
  uint64_t max_payer_resources;
  uint64_t trx_resource_limit, t1_rc_limit;
  chain::value_type nonce_value;

  broadcast::block_accepted bam;
  producer1 = _key1.get_public_key().to_address_bytes();
  producer2 = _key2.get_public_key().to_address_bytes();
  producer3 = _key3.get_public_key().to_address_bytes();

  payer               = _key1.get_public_key().to_address_bytes();
  max_payer_resources = 1'000'000'000'000;
  t1_rc_limit         = 10;
  nonce_value.set_uint64_value( 1 );
  t1.mutable_header()->set_rc_limit( t1_rc_limit );
  t1.mutable_header()->set_payer( payer );
  t1.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  t1.set_id( sign( _key1, t1 ) );

  mempool.add_pending_transaction( make_pending_transaction( t1, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  protocol::block a1;
  a1.mutable_header()->set_height( 1 );
  a1.mutable_header()->set_timestamp( 1 );
  a1.mutable_header()->set_signer( producer1 );
  a1.mutable_header()->set_previous(
    util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );
  auto a1_id = crypto::hash( crypto::multicodec::sha2_256, a1.header() );
  a1.set_id( util::converter::as< std::string >( a1_id ) );

  protocol::block b1;
  b1.mutable_header()->set_height( 1 );
  b1.mutable_header()->set_timestamp( 1 );
  b1.mutable_header()->set_signer( producer2 );
  b1.mutable_header()->set_previous(
    util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );
  auto b1_id             = crypto::hash( crypto::multicodec::sha2_256, b1.header() );
  *b1.add_transactions() = t1;
  b1.set_id( util::converter::as< std::string >( b1_id ) );

  *bam.mutable_block() = a1;
  mempool.handle_block( bam );

  *bam.mutable_block() = b1;
  mempool.handle_block( bam );

  BOOST_CHECK_EQUAL( mempool.get_reserved_account_rc( payer ), t1_rc_limit );
}

BOOST_AUTO_TEST_CASE( pending_nonce_test )
{
  // This test ensures the correct pending nonce is returned.

  mempool::mempool mempool;
  protocol::transaction trx;
  mempool::account_type payer  = _key1.get_public_key().to_address_bytes();
  uint64_t max_payer_resources = 1'000'000'000'000;
  chain::value_type nonce_value, expected_nonce;

  nonce_value.set_uint64_value( 1 );
  trx.mutable_header()->set_rc_limit( 1 );
  trx.mutable_header()->set_payer( payer );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key1, trx ) );
  auto trx_1_id = trx.id();

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  BOOST_CHECK_EQUAL( mempool.get_pending_nonce( payer ), util::converter::as< std::string >( nonce_value ) );

  // payer2 is before payer1. payer3 is after payer1
  mempool::account_type payer2 = _key2.get_public_key().to_address_bytes();
  mempool::account_type payer3 = _key3.get_public_key().to_address_bytes();

  nonce_value.clear_uint64_value();
  BOOST_CHECK_EQUAL( mempool.get_pending_nonce( payer2 ), util::converter::as< std::string >( nonce_value ) );
  BOOST_CHECK_EQUAL( mempool.get_pending_nonce( payer3 ), util::converter::as< std::string >( nonce_value ) );

  // Check adding a transaction on a new node.
  protocol::block b;
  b.mutable_header()->set_height( 1 );
  b.mutable_header()->set_timestamp( 1 );
  b.mutable_header()->set_signer( payer );
  b.mutable_header()->set_previous(
    util::converter::as< std::string >( crypto::multihash::zero( crypto::multicodec::sha2_256 ) ) );
  *b.add_transactions() = trx;
  b.set_id( util::converter::as< std::string >( crypto::hash( crypto::multicodec::sha2_256, b.header() ) ) );

  broadcast::block_accepted bam;
  *bam.mutable_block() = b;
  mempool.handle_block( bam );

  nonce_value.set_uint64_value( 1 );
  BOOST_CHECK_EQUAL( mempool.get_pending_nonce( payer ), "" );
  BOOST_CHECK_EQUAL( mempool.get_pending_nonce( payer, crypto::multihash::zero( crypto::multicodec::sha2_256 ) ),
                     util::converter::as< std::string >( nonce_value ) );

  nonce_value.set_uint64_value( 2 );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key1, trx ) );
  auto trx_2_id = trx.id();

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  BOOST_CHECK_EQUAL( mempool.get_pending_nonce( payer ), util::converter::as< std::string >( nonce_value ) );
  BOOST_CHECK_EQUAL( mempool.get_pending_nonce( payer, crypto::multihash::zero( crypto::multicodec::sha2_256 ) ),
                     util::converter::as< std::string >( nonce_value ) );

  BOOST_CHECK_EQUAL( mempool.get_pending_transaction_count( payer ), 1 );
  BOOST_CHECK_EQUAL( mempool.get_pending_transaction_count( payer2 ), 0 );
  BOOST_CHECK_EQUAL( mempool.get_pending_transaction_count( payer3 ), 0 );

  BOOST_CHECK_EQUAL(
    mempool.get_pending_transaction_count( payer, crypto::multihash::zero( crypto::multicodec::sha2_256 ) ),
    2 );
  BOOST_CHECK_EQUAL(
    mempool.get_pending_transaction_count( payer2, crypto::multihash::zero( crypto::multicodec::sha2_256 ) ),
    0 );
  BOOST_CHECK_EQUAL(
    mempool.get_pending_transaction_count( payer3, crypto::multihash::zero( crypto::multicodec::sha2_256 ) ),
    0 );

  mempool.remove_pending_transactions( { trx_2_id } );
  BOOST_CHECK_EQUAL( mempool.get_pending_transaction_count( payer ), 0 );
}

BOOST_AUTO_TEST_CASE( nonce_limits )
{
  mempool::mempool mempool;
  protocol::transaction trx;
  mempool::account_type payer  = _key1.get_public_key().to_address_bytes();
  uint64_t max_payer_resources = 1'000'000'000'000;
  chain::value_type nonce_value, expected_nonce;

  nonce_value.set_uint64_value( 3'071 );
  trx.mutable_header()->set_rc_limit( 1 );
  trx.mutable_header()->set_payer( payer );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key1, trx ) );

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  BOOST_CHECK_EQUAL( mempool.get_pending_nonce( payer ), util::converter::as< std::string >( nonce_value ) );

  nonce_value.set_uint64_value( 3'072 );
  trx.mutable_header()->set_nonce( util::converter::as< std::string >( nonce_value ) );
  trx.set_id( sign( _key1, trx ) );

  mempool.add_pending_transaction( make_pending_transaction( trx, 1, 1, 1 ),
                                   std::chrono::system_clock::now(),
                                   max_payer_resources );

  BOOST_CHECK_EQUAL( mempool.get_pending_nonce( payer ), util::converter::as< std::string >( nonce_value ) );
}

BOOST_AUTO_TEST_SUITE_END()
