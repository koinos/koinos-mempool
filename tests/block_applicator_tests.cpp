#include <boost/test/unit_test.hpp>

#include <koinos/mempool/block_applicator.hpp>

#include <set>

using namespace koinos;

struct block_applicator_fixture
{
  block_applicator_fixture()
  {
    _applied_blocks.insert( std::string() );
  }

  bool handle_block( const protocol::block& block )
  {
    if( !_applied_blocks.contains(block.header().previous()) )
      return false;

    _applied_blocks.insert( block.id() );
    return true;
  }

  std::set< std::string > _applied_blocks;
};

BOOST_FIXTURE_TEST_SUITE( block_applicator_tests, block_applicator_fixture )

BOOST_AUTO_TEST_CASE( block_applicator_test )
{
  // This test applies blocks out of order and advances irreversibility to ensure blocks are applied correctly.

  protocol::block b1a;
  protocol::block b1b;
  protocol::block b2a;
  protocol::block b2b;

  b1a.set_id("b1a");
  b1a.mutable_header()->set_previous("");
  b1a.mutable_header()->set_height(1);

  b1b.set_id("b1b");
  b1b.mutable_header()->set_previous("");
  b1b.mutable_header()->set_height(1);

  b2a.set_id("b2a");
  b2a.mutable_header()->set_previous("b1a");
  b2a.mutable_header()->set_height(2);

  b2b.set_id("b2b");
  b2b.mutable_header()->set_previous("b1b");
  b2b.mutable_header()->set_height(2);


  koinos::mempool::block_applicator block_applicator;

  auto handle_func = [&]( const koinos::protocol::block& block ){ return handle_block( block ); };

  block_applicator.handle_block(b2a, handle_func);
  BOOST_REQUIRE(!_applied_blocks.contains(b2a.id()));

  block_applicator.handle_block(b2b, handle_func);
  BOOST_REQUIRE(!_applied_blocks.contains(b2b.id()));

  block_applicator.handle_block(b1a, handle_func);
  BOOST_REQUIRE(_applied_blocks.contains(b1a.id()));
  BOOST_REQUIRE(_applied_blocks.contains(b2a.id()));

  block_applicator.handle_irreversible(2);

  // Normally, this would not happen, but applying b1b should not trigger b2b because it was removed
  block_applicator.handle_block(b1b, handle_func);
  BOOST_REQUIRE(_applied_blocks.contains(b1b.id()));
  BOOST_REQUIRE(!_applied_blocks.contains(b2b.id()));
}

BOOST_AUTO_TEST_SUITE_END()