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

  bool handle_block( const broadcast::block_accepted& bam )
  {
    if( !_applied_blocks.contains(bam.block().header().previous()) )
      return false;

    _applied_blocks.insert( bam.block().id() );
    return true;
  }

  std::set< std::string > _applied_blocks;
};

BOOST_FIXTURE_TEST_SUITE( block_applicator_tests, block_applicator_fixture )

BOOST_AUTO_TEST_CASE( block_applicator_test )
{
  // This test applies blocks out of order and advances irreversibility to ensure blocks are applied correctly.

  broadcast::block_accepted b1a;
  broadcast::block_accepted b1b;
  broadcast::block_accepted b2a;
  broadcast::block_accepted b2b;

  b1a.mutable_block()->set_id("b1a");
  b1a.mutable_block()->mutable_header()->set_previous("");
  b1a.mutable_block()->mutable_header()->set_height(1);

  b1b.mutable_block()->set_id("b1b");
  b1b.mutable_block()->mutable_header()->set_previous("");
  b1b.mutable_block()->mutable_header()->set_height(1);

  b2a.mutable_block()->set_id("b2a");
  b2a.mutable_block()->mutable_header()->set_previous("b1a");
  b2a.mutable_block()->mutable_header()->set_height(2);

  b2b.mutable_block()->set_id("b2b");
  b2b.mutable_block()->mutable_header()->set_previous("b1b");
  b2b.mutable_block()->mutable_header()->set_height(2);


  koinos::mempool::block_applicator block_applicator;

  auto handle_func = [&]( const koinos::broadcast::block_accepted& bam ){ return handle_block( bam ); };

  block_applicator.handle_block(b2a, handle_func);
  BOOST_REQUIRE(!_applied_blocks.contains(b2a.block().id()));

  block_applicator.handle_block(b2b, handle_func);
  BOOST_REQUIRE(!_applied_blocks.contains(b2b.block().id()));

  block_applicator.handle_block(b1a, handle_func);
  BOOST_REQUIRE(_applied_blocks.contains(b1a.block().id()));
  BOOST_REQUIRE(_applied_blocks.contains(b2a.block().id()));

  block_applicator.handle_irreversible(2);

  // Normally, this would not happen, but applying b1b should not trigger b2b because it was removed
  block_applicator.handle_block(b1b, handle_func);
  BOOST_REQUIRE(_applied_blocks.contains(b1b.block().id()));
  BOOST_REQUIRE(!_applied_blocks.contains(b2b.block().id()));
}

BOOST_AUTO_TEST_SUITE_END()