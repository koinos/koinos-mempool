#include <koinos/mempool/block_applicator.hpp>

#include <deque>
#include <string>

namespace koinos::mempool {

void block_applicator::handle_block( const broadcast::block_accepted& bam,
                                     std::function< bool( const broadcast::block_accepted& ) > handle_block_func )
{
  std::lock_guard< std::mutex > lock( _map_mutex );

  // If block is successfully applied, try and apply potential child blocks
  if( handle_block_func( bam ) )
  {
    // Check for waiting children and handle them.
    // Every block applied will be added to the deque and drained so that we can apply entire
    // waiting chains of blocks
    std::deque< std::pair< std::string, uint64_t > > applied_blocks;
    applied_blocks.emplace_back( std::make_pair( bam.block().id(), bam.block().header().height() ) );

    while( applied_blocks.size() )
    {
      std::string id;
      uint64_t height;
      std::tie( id, height ) = applied_blocks.front();

      if( auto blocks_itr = _block_map.find( height + 1 ); blocks_itr != _block_map.end() )
      {
        std::erase_if( blocks_itr->second,
                       [ & ]( const broadcast::block_accepted& bam )
                       {
                         if( bam.block().header().previous() != id )
                           return false;

                         if( !handle_block_func( bam ) )
                           return false;

                         applied_blocks.emplace_back(
                           std::make_pair( bam.block().id(), bam.block().header().height() ) );

                         return true;
                       } );
      }

      applied_blocks.pop_front();
    }
  }
  else
  {
    // Otherwise add current block to map for later application
    if( auto blocks_itr = _block_map.find( bam.block().header().height() ); blocks_itr != _block_map.end() )
    {
      blocks_itr->second.push_back( bam );
    }
    else
    {
      _block_map[ bam.block().header().height() ] = std::vector< broadcast::block_accepted >{ bam };
    }
  }
}

void block_applicator::handle_irreversible( uint64_t block_height )
{
  // Erase all entries earlier than the LIB height
  std::lock_guard< std::mutex > lock( _map_mutex );
  std::erase_if( _block_map,
                 [ & ]( const auto& item )
                 {
                   return item.first <= block_height;
                 } );
}

} // namespace koinos::mempool
