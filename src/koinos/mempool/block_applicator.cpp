#include <koinos/mempool/block_applicator.hpp>

namespace koinos::mempool {

void block_applicator::handle_block( const protocol::block& block,
                                     std::function< bool( const protocol::block& ) > handle_block_func )
{
  // If block is successfully applied, try and apply potential child blocks
  if( handle_block_func( block ) )
  {
    std::lock_guard< std::mutex > lock( _map_mutex );
    if( auto blocks_itr = _block_map.find( block.header().height() + 1 ); blocks_itr != _block_map.end() )
    {
      std::erase_if( blocks_itr->second,
                     [ & ]( protocol::block& block )
                     {
                       if( block.header().previous() != block.id() )
                         return false;

                       return handle_block_func( block );
                     } );
    }
  }
  else
  {
    std::lock_guard< std::mutex > lock( _map_mutex );
    // Otherwise add current block to map for later application
    if( auto blocks_itr = _block_map.find( block.header().height() ); blocks_itr != _block_map.end() )
    {
      blocks_itr->second.push_back( block );
    }
    else
    {
      _block_map[ block.header().height() ] = std::vector< protocol::block >{ block };
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
