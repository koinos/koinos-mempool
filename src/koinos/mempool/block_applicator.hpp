#pragma once

#include <map>
#include <mutex>
#include <vector>

#include <koinos/protocol/protocol.pb.h>

namespace koinos::mempool {

class block_applicator
{
private:
  std::map< uint64_t, std::vector< protocol::block > > _block_map;
  std::mutex _map_mutex;

public:
  void handle_block( const protocol::block& block, std::function< bool( const protocol::block& ) > handle_block_func );
  void handle_irreversible( uint64_t block_height );
};

} // namespace koinos::mempool
