#pragma once

#include <map>
#include <mutex>
#include <vector>

#include <koinos/broadcast/broadcast.pb.h>

namespace koinos::mempool {

class block_applicator
{
private:
  std::map< uint64_t, std::vector< broadcast::block_accepted > > _block_map;
  std::mutex _map_mutex;

public:
  void handle_block( const broadcast::block_accepted& bam, std::function< bool( const broadcast::block_accepted& ) > handle_block_func );
  void handle_irreversible( uint64_t block_height );
};

} // namespace koinos::mempool
