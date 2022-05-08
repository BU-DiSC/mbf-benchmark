#include "emu_environment.h"




/*Set up the singleton object with the experiment wide options*/
EmuEnv* EmuEnv::instance = 0;


EmuEnv::EmuEnv() 
{
// Options set through command line 
  size_ratio = 2;
  buffer_size_in_pages = 128;
  entries_per_page = 128;
  entry_size = 128;                                                    // in Bytes 
  buffer_size = buffer_size_in_pages * entries_per_page * entry_size;  // M = P*B*E = 128 * 128 * 128 B = 2 MB 
  file_to_memtable_size_ratio = 1; // f
  file_size = buffer_size * file_to_memtable_size_ratio;
  verbosity = 0;

  // adding new parameters with Guanting
  compaction_pri = 1;                 // c | 1:kMinOverlappingRatio, 2:kByCompensatedSize, 3:kOldestLargestSeqFirst, 4:kOldestSmallestSeqFirst
  bits_per_key = 0;                   // b

  experiment_runs = 1;                // run
  clear_sys_page_cache = false;       // cc
  destroy = true;                     // dd
  use_direct_reads = false;           // dr


// Options hardcoded in code
  // Memory allocation options
    max_write_buffer_number = 2;
    memtable_factory = 2;                         // 1:skiplist, 2:vector, 3:hash skiplist, 4:hash linklist
    target_file_size_base = buffer_size;
    level_compaction_dynamic_level_bytes = false;
    compaction_style = 1;                         // 1:kCompactionStyleLevel, 2:kCompactionStyleUniversal, 3:kCompactionStyleFIFO, 4:kCompactionStyleNone 
    disable_auto_compactions = false;             // TBC
    compaction_filter = 0;                        // 0:nullptr, 1:invoking custom compaction filter, if any
    compaction_filter_factory = 0;                // 0:nullptr, 1:invoking custom compaction filter factory, if any
    access_hint_on_compaction_start = 2;          // TBC
    level0_file_num_compaction_trigger = 2;       // set to 2
    level0_slowdown_writes_trigger = 2;           // set to 2
    level0_stop_writes_trigger = 2;               // set to 2 to ensure at most 2 files in level0
    target_file_size_multiplier = 1;              
    max_background_jobs = 1;                 
    max_compaction_bytes = 0;                     // TBC
    max_bytes_for_level_base = buffer_size * size_ratio;
    merge_operator = 0; 
    soft_pending_compaction_bytes_limit = 64 * 1073741824ull;     // In default, no pending compaction anytime, try and see
    hard_pending_compaction_bytes_limit = 256 * 1073741824ull;    // In default, no compaction anytime, try and see
    periodic_compaction_seconds = 0;
    use_direct_io_for_flush_and_compaction = false;
    num_levels = 4;                                             // Maximum number of levels that a tree may have [RDB_default: 7]

  // TableOptions
    no_block_cache = false;                                        // TBC
    block_cache_capacity = 8*1024*1024;				// default 8 MB
    block_cache_high_priority_ratio = 1.0;
    cache_index_and_filter_blocks = true;
    cache_index_and_filter_blocks_with_high_priority = true;      // Deprecated by no_block_cache
    read_amp_bytes_per_bit = 4;                                   // Temporarily 4; why 4 ?
    modular_filters = true;
    adaptive_pf_mf = false;
    concurrent_load = true;
    prefetch_bpk = 0;
    bpk_bounded = false;
    require_all_modules = false;
    allow_whole_filter_skipping = false;

    data_block_index_type = 1;                                    // 1:kDataBlockBinarySearch, 2:kDataBlockBinaryAndHash
    index_type = 1;                                               // 1:kBinarySearch, 2:kHashSearch, 3:kTwoLevelIndexSearch
    partition_filters = false;
    metadata_block_size = 4096;                                   // TBC, currently deprecated by data_block_index_type
    pin_top_level_index_and_filter = false;                       // TBC
    index_shortening = 1;                                         // 1:kNoShortening, 2:kShortenSeparators, 3:kShortenSeparatorsAndSuccessor
    block_size_deviation = 0;                                     // TBC
    enable_index_compression = false;                             // TBC

  //Compression
    compression = 1;                                              // 1:kNoCompression, 2:kSnappyCompression, 3:kZlibCompression, 4:kBZip2Compression,
                                                                  // 5:kLZ4Compression, 6:kLZ4HCCompression, 7:kXpressCompression, 8:kZSTD, 9:kZSTDNotFinalCompression, 10:kDisableCompressionOption

  // ReadOptions
    verify_checksums = true;                                      // TBC
    fill_cache = false;                                           // data block/index block read:this iteration will not be cached
    iter_start_seqnum = 0;                                        // TBC
    ignore_range_deletions = true;                                // TBC
    read_tier = 1;                                                // 1:kReadAllTier, 2:kBlockCacheTier, 3:kPersistedTier, 4:kMemtableTier

  // WriteOptions
    low_pri = true;                                               // every insert is less important than compaction
    sync = false;                                                 // make every write wait:sync with log (so we see real perf impact of insert)
    disableWAL = false;                                           // TBC
    no_slowdown = false;                                          // enabling this will make some insertions fail 
    ignore_missing_column_families = false;                       // TBC

  // Other CFOptions
    comparator = 1;                                               // 1:BytewiseComparator(), 2:...
    max_sequential_skip_in_iterations = 8;                        // TBC
    memtable_prefix_bloom_size_ratio = 0;                         // disabled
    paranoid_file_checks = false;
    optimize_filters_for_hits = false;
    inplace_update_support = false;
    inplace_update_num_locks = 10000;
    report_bg_io_stats = true;
    max_successive_merges = 0;                                    // read-modified-write related

  // Other DBOptions
    create_if_missing = true;
    delayed_write_rate = 0;
    // By default index, filter, and compression dictionary blocks (with the exception of the partitions of partitioned indexes/filters) are cached outside of block cache, 
    // and users won't be able to control how much memory should be used to cache these blocks, other than setting max_open_files.
    max_open_files = -1;   
    max_file_opening_threads = 16;
    bytes_per_sync = 0;
    stats_persist_period_sec = 600;
    enable_thread_tracking = false;
    stats_history_buffer_size = 1024 * 1024;
    allow_concurrent_memtable_write = false;
    dump_malloc_stats = false;
//    use_direct_reads = false;                                      // turn off when testing bloom filter
    avoid_flush_during_shutdown = false;
    advise_random_on_open = true;
    delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;   // 6 hours
    allow_mmap_reads = false;
    allow_mmap_writes = false;

    // Flush Options
    wait = true;
    allow_write_stall = true;



  // Workload options -- not sure if necessary to have these here!
    int num_inserts = 0;

// old options


  path = ""; 
  wpath = ""; 
  debugging = false; 
  FPR_optimization_level = 1; 
  derived_num_levels=-1; 
  num_queries = 0; 
  N=-1; 
  derived_N=-1; 
  K=1; 
  Z=1; 
  nonzero_to_zero_ratio=0; 
  use_block_based_filter=false;
  string experiment_name = ""; 
  string experiment_starting_time = "";
  max_levels=1000;
  show_progress=false;
  measure_IOs=false; 
  total_IOs=0;
  target_level_for_non_zero_result_point_lookups = num_levels;
  key_prefix_for_entries_to_target_in_queries = "+";
  clean_caches_for_experiments=false;
  print_IOs_per_file=false;
  compaction_readahead_size_KB=2048;

  
  file_system_page_size=4096;
  num_pq_executed=0;
  num_rq_executed=0;
  only_tune=false;
  num_read_query_sessions=1;
}

EmuEnv* EmuEnv::getInstance()
{
  if (instance == 0)
    instance = new EmuEnv();

  return instance;
}




/*

std::string get_time_string() {
   time_t rawtime;
   struct tm * timeinfo;
   char buffer[80];
   time (&rawtime);
   timeinfo = localtime(&rawtime);
   strftime(buffer,80,"%Y-%m-%d_%H:%M:%S",timeinfo);
   std::string str(buffer);
   return str;
}


bool is_zero(double val)
{
    double epsilon = 0.00001;
    return ( abs(val)<epsilon );
}




// Collect some existing keys to be used within the experiment
void collect_existing_entries(ExpEnv* _env, vector<string>& existing_keys ) {
  Options options;
  options.create_if_missing = false;
  options.use_direct_reads = false;
  options.num_levels = _env->rocksDB_max_levels;
  options.IncreaseParallelism(6);
  rocksdb::BlockBasedTableOptions table_options;
  options.num_levels = _env->rocksDB_max_levels;
  
  
  //file_size is defaulting to : std::numeric_limits<uint64_t>::max()
  FluidLSMTree* tree = new FluidLSMTree(_env->T, _env->K, _env->Z, _env->file_size, options);
  options.listeners.emplace_back(tree);


  DB* db = nullptr;
  Status s = DB::OpenForReadOnly(options, _env->path, &db);

  if (!s.ok()) {
    std::cerr << "Problem opening DB. Closing." << std::endl;
    delete db;
    exit(0);
  }

  tree->buildStructure(db);

  int num_existing_keys_to_get = _env->num_queries * (_env->nonzero_to_zero_ratio / (_env->nonzero_to_zero_ratio + 1));

  if (num_existing_keys_to_get == 0) {
    return;
  }

  struct timeval t1, t2;
  gettimeofday(&t1, NULL);

  string key_prefix = "";
  string val_prefix = "";
  //if (_env->target_level_for_non_zero_result_point_lookups != -1) {
  key_prefix = _env->key_prefix_for_entries_to_target_in_queries;
  val_prefix = to_string(_env->target_level_for_non_zero_result_point_lookups) + "-";
  //}
  //else {
  //  val_prefix = to_string(tree->largestOccupiedLevel()) + "-";
  //}


  // verify the values are still there
  existing_keys.reserve(num_existing_keys_to_get);
  std::string existing_key;
  std::string existing_val;
  auto iter = db->NewIterator(ReadOptions());
  while (existing_keys.size() < num_existing_keys_to_get) {
    string key = DataGenerator::generate_key(key_prefix);
    iter->Seek(key);
    if (iter->Valid()) {
      existing_key = iter->key().ToString();
      existing_val = iter->value().ToString();
      if (existing_val.compare(0, val_prefix.size(), val_prefix) == 0) {
        existing_keys.push_back(existing_key);
      }
    }
    //assert(value == std::string(500, 'a' + (i % 26)));
  }
  //double query_time = difftime(end_q, start_q);
  delete iter;
  gettimeofday(&t2, NULL);
  double experiment_time = (t2.tv_sec - t1.tv_sec) * 1000.0 + (t2.tv_usec - t1.tv_usec)/1000.0;
  experiment_time /= 1000.0;  // get it to be in seconds

  if (_env->debugging) {
    std::cerr << "collected " << num_existing_keys_to_get << " existing keys in " << experiment_time << "seconds" << endl;
    std::cerr << endl;
  }

  db->Close();
  delete db;
}
*/
