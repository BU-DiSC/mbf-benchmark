// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <iomanip>
#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "../util/string_util.h"
#include "args.hxx"
#include "aux_time.h"
#include "emu_environment.h"
#include "workload_stats.h"
#include "emu_util.h"
#include "rocksdb/sst_file_reader.h"

using namespace rocksdb;

// Specify your path of workload file here
std::string workloadPath = "./workload.txt";
std::string kDBPath = "./db_working_home";
QueryTracker query_stats;

int num_lookup_threads = 1;
double ucpu_pct = 0.0;
double scpu_pct = 0.0;
uint64_t warmup_queries = 0;
bool warmup = true;
std::vector<std::thread*> lookup_threads_pool;
std::atomic<int> atomic_lookup_counter (0); 
std::mutex mtx;
DB* db = nullptr;

int parse_arguments2(int argc, char *argv[], EmuEnv* _env);
void printEmulationOutput(const EmuEnv* _env, const QueryTracker *track, uint16_t n = 1);
void configOptions(EmuEnv* _env, Options *op, BlockBasedTableOptions *table_op, WriteOptions *write_op, ReadOptions *read_op, FlushOptions *flush_op);
void db_point_lookup(const ReadOptions *read_op, const std::string key, const int verbosity, QueryTracker *query_track, bool _warmup);

int runWorkload(const EmuEnv* _env, const Options *op,
                const BlockBasedTableOptions *table_op, const WriteOptions *write_op, 
                const ReadOptions *read_op, const FlushOptions *flush_op,
                const WorkloadDescriptor *wd, QueryTracker *query_track);   // run_workload internal
int runExperiments(EmuEnv* _env);    // API

int main(int argc, char *argv[]) {
  // check emu_environment.h for the contents of EmuEnv and also the definitions of the singleton experimental environment 
  EmuEnv* _env = EmuEnv::getInstance();
  // parse the command line arguments
  if (parse_arguments2(argc, argv, _env)) {
    exit(1);
  }

  my_clock start_time, end_time;
  std::cout << "Starting experiments ..."<<std::endl;
  if (my_clock_get_time(&start_time) == -1) {
    std::cerr << "Failed to get experiment start time" << std::endl;
  }
  int s = runExperiments(_env); 
  if (my_clock_get_time(&end_time) == -1) {
    std::cerr << "Failed to get experiment end time" << std::endl;
  }
  query_stats.experiment_exec_time = getclock_diff_ns(start_time, end_time);

  std::cout << std::endl << std::fixed << std::setprecision(2) 
            << "===== End of all experiments in "
            << static_cast<double>(query_stats.experiment_exec_time)/1000000 << "ms !! ===== "<< std::endl;
  
  // show average results for the number of experiment runs
  printEmulationOutput(_env, &query_stats, _env->experiment_runs);
  
  std::cout << "===== Average stats of " << _env->experiment_runs << " runs ====="  << std::endl;


  return 0;
}

void configOptions(EmuEnv* _env, Options *op, BlockBasedTableOptions *table_op, WriteOptions *write_op, ReadOptions *read_op, FlushOptions *flush_op) {
    // Experiment settings
    _env->experiment_runs = (_env->experiment_runs >= 1) ? _env->experiment_runs : 1;
    // *op = Options();
    op->write_buffer_size = _env->buffer_size;
    op->max_write_buffer_number = _env->max_write_buffer_number;   // min 2
    
    switch (_env->memtable_factory) {
      case 1:
        op->memtable_factory = std::shared_ptr<SkipListFactory>(new SkipListFactory); break;
      case 2:
        op->memtable_factory = std::shared_ptr<VectorRepFactory>(new VectorRepFactory); break;
      case 3:
        op->memtable_factory.reset(NewHashSkipListRepFactory()); break;
      case 4:
        op->memtable_factory.reset(NewHashLinkListRepFactory()); break;
      default:
        std::cerr << "error: memtable_factory" << std::endl;
    }

    // Compaction
    switch (_env->compaction_pri) {
      case 1:
        op->compaction_pri = kMinOverlappingRatio; break;
      case 2:
        op->compaction_pri = kByCompensatedSize; break;
      case 3:
        op->compaction_pri = kOldestLargestSeqFirst; break;
      case 4:
        op->compaction_pri = kOldestSmallestSeqFirst; break;
      default:
        std::cerr << "error: compaction_pri" << std::endl;
    }

    op->max_bytes_for_level_multiplier = _env->size_ratio;
    //op->compaction_options_universal.size_ratio = _env->size_ratio;
    op->target_file_size_base = _env->file_size;
    op->level_compaction_dynamic_level_bytes = _env->level_compaction_dynamic_level_bytes;
    switch (_env->compaction_style) {
      case 1:
        op->compaction_style = kCompactionStyleLevel; break;
      case 2:
        op->compaction_style = kCompactionStyleUniversal; break;
      case 3:
        op->compaction_style = kCompactionStyleFIFO; break;
      case 4:
        op->compaction_style = kCompactionStyleNone; break;
      default:
        std::cerr << "error: compaction_style" << std::endl;
    }
    
    op->disable_auto_compactions = _env->disable_auto_compactions;
    if (_env->compaction_filter == 0) {
      ;// do nothing
    } else {
      ;// invoke manual compaction_filter
    }
    if (_env->compaction_filter_factory == 0) {
      ;// do nothing
    } else {
      ;// invoke manual compaction_filter_factory
    }
    switch (_env->access_hint_on_compaction_start) {
      case 1:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::NONE; break;
      case 2:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::NORMAL; break;
      case 3:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::SEQUENTIAL; break;
      case 4:
        op->access_hint_on_compaction_start = DBOptions::AccessHint::WILLNEED; break;
      default:
        std::cerr << "error: access_hint_on_compaction_start" << std::endl;
    }
    
    op->level0_file_num_compaction_trigger = _env->level0_file_num_compaction_trigger;
    if(op->compaction_style == kCompactionStyleUniversal){
      op->level0_file_num_compaction_trigger = 1;
    }
    op->level0_slowdown_writes_trigger = _env->level0_slowdown_writes_trigger;
    op->level0_stop_writes_trigger = _env->level0_stop_writes_trigger;
    op->target_file_size_multiplier = _env->target_file_size_multiplier;
    op->max_background_jobs = _env->max_background_jobs;
    op->max_compaction_bytes = _env->max_compaction_bytes;
    op->max_bytes_for_level_base = _env->buffer_size * _env->size_ratio;;

    if (_env->merge_operator == 0) {
      ;// do nothing
    } 
    else {
      ;// use custom merge operator
    }
    op->soft_pending_compaction_bytes_limit = _env->soft_pending_compaction_bytes_limit;    // No pending compaction anytime, try and see
    op->hard_pending_compaction_bytes_limit = _env->hard_pending_compaction_bytes_limit;    // No pending compaction anytime, try and see
    op->periodic_compaction_seconds = _env->periodic_compaction_seconds;
    op->use_direct_io_for_flush_and_compaction = _env->use_direct_io_for_flush_and_compaction;
    op->num_levels = _env->num_levels;


    //Compression
    switch (_env->compression) {
      case 1:
        op->compression = kNoCompression; break;
      case 2:
        op->compression = kSnappyCompression; break;
      case 3:
        op->compression = kZlibCompression; break;
      case 4:
        op->compression = kBZip2Compression; break;
      case 5:
      op->compression = kLZ4Compression; break;
      case 6:
      op->compression = kLZ4HCCompression; break;
      case 7:
      op->compression = kXpressCompression; break;
      case 8:
      op->compression = kZSTD; break;
      case 9:
      op->compression = kZSTDNotFinalCompression; break;
      case 10:
      op->compression = kDisableCompressionOption; break;

      default:
        std::cerr << "error: compression" << std::endl;
    }

  // table_options.enable_index_compression = kNoCompression;

  // Other CFOptions
  switch (_env->comparator) {
      case 1:
        op->comparator = BytewiseComparator(); break;
      case 2:
        op->comparator = ReverseBytewiseComparator(); break;
      case 3:
        // use custom comparator
        break;
      default:
        std::cerr << "error: comparator" << std::endl;
    }

  op->max_sequential_skip_in_iterations = _env-> max_sequential_skip_in_iterations;
  op->memtable_prefix_bloom_size_ratio = _env-> memtable_prefix_bloom_size_ratio;    // disabled
  op->paranoid_file_checks = _env->paranoid_file_checks;
  op->optimize_filters_for_hits = _env->optimize_filters_for_hits;
  op->inplace_update_support = _env->inplace_update_support;
  op->inplace_update_num_locks = _env->inplace_update_num_locks;
  op->report_bg_io_stats = _env->report_bg_io_stats;
  op->max_successive_merges = _env->max_successive_merges;   // read-modified-write related

  //Other DBOptions
  op->create_if_missing = _env->create_if_missing;
  op->delayed_write_rate = _env->delayed_write_rate;
  op->max_open_files = _env->max_open_files;
  op->max_file_opening_threads = _env->max_file_opening_threads;
  op->bytes_per_sync = _env->bytes_per_sync;
  op->stats_persist_period_sec = _env->stats_persist_period_sec;
  op->enable_thread_tracking = _env->enable_thread_tracking;
  op->stats_history_buffer_size = _env->stats_history_buffer_size;
  op->allow_concurrent_memtable_write = _env->allow_concurrent_memtable_write;
  op->dump_malloc_stats = _env->dump_malloc_stats;
  op->use_direct_reads = _env->use_direct_reads;
  op->avoid_flush_during_shutdown = _env->avoid_flush_during_shutdown;
  op->advise_random_on_open = _env->advise_random_on_open;
  op->delete_obsolete_files_period_micros = _env->delete_obsolete_files_period_micros;   // 6 hours
  op->allow_mmap_reads = _env->allow_mmap_reads;
  op->allow_mmap_writes = _env->allow_mmap_writes;

  //TableOptions
  table_op->no_block_cache = _env->no_block_cache; // TBC
  if(table_op->no_block_cache){
     _env->block_cache_capacity = 0;
  }else{
     _env->cache_index_and_filter_blocks = true;
  }
  if (_env->block_cache_capacity == 0) {
      ;// do nothing
  } else {
      std::shared_ptr<Cache> cache = NewLRUCache(_env->block_cache_capacity, -1, false, _env->block_cache_high_priority_ratio);
      table_op->block_cache = cache;
      ;// invoke manual block_cache
  }

  if (_env->bits_per_key == 0) {
      ;// do nothing
  } else {
    table_op->filter_policy.reset(NewBloomFilterPolicy(_env->bits_per_key, false));    // currently build full filter instead of blcok-based filter
  }

  table_op->cache_index_and_filter_blocks = _env->cache_index_and_filter_blocks;
  table_op->cache_index_and_filter_blocks_with_high_priority = _env->cache_index_and_filter_blocks_with_high_priority;    // Deprecated by no_block_cache
  table_op->read_amp_bytes_per_bit = _env->read_amp_bytes_per_bit;
  table_op->partition_filters = _env->partition_filters;
  table_op->modular_filters = _env->modular_filters;
  table_op->concurrent_load = _env->concurrent_load;
  table_op->adaptive_prefetch_modular_filters = _env->adaptive_pf_mf;
  table_op->require_all_modules = _env->require_all_modules;
  table_op->allow_whole_filter_skipping = _env->allow_whole_filter_skipping;
  if(table_op->require_all_modules && table_op->allow_whole_filter_skipping){
       std::cerr << "warning: allow_whole_filter_skipping is deprecated when all_mods is set" << std::endl;
  }
  table_op->prefetch_bpk = _env->prefetch_bpk;
  table_op->bpk_bounded = _env->bpk_bounded;
  
  switch (_env->data_block_index_type) {
      case 1:
        table_op->data_block_index_type = BlockBasedTableOptions::kDataBlockBinarySearch; break;
      case 2:
        table_op->data_block_index_type = BlockBasedTableOptions::kDataBlockBinaryAndHash; break;
      default:
        std::cerr << "error: TableOptions::data_block_index_type" << std::endl;
  }
  switch (_env->index_type) {
      case 1:
        table_op->index_type = BlockBasedTableOptions::kBinarySearch; break;
      case 2:
        table_op->index_type = BlockBasedTableOptions::kHashSearch; break;
      case 3:
        table_op->index_type = BlockBasedTableOptions::kTwoLevelIndexSearch; break;
      default:
        std::cerr << "error: TableOptions::index_type" << std::endl;
  }
  table_op->partition_filters = _env->partition_filters;
  table_op->block_size = _env->entries_per_page * _env->entry_size;
  table_op->metadata_block_size = _env->metadata_block_size;
  table_op->pin_top_level_index_and_filter = _env->pin_top_level_index_and_filter;
  
  switch (_env->index_shortening) {
      case 1:
        table_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::kNoShortening; break;
      case 2:
        table_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators; break;
      case 3:
        table_op->index_shortening = BlockBasedTableOptions::IndexShorteningMode::kShortenSeparatorsAndSuccessor; break;
      default:
        std::cerr << "error: TableOptions::index_shortening" << std::endl;
  }
  table_op->block_size_deviation = _env->block_size_deviation;
  table_op->enable_index_compression = _env->enable_index_compression;
  // Set all table options
  op->table_factory.reset(NewBlockBasedTableFactory(*table_op));

  //WriteOptions
  write_op->sync = _env->sync; // make every write wait for sync with log (so we see real perf impact of insert)
  write_op->low_pri = _env->low_pri; // every insert is less important than compaction
  write_op->disableWAL = _env->disableWAL; 
  write_op->no_slowdown = _env->no_slowdown; // enabling this will make some insertions fail
  write_op->ignore_missing_column_families = _env->ignore_missing_column_families;
  
  //ReadOptions
  read_op->verify_checksums = _env->verify_checksums;
  read_op->fill_cache = _env->fill_cache;
  read_op->iter_start_seqnum = _env->iter_start_seqnum;
  read_op->ignore_range_deletions = _env->ignore_range_deletions;
  switch (_env->read_tier) {
    case 1:
      read_op->read_tier = kReadAllTier; break;
    case 2:
      read_op->read_tier = kBlockCacheTier; break;
    case 3:
      read_op->read_tier = kPersistedTier; break;
    case 4:
      read_op->read_tier = kMemtableTier; break;
    default:
      std::cerr << "error: ReadOptions::read_tier" << std::endl;
  }

  //FlushOptions
  flush_op->wait = _env->wait;
  flush_op->allow_write_stall = _env->allow_write_stall;


}

// Run rocksdb experiments for experiment_runs
// 1.Initiate experiments environment and rocksDB options
// 2.Preload workload into memory
// 3.Run workload and collect stas for each run
int runExperiments(EmuEnv* _env) {
  Options options;
  WriteOptions write_options;
  ReadOptions read_options;
  BlockBasedTableOptions table_options;
  FlushOptions flush_options;
  WorkloadDescriptor wd(_env->wpath);
  //WorkloadDescriptor wd(workloadPath);
  // init RocksDB configurations and experiment settings
  configOptions(_env, &options, &table_options, &write_options, &read_options, &flush_options);
  // parsing workload
  loadWorkload(&wd);
  
  // Starting experiments
  assert(_env->experiment_runs >= 1);
  for (int i = 0; i < _env->experiment_runs; ++i) {
    // Reopen DB
    if (_env->destroy) {
      //DestroyDB(kDBPath, options);
      DestroyDB(_env->path, options);
    }
    //Status s = DB::Open(options, kDBPath, &db);
    Status s = DB::Open(options, _env->path, &db);
    if (!s.ok()) std::cerr << s.ToString() << std::endl;
    assert(s.ok());
    
    // Prepare Perf and I/O stats
    QueryTracker *query_track = new QueryTracker();   // stats tracker for each run
    get_iostats_context()->Reset();
    // Run workload
    runWorkload(_env, &options, &table_options, &write_options, &read_options, &flush_options, &wd, query_track);

    // Collect stats after per run
    SetPerfLevel(kDisable);
    query_track->workload_exec_time = query_track->inserts_cost + query_track->updates_cost + query_track->point_deletes_cost 
                                    + query_track->range_deletes_cost + query_track->point_lookups_cost + query_track->zero_point_lookups_cost
                                    + query_track->range_lookups_cost;
    query_track->get_from_memtable_count += get_perf_context()->get_from_memtable_count;
    query_track->get_from_memtable_time += get_perf_context()->get_from_memtable_time;
    query_track->get_from_output_files_time += get_perf_context()->get_from_output_files_time;
    query_track->filter_block_read_count += get_perf_context()->filter_block_read_count;
    query_track->index_block_read_count += get_perf_context()->index_block_read_count;
    query_track->block_cache_filter_hit_count += get_perf_context()->block_cache_filter_hit_count;
    query_track->block_cache_index_hit_count += get_perf_context()->block_cache_index_hit_count;
    query_track->bloom_memtable_hit_count += get_perf_context()->bloom_memtable_hit_count;
    query_track->bloom_memtable_miss_count += get_perf_context()->bloom_memtable_miss_count;
    query_track->bloom_sst_hit_count += get_perf_context()->bloom_sst_hit_count;
    query_track->bloom_sst_miss_count += get_perf_context()->bloom_sst_miss_count;
    query_track->get_cpu_nanos += get_perf_context()->get_cpu_nanos;

    query_track->bytes_read += get_iostats_context()->bytes_read;
    query_track->read_nanos += get_iostats_context()->read_nanos;
    query_track->cpu_read_nanos += get_iostats_context()->cpu_read_nanos;
    query_track->bytes_written += get_iostats_context()->bytes_written;
    query_track->write_nanos += get_iostats_context()->write_nanos;
    query_track->cpu_write_nanos += get_iostats_context()->cpu_write_nanos;

    // Space amp
    uint64_t live_sst_size = 0;
    db->GetIntProperty("rocksdb.live-sst-files-size", &live_sst_size);
    uint64_t calculate_size = 1024 * (query_track->inserts_completed - query_track->point_deletes_completed);
    query_track->space_amp = static_cast<double>(live_sst_size) / calculate_size;

    std::map<std::string, std::string> cfstats;
    db->GetMapProperty("rocksdb.cfstats", &cfstats);
//    for (std::map<std::string, std::string>::iterator it=cfstats.begin(); it !=cfstats.end(); ++it)
//    std::cout << it->first << " => " << it->second << '\n';

    // Write amp
    query_track->write_amp = std::stod(cfstats.find("compaction.Sum.WriteAmp")->second);

    // TODO:: wrong read amp
    query_track->read_amp = std::stod(cfstats.find("compaction.Sum.Rnp1GB")->second)
        / std::stod(cfstats.find("compaction.Sum.RnGB")->second);

    // stalls triggered by compactions
    query_track->stalls = std::stod(cfstats.find("io_stalls.total_stop")->second);

    if (_env->verbosity > 0) {
      printEmulationOutput(_env, query_track);
    } else if (_env->verbosity > 1) {
      std::string state;
      db->GetProperty("rocksdb.cfstats-no-file-histogram", &state);
      std::cout << state << std::endl;
    }

    uint64_t tree_level=0;
    db->GetIntProperty("rocksdb.base-level", &tree_level);
    cout << "tree level " << tree_level << endl;

    std::string prop;
    db->GetProperty("rocksdb.stats", &prop);
    cout << "tree level " << prop << endl;


      string table_readers_mem;
      db->GetProperty("rocksdb.estimate-table-readers-mem", &table_readers_mem);
      std::cout << "rocksdb.estimate-table-readers-mem:" << table_readers_mem << std::endl;
      query_track->read_table_mem += atoi(table_readers_mem.c_str());
      std::cout << std::endl;
  if(table_options.block_cache){
    std::cout << "rocksdb.block_cache_usage:" << table_options.block_cache->GetUsage() << std::endl;
      query_track->block_cache_usage += table_options.block_cache->GetUsage();
  }
  dumpStats(&query_stats, query_track);    // dump stat of each run into acmulative stat

  if ( _env->print_sst_stat ){
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = db->DefaultColumnFamily();
    auto cfh = reinterpret_cast<rocksdb::ColumnFamilyHandleImpl*>(get_impl_options.column_family);
    auto cfd = cfh->cfd();
    const auto* vstorage = cfd->current()->storage_info();
    for (int i = 1; i < cfd->NumberLevels(); i++) {
      if (vstorage->LevelFiles(i).empty()){
        continue;
      }
      std::cout << i << std::endl;
      std::vector<FileMetaData*> level_files = vstorage->LevelFiles(i);
      FileMetaData* level_file;
      for (uint32_t j = 0; j < level_files.size(); j++) {
        level_file = level_files[j];
        std::cout<< level_file->stats.num_reads_sampled.load(std::memory_order_relaxed) << " " << level_file->stats.num_tps_sampled.load(std::memory_order_relaxed) << " "<< level_file->prefetch_bpk << std::endl;
      }
    }
  }


    CloseDB(db, flush_options);
    std::cout << "End of experiment run: " << i+1 << std::endl;
    std::cout << std::endl;
  }
  return 0;
}
void db_point_lookup(const ReadOptions *read_op, const std::string key, const int verbosity, QueryTracker *query_track, bool _warmup){
    mtx.lock();
    my_clock start_clock, end_clock;    // clock to get query time
    string value;
    Status s;

    //SetPerfLevel(kEnableTimeExceptForMutex);
    if(!_warmup){
    SetPerfLevel(rocksdb::PerfLevel::kEnableTime);
    get_perf_context()->Reset();
    get_iostats_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();
    }
    mtx.unlock();

    if (verbosity == 2)
      std::cout << "Q " << key << "" << std::endl;
    if (my_clock_get_time(&start_clock) == -1)
      std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
    //s = db->Get(*read_op, ToString(key), &value);
    s = db->Get(*read_op, key, &value);
    // assert(s.ok());
    if (my_clock_get_time(&end_clock) == -1) 
      std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
    mtx.lock();
    if(!_warmup){
      if (!s.ok()) {    // zero_reuslt_point_lookup
        if (verbosity == 2) {

           std::cerr << s.ToString() << "key = " << key << std::endl;
        }
        query_track->zero_point_lookups_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->zero_point_lookups_completed;
      } else{

        query_track->point_lookups_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->point_lookups_completed;
      }
      ++query_track->total_completed;
        query_track->get_from_memtable_count += get_perf_context()->get_from_memtable_count;
        query_track->get_from_memtable_time += get_perf_context()->get_from_memtable_time;
        query_track->get_from_output_files_time += get_perf_context()->get_from_output_files_time;
        query_track->filter_block_read_count += get_perf_context()->filter_block_read_count;
	query_track->index_block_read_count += get_perf_context()->index_block_read_count;
    	query_track->block_cache_filter_hit_count += get_perf_context()->block_cache_filter_hit_count;
    	query_track->block_cache_index_hit_count += get_perf_context()->block_cache_index_hit_count;
        query_track->bloom_memtable_hit_count += get_perf_context()->bloom_memtable_hit_count;
        query_track->bloom_memtable_miss_count += get_perf_context()->bloom_memtable_miss_count;
        query_track->bloom_sst_hit_count += get_perf_context()->bloom_sst_hit_count;
        query_track->bloom_sst_miss_count += get_perf_context()->bloom_sst_miss_count;
        query_track->get_cpu_nanos += get_perf_context()->get_cpu_nanos;
        query_track->bytes_read += get_iostats_context()->bytes_read; 
        query_track->read_nanos += get_iostats_context()->read_nanos;
        query_track->cpu_read_nanos += get_iostats_context()->cpu_read_nanos;
        query_track->bytes_written += get_iostats_context()->bytes_written;
        query_track->write_nanos += get_iostats_context()->write_nanos;
        query_track->cpu_write_nanos += get_iostats_context()->cpu_write_nanos; 
    }
    mtx.unlock();
}
// Run a workload from memory
// The workload is stored in WorkloadDescriptor
// Use QueryTracker to record performance for each query operation
int runWorkload(const EmuEnv* _env, const Options *op, const BlockBasedTableOptions *table_op, 
                const WriteOptions *write_op, const ReadOptions *read_op, const FlushOptions *flush_op,
                const WorkloadDescriptor *wd, QueryTracker *query_track) {
  Status s;
  Iterator *it = db-> NewIterator(*read_op); // for range reads
  uint64_t counter = 0, mini_counter = 0; // tracker for progress bar. TODO: avoid using these two 
  uint32_t cpu_sample_counter = 0;
  my_clock start_clock, end_clock;    // clock to get query time
  // Clear System page cache before running
  if (_env->clear_sys_page_cache) { 
    std::cout << "\nClearing system page cache before experiment ..."; 
    fflush(stdout);
    clearPageCache();
    get_perf_context()->Reset();
    get_iostats_context()->Reset();
    std::cout << " OK!" << std::endl;
  }

  CpuUsage cpu_stat;
  cpuUsageInit();
  for (const auto &qd : wd->queries) {
    // Reopen DB and clear cache before bulk reading
    // if (counter == wd->insert_num) {
    //   std::cout << "\nRe-opening DB and clearing system page cache ...";
    //   fflush(stdout);
    //   ReopenDB(db, *op, *flush_op);
    //   clearPageCache();
    //   get_perf_context()->Reset();
    //   get_iostats_context()->Reset();
    //   std::cout << " OK!" << std::endl;
    // }
    //uint32_t key, start_key, end_key;
    std::string key, start_key, end_key;
    std::string value;
    int thread_index;
    Entry *entry_ptr = nullptr;
    RangeEntry *rentry_ptr = nullptr;

    switch (qd.type) {
      case INSERT:
        ++counter;
        assert(counter = qd.seq);
        if (query_track->inserts_completed == wd->actual_insert_num) break;
        for(size_t k = 0; k < lookup_threads_pool.size(); k++){
            if(lookup_threads_pool[k] != nullptr){
                lookup_threads_pool[k]->join();
     	        delete lookup_threads_pool[k];
                lookup_threads_pool[k] = nullptr;
            }
        }
        entry_ptr = dynamic_cast<Entry*>(qd.entry_ptr);
        key = entry_ptr->key;
        value = entry_ptr->value;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << key << " " << value << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        //s = db->Put(*write_op, ToString(key), value);
        s = db->Put(*write_op, key, value);
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        if (!s.ok()) std::cerr << s.ToString() << std::endl;
        assert(s.ok());
        query_track->inserts_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->total_completed;
        ++query_track->inserts_completed;
        break;

      case UPDATE:
        ++counter;
        assert(counter = qd.seq);
        entry_ptr = dynamic_cast<Entry*>(qd.entry_ptr);
        key = entry_ptr->key;
        value = entry_ptr->value;
        for(size_t k = 0; k < lookup_threads_pool.size(); k++){
            if(lookup_threads_pool[k] != nullptr){
            lookup_threads_pool[k]->join();
     	    delete lookup_threads_pool[k];
            lookup_threads_pool[k] = nullptr;
		}
        }
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << key << " " << value << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        //s = db->Put(*write_op, ToString(key), value);
        s = db->Put(*write_op, key, value);
        if (!s.ok()) std::cerr << s.ToString() << std::endl;
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        assert(s.ok());
        query_track->updates_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->updates_completed;
        ++query_track->total_completed;
        break;

      case DELETE:
        ++counter;
        assert(counter = qd.seq);
        key = qd.entry_ptr->key;
        for(size_t k = 0; k < lookup_threads_pool.size(); k++){
            if(lookup_threads_pool[k] != nullptr){
                lookup_threads_pool[k]->join();
     	        delete lookup_threads_pool[k];
                lookup_threads_pool[k] = nullptr;
	    }
        } 
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << key << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        //s = db->Delete(*write_op, ToString(key));
        s = db->Delete(*write_op, key);
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        assert(s.ok());
        query_track->point_deletes_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->point_deletes_completed;
        ++query_track->total_completed;
        break;

      case RANGE_DELETE:
        ++counter;
        assert(counter = qd.seq);
        for(size_t k = 0; k < lookup_threads_pool.size(); k++){
            if(lookup_threads_pool[k] != nullptr){
                lookup_threads_pool[k]->join();
     	        delete lookup_threads_pool[k];
                lookup_threads_pool[k] = nullptr;
	    }
        }
        rentry_ptr = dynamic_cast<RangeEntry*>(qd.entry_ptr);
        start_key = rentry_ptr->key;
        end_key = rentry_ptr->end_key;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << start_key << " " << end_key << "" << std::endl;
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
        //s = db->DeleteRange(*write_op, db->DefaultColumnFamily(), ToString(start_key), ToString(end_key));
        s = db->DeleteRange(*write_op, db->DefaultColumnFamily(), start_key, end_key);
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        assert(s.ok());
        query_track->range_deletes_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->range_deletes_completed;
        ++query_track->total_completed;
        break;

      case LOOKUP:
        ++counter;
        assert(counter = qd.seq);
        // for pseudo zero-reuslt point lookup
        // if (query_track->point_lookups_completed + query_track->zero_point_lookups_completed >= 10) break;
        key = qd.entry_ptr->key;
	if(warmup){
        	thread_index = atomic_lookup_counter%num_lookup_threads;
        	if(atomic_lookup_counter.load(std::memory_order_relaxed) >= warmup_queries){
        	    warmup = false;
                    CompactionMayAllComplete(db);
		    thread_index = 0;
		    for(int i = 0; i < lookup_threads_pool.size(); i++){
                        if(lookup_threads_pool[i] != nullptr){
        	        lookup_threads_pool[i]->join();
			delete lookup_threads_pool[i];
			lookup_threads_pool[i] = nullptr;
                        }
        	    }
		    lookup_threads_pool.clear();
        	}

        	if(thread_index >= lookup_threads_pool.size()){
        	    lookup_threads_pool.push_back(new std::thread(db_point_lookup, read_op, key, _env->verbosity, query_track, warmup));
        	}else{
		    if(lookup_threads_pool[thread_index] != nullptr){
		       lookup_threads_pool[thread_index]->join();
		       delete lookup_threads_pool[thread_index];
		       lookup_threads_pool[thread_index] = nullptr;
		    }
        	    lookup_threads_pool[thread_index] = new std::thread(db_point_lookup, read_op, key, _env->verbosity, query_track, warmup);
        	}
        	atomic_lookup_counter++;
	}else{
		db_point_lookup(read_op, key, _env->verbosity, query_track, false);
	}
        break;

      case RANGE_LOOKUP:
        ++counter; 
        assert(counter = qd.seq);
        rentry_ptr = dynamic_cast<RangeEntry*>(qd.entry_ptr);
        start_key = rentry_ptr->key;
        //end_key = start_key + rentry_ptr->range;
        end_key = rentry_ptr->end_key;
        if (_env->verbosity == 2)
          std::cout << static_cast<char>(qd.type) << " " << start_key << " " << end_key << "" << std::endl;
        it->Refresh();    // to update a stale iterator view
        assert(it->status().ok());
        if (my_clock_get_time(&start_clock) == -1)
          std::cerr << s.ToString() << "start_clock failed to get time" << std::endl;
       
        for (it->Seek(start_key); it->Valid(); it->Next()) {
          // std::cout << "found key = " << it->key().ToString() << std::endl;
          if(it->key() == end_key) {    // TODO: check correntness
            break;
          }
        }
        if (my_clock_get_time(&end_clock) == -1) 
          std::cerr << s.ToString() << "end_clock failed to get time" << std::endl;
        if (!it->status().ok()) {
          std::cerr << it->status().ToString() << std::endl;
        }
        query_track->range_lookups_cost += getclock_diff_ns(start_clock, end_clock);
        ++query_track->range_lookups_completed;
        ++query_track->total_completed;
        break;

      default:
          std::cerr << "Unknown query type: " << static_cast<char>(qd.type) << std::endl;
    }
    if(counter%(wd->total_num/50) == 0){ 
    cpu_stat = getCurrentCpuUsage();
    ucpu_pct += cpu_stat.user_time_pct;
    scpu_pct += cpu_stat.sys_time_pct;
    cpu_sample_counter++;
    }
    showProgress(wd->total_num, counter, mini_counter);
  }
  
  for(std::thread* t: lookup_threads_pool){
     if(t != nullptr){
        t->join();
        delete t;
     }
  } 
  lookup_threads_pool.clear();
  if(cpu_sample_counter == 0) cpu_sample_counter = 1;
  query_track->ucpu_pct += ucpu_pct/cpu_sample_counter;
  query_track->scpu_pct += scpu_pct/cpu_sample_counter;
  std::cout << std::endl << ucpu_pct << " " << scpu_pct << std::endl;
  ucpu_pct = 0.0;
  scpu_pct = 0.0;
  
  // flush and wait for the steady state
  db->Flush(*flush_op);
  FlushMemTableMayAllComplete(db);
  CompactionMayAllComplete(db);
  return 0;
}

int parse_arguments2(int argc, char *argv[], EmuEnv* _env) {
  args::ArgumentParser parser("RocksDB_parser.", "");

  args::Group group1(parser, "This group is all exclusive:", args::Group::Validators::DontCare);
  args::Group group4(parser, "Optional switches and parameters:", args::Group::Validators::DontCare);
  args::ValueFlag<int> size_ratio_cmd(group1, "T", "The size ratio of two adjacent levels  [def: 2]", {'T', "size_ratio"});
  args::ValueFlag<int> buffer_size_in_pages_cmd(group1, "P", "The number of pages that can fit into a buffer [def: 128]", {'P', "buffer_size_in_pages"});
  args::ValueFlag<int> entries_per_page_cmd(group1, "B", "The number of entries that fit into a page [def: 128]", {'B', "entries_per_page"});
  args::ValueFlag<int> entry_size_cmd(group1, "E", "The size of a key-value pair inserted into DB [def: 128 B]", {'E', "entry_size"});
  args::ValueFlag<long> buffer_size_cmd(group1, "M", "The size of a buffer that is configured manually [def: 2 MB]", {'M', "memory_size"});
  args::ValueFlag<int> file_to_memtable_size_ratio_cmd(group1, "file_to_memtable_size_ratio", "The size of a file over the size of configured buffer size [def: 1]", {'f', "file_to_memtable_size_ratio"});
  args::ValueFlag<long> file_size_cmd(group1, "file_size", "The size of a file that is configured manually [def: 2 MB]", {'F', "file_size"});
  args::ValueFlag<int> compaction_pri_cmd(group1, "compaction_pri", "[Compaction priority: 1 for kMinOverlappingRatio, 2 for kByCompensatedSize, 3 for kOldestLargestSeqFirst, 4 for kOldestSmallestSeqFirst; def: 2]", {'C', "compaction_pri"});
  args::ValueFlag<int> compaction_style_cmd(group1, "compaction_style", "[Compaction style: 1 for kCompactionStyleLevel, 2 for kCompactionStyleUniversal, 3 for kCompactionStyleFIFO, 4 for kCompactionStyleNone; def: 1]", {'c', "compaction_style"});
  args::ValueFlag<int> bits_per_key_cmd(group1, "bits_per_key", "The number of bits per key assigned to Bloom filter [def: 0]", {'b', "bits_per_key"});

  args::Flag partition_filter_cmd(group1, "part_bf", "Enable partition bloom filters", {"part","enable_partition_filters"});
  args::Flag modular_filters_cmd(group1, "mod_bf", "Enable modular filters. Useless when partition filter is enable. ", {"mod", "enable_modular_filters"});
  args::Flag adaptive_prefetch_modular_filters_cmd(group1, "adaptive_prefetch_modular_filter", "Enable adaptive modular filters. Useless when partition filter is enable. ", {"ada_Pb_mf", "adaptive_prefetch_modular_filter"});
  args::Flag require_all_modules_cmd(group1, "require_all_mods", "Require all modules during the query", {"all_mods", "require_all_modules"});
  args::Flag allow_whole_filter_skipping_cmd(group1, "allow_whole_filter_skipping", "allow the whole filter is skipped under non all_mods setting (deprecated if all_mods is set)", {"allow_WFS_mf", "allow_whole_filter_skip_modular_filters"});
  args::Flag concurrent_loading_modules_cmd(group1, "concurrent_load_mods", "Concurrently loading two modules (useless when all_mods is not specified)", {"cc_mods", "concurrent_load_modules"});
  args::Flag bounded_prefetch_bpk_cmd(group1, "bounded_prefetch_bpk", "prefetch bpk is bounded", {"UB_Pb", "enable_upper_bounded_prefetch_bpk"});
  args::ValueFlag<double> prefetch_bpk_cmd(group1, "prefetch_bpk", "The bits per key of prefetching modules [def: 0]", {"Pb", "prefetch_bpk"});

  args::Flag block_cache_cmd(group1, "block_cache", "Enable block cache", {"blk_cache", "enable_block_cache"});
  args::ValueFlag<int> block_cache_capacity_cmd(group1, "block_cache_capacity", "The capacity (bytes) of block cache [def: 8 MB]", {"BCC", "block_cache_capacity"});
  args::ValueFlag<double> block_cache_high_priority_ratio_cmd(group1, "block_cache_high_priority_ratio", "The ratio of capacity reserved for high priority blocks in block cache [def: 1.0 ]", {"BCHPR", "block_cache_high_priority_ratio"});
  args::ValueFlag<int> experiment_runs_cmd(group1, "experiment_runs", "The number of experiments repeated each time [def: 1]", {'R', "run"});
//  args::ValueFlag<long> num_inserts_cmd(group1, "inserts", "The number of unique inserts to issue in the experiment [def: 0]", {'i', "inserts"});
  args::Flag clear_sys_page_cache_cmd(group4, "clear_sys_page_cache", "Clear system page cache before experiments", {"cc", "clear_cache"});
  args::Flag destroy_cmd(group4, "destroy_db", "Destroy and recreate the database", {"dd", "destroy_db"});
  args::Flag direct_reads_cmd(group4, "use_direct_reads", "Use direct reads", {"dr", "use_direct_reads"});
  args::ValueFlag<int> verbosity_cmd(group4, "verbosity", "The verbosity level of execution [0,1,2; def: 0]", {'V', "verbosity"});
  args::ValueFlag<int> num_lookup_threads_cmd(group4, "num_lookup_threads", "The number of threads for point lookup", {'t', "num_lookup_threads"});
  args::ValueFlag<std::string> path_cmd(group4, "path", "path for writing the DB and all the metadata files", {'p', "path"});
  args::ValueFlag<std::string> wpath_cmd(group4, "wpath", "path for workload files", {"wp", "wpath"});
  args::ValueFlag<int> num_levels_cmd(group1, "L", "The number of levels to fill up with data [def: -1]", {'L', "num_levels"});

  args::Flag print_sst_stat_cmd(group4, "print_sst_stat", "print the stat of SST files", {"ps", "print_sst"});
  args::ValueFlag<int> warmup_queries_cmd(group4, "warmup_queries", "The number of queries for warmup", {"WQ", "warmup_queries"});

  try {
      parser.ParseCLI(argc, argv);
  }
  catch (args::Help&) {
      std::cout << parser;
      exit(0);
      // return 0;
  }
  catch (args::ParseError& e) {
      std::cerr << e.what() << std::endl;
      std::cerr << parser;
      return 1;
  }
  catch (args::ValidationError& e) {
      std::cerr << e.what() << std::endl;
      std::cerr << parser;
      return 1;
  }

  _env->size_ratio = size_ratio_cmd ? args::get(size_ratio_cmd) : 2;
  _env->buffer_size_in_pages = buffer_size_in_pages_cmd ? args::get(buffer_size_in_pages_cmd) : 128;
  _env->entries_per_page = entries_per_page_cmd ? args::get(entries_per_page_cmd) : 128;
  _env->entry_size = entry_size_cmd ? args::get(entry_size_cmd) : 128;
  _env->buffer_size = buffer_size_cmd ? args::get(buffer_size_cmd) : _env->buffer_size_in_pages * _env->entries_per_page * _env->entry_size;
  _env->file_to_memtable_size_ratio = file_to_memtable_size_ratio_cmd ? args::get(file_to_memtable_size_ratio_cmd) : 1;
  _env->file_size = file_size_cmd ? args::get(file_size_cmd) : _env->file_to_memtable_size_ratio * _env-> buffer_size;
  _env->verbosity = verbosity_cmd ? args::get(verbosity_cmd) : 0;
  num_lookup_threads = num_lookup_threads_cmd ? args::get(num_lookup_threads_cmd) : 1;
  _env->compaction_pri = compaction_pri_cmd ? args::get(compaction_pri_cmd) : 1;
  _env->compaction_style = compaction_style_cmd ? args::get(compaction_style_cmd) : 1;
  _env->partition_filters = partition_filter_cmd ? true : false;
  _env->modular_filters= modular_filters_cmd ? true : false;
  _env->adaptive_pf_mf = adaptive_prefetch_modular_filters_cmd ? true : false;
  _env->require_all_modules= require_all_modules_cmd ? true : false;
  _env->allow_whole_filter_skipping = allow_whole_filter_skipping_cmd ? true : false;
  _env->concurrent_load = concurrent_loading_modules_cmd ? true : false;
  _env->no_block_cache = block_cache_cmd ? false : true;
  _env->block_cache_capacity = block_cache_capacity_cmd ? args::get(block_cache_capacity_cmd) : 8*1024*1024;
  _env->block_cache_high_priority_ratio = block_cache_high_priority_ratio_cmd ? args::get(block_cache_high_priority_ratio_cmd) : 0.5;
  _env->bits_per_key = bits_per_key_cmd ? args::get(bits_per_key_cmd) : 0;
  _env->prefetch_bpk = prefetch_bpk_cmd ? args::get(prefetch_bpk_cmd) : 0;
  _env->bpk_bounded = bounded_prefetch_bpk_cmd ? args::get(bounded_prefetch_bpk_cmd) : 0;

  _env->experiment_runs = experiment_runs_cmd ? args::get(experiment_runs_cmd) : 1;
//  _env->num_inserts = num_inserts_cmd ? args::get(num_inserts_cmd) : 0;
  _env->clear_sys_page_cache = clear_sys_page_cache_cmd ? true : false;
  _env->destroy = destroy_cmd ? true : false;
  _env->use_direct_reads = direct_reads_cmd ? true : false;
  _env->path = path_cmd ? args::get(path_cmd) : kDBPath;
  _env->wpath = wpath_cmd ? args::get(wpath_cmd) : workloadPath;
  _env->num_levels = num_levels_cmd ? args::get(num_levels_cmd) : 999;
  _env->print_sst_stat = print_sst_stat_cmd ? true : false;
  warmup_queries = warmup_queries_cmd ? args::get(warmup_queries_cmd) : 0;
  return 0;
}

void printEmulationOutput(const EmuEnv* _env, const QueryTracker *track, uint16_t runs) {
  int l = 16;
  std::cout << std::endl;
  std::cout << "-----LSM state-----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "T" << std::setfill(' ') << std::setw(l) 
                                                  << "P" << std::setfill(' ') << std::setw(l) 
                                                  << "B" << std::setfill(' ') << std::setw(l) 
                                                  << "E" << std::setfill(' ') << std::setw(l) 
                                                  << "M" << std::setfill(' ') << std::setw(l) 
                                                  << "f" << std::setfill(' ') << std::setw(l) 
                                                  << "file_size" << std::setfill(' ') << std::setw(l) 
                                                  << "compaction_pri" << std::setfill(' ') << std::setw(l) 
                                                  << "bpk" << std::setfill(' ') << std::setw(l);
  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << _env->size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size_in_pages;  
  std::cout << std::setfill(' ') << std::setw(l) << _env->entries_per_page;
  std::cout << std::setfill(' ') << std::setw(l) << _env->entry_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->buffer_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->file_to_memtable_size_ratio;
  std::cout << std::setfill(' ') << std::setw(l) << _env->file_size;
  std::cout << std::setfill(' ') << std::setw(l) << _env->compaction_pri;
  std::cout << std::setfill(' ') << std::setw(l) << _env->bits_per_key;
  std::cout << std::endl;

  std::cout << std::endl;
  std::cout << "----- Query summary -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "#I" << std::setfill(' ') << std::setw(l)
                                                << "#U" << std::setfill(' ') << std::setw(l)
                                                << "#D" << std::setfill(' ') << std::setw(l)
                                                << "#R" << std::setfill(' ') << std::setw(l)
                                                << "#Q" << std::setfill(' ') << std::setw(l)
                                                << "#Z" << std::setfill(' ') << std::setw(l)
                                                << "#S" << std::setfill(' ') << std::setw(l)
                                                << "#TOTAL" << std::setfill(' ') << std::setw(l);;            
  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << track->inserts_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->updates_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->point_deletes_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->range_deletes_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->point_lookups_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->zero_point_lookups_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->range_lookups_completed/runs;
  std::cout << std::setfill(' ') << std::setw(l) << track->total_completed/runs;
  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "I" << std::setfill(' ') << std::setw(l)
                                                << "U" << std::setfill(' ') << std::setw(l)
                                                << "D" << std::setfill(' ') << std::setw(l)
                                                << "R" << std::setfill(' ') << std::setw(l)
                                                << "Q" << std::setfill(' ') << std::setw(l)
                                                << "Z" << std::setfill(' ') << std::setw(l)
                                                << "S" << std::setfill(' ') << std::setw(l)
                                                << "TOTAL" << std::setfill(' ') << std::setw(l);   

  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->inserts_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->updates_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->point_deletes_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_deletes_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4) 
                                              << static_cast<double>(track->point_lookups_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4) 
                                              << static_cast<double>(track->zero_point_lookups_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_lookups_cost)/runs/1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->workload_exec_time)/runs/1000000;
  std::cout << std::endl;

  std::cout << "----- Latency(ms/op) -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "avg_I" << std::setfill(' ') << std::setw(l)
                                                << "avg_U" << std::setfill(' ') << std::setw(l)
                                                << "avg_D" << std::setfill(' ') << std::setw(l)
                                                << "avg_R" << std::setfill(' ') << std::setw(l)
                                                << "avg_Q" << std::setfill(' ') << std::setw(l)
                                                << "avg_Z" << std::setfill(' ') << std::setw(l)
                                                << "avg_S" << std::setfill(' ') << std::setw(l)
                                                << "avg_query" << std::setfill(' ') << std::setw(l);   

  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->inserts_cost) / track->inserts_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->updates_cost) / track->updates_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->point_deletes_cost) / track->point_deletes_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_deletes_cost) / track->range_deletes_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4) 
                                              << static_cast<double>(track->point_lookups_cost) / track->point_lookups_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4) 
                                              << static_cast<double>(track->zero_point_lookups_cost) / track->zero_point_lookups_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->range_lookups_cost) / track->range_lookups_completed / 1000000;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2) 
                                              << static_cast<double>(track->workload_exec_time) / track->total_completed / 1000000;
  std::cout << std::endl;

  std::cout << "----- Throughput(op/ms) -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "avg_I" << std::setfill(' ') << std::setw(l)
            << "avg_U" << std::setfill(' ') << std::setw(l)
            << "avg_D" << std::setfill(' ') << std::setw(l)
            << "avg_R" << std::setfill(' ') << std::setw(l)
            << "avg_Q" << std::setfill(' ') << std::setw(l)
            << "avg_Z" << std::setfill(' ') << std::setw(l)
            << "avg_S" << std::setfill(' ') << std::setw(l)
            << "avg_query" << std::setfill(' ') << std::setw(l);

  std::cout << std::endl;

  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->inserts_completed*1000000) / track->inserts_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->updates_completed*1000000) / track->updates_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->point_deletes_completed*1000000) / track->point_deletes_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->range_deletes_completed*1000000) / track->range_deletes_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4)
            << static_cast<double>(track->point_lookups_completed*1000000) / track->point_lookups_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(4)
            << static_cast<double>(track->zero_point_lookups_completed*1000000) / track->zero_point_lookups_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->range_lookups_completed*1000000) / track->range_lookups_cost;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << static_cast<double>(track->total_completed*1000000) / track->workload_exec_time;
  std::cout << std::endl;

  std::cout << "----- Compaction costs -----" << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << "avg_space_amp" << std::setfill(' ') << std::setw(l)
            << "avg_write_amp" << std::setfill(' ') << std::setw(l)
            << "avg_read_amp" << std::setfill(' ') << std::setw(l)
            << "avg_stalls" << std::setfill(' ') << std::setw(l);

  std::cout << std::endl;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << track->space_amp/runs;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << track->write_amp/runs;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << track->read_amp/runs;
  std::cout << std::setfill(' ') << std::setw(l) << std::fixed << std::setprecision(2)
            << track->stalls/runs;
  std::cout << std::endl;

  if (_env->verbosity >= 1) {
    std::cout << std::endl;
    std::cout << "-----I/O stats-----" << std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << "mem_get_count" << std::setfill(' ') << std::setw(l)
                                                  << "mem_get_time" << std::setfill(' ') << std::setw(l)
                                                  << "sst_get_time" << std::setfill(' ') << std::setw(l)
                                                  << "mem_bloom_hit" << std::setfill(' ') << std::setw(l)
                                                  << "mem_bloom_miss" << std::setfill(' ') << std::setw(l)
                                                  << "sst_bloom_hit" << std::setfill(' ') << std::setw(l)
                                                  << "sst_bloom_miss" << std::setfill(' ') << std::setw(l)  
                                                  << "get_cpu_nanos" << std::setfill(' ') << std::setw(l);  
    std::cout << std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << track->get_from_memtable_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->get_from_memtable_time/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->get_from_output_files_time/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->bloom_memtable_hit_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->bloom_memtable_miss_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->bloom_sst_hit_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->bloom_sst_miss_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->get_cpu_nanos/runs;
    std::cout << std::endl;


    std::cout << std::setfill(' ') << std::setw(l) << "bloom_accesses" << std::setfill(' ') << std::setw(l) 
                                                  << "index_accesses" << std::setfill(' ') << std::setw(l)
                                                  << "filter_blk_hit" << std::setfill(' ') << std::setw(l)
                                                  << "index_blk_hit" << std::setfill(' ') << std::setw(l);
    std::cout << std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << track->filter_block_read_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->index_block_read_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->block_cache_filter_hit_count/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->block_cache_index_hit_count/runs;
    std::cout << std::endl;

    std::cout << std::setfill(' ') << std::setw(l) << "read_bytes" << std::setfill(' ') << std::setw(l)
                                                  << "read_nanos" << std::setfill(' ') << std::setw(l)
                                                  << "cpu_read_nanos" << std::setfill(' ') << std::setw(l)
                                                  << "write_bytes" << std::setfill(' ') << std::setw(l)
                                                  << "write_nanos" << std::setfill(' ') << std::setw(l)
                                                  << "cpu_write_nanos" << std::setfill(' ') << std::setw(l);  
    std::cout << std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << track->bytes_read/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->read_nanos/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->cpu_read_nanos/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->bytes_written/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->write_nanos/runs;
    std::cout << std::setfill(' ') << std::setw(l) << track->cpu_write_nanos/runs;
    std::cout << std::endl;
  }
    std::cout << std::endl;
    std::cout << "-----Other stats-----" << std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << "read_table_mem" << std::setfill(' ') << std::setw(l) << "block_cache_usage" << std::setfill(' ') << std::setw(l)<< std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << track->read_table_mem/runs << std::setfill(' ') << std::setw(l) << track->block_cache_usage << std::setfill(' ') << std::setw(l) << std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << "user_cpu_usage" << std::setfill(' ') << std::setw(l) << "sys_cpu_usage" << std::setfill(' ') << std::setw(l)<< std::endl;
    std::cout << std::setfill(' ') << std::setw(l) << track->ucpu_pct/runs << std::setfill(' ') << std::setw(l) << track->scpu_pct/runs << std::setfill(' ') << std::setw(l) << std::endl;
}


