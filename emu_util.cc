/*
 *  Created on: Oct 9, 2019
 *  Author: Guanting Chen
 */

#include "emu_util.h"

#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include "stdlib.h"
#include "stdio.h"
#include "sys/times.h"
#include "sys/vtimes.h"

void cpuUsageInit(){
    struct tms timeSample;
    lastCPU = times(&timeSample);
    lastSysCPU = timeSample.tms_stime;
    lastUserCPU = timeSample.tms_utime;
}

CpuUsage getCurrentCpuUsage(){
    struct tms timeSample;
    clock_t now;
    double user_percent, sys_percent;
    CpuUsage result;

    now = times(&timeSample);
    if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
        timeSample.tms_utime < lastUserCPU){
        //Overflow detection. Just skip this value.
                user_percent = 0.0;
                sys_percent = 0.0;
    } else{
        user_percent = timeSample.tms_utime - lastUserCPU;
        user_percent /= (now - lastCPU);
        user_percent *= 100;
        sys_percent = timeSample.tms_stime - lastSysCPU;
        sys_percent /= (now - lastCPU);
        sys_percent *= 100;
     }
     lastCPU = now;
     lastSysCPU = timeSample.tms_stime;
     lastUserCPU = timeSample.tms_utime;
     result.user_time_pct = user_percent;
     result.sys_time_pct = sys_percent; 
     return result;
}

std::vector<std::string> StringSplit(std::string &str, char delim){
  std::vector<std::string> splits;
  std::stringstream ss(str);
  std::string item;
  while(getline(ss, item, delim)){
    splits.push_back(item);
  }
  return splits;
}

Status ReopenDB(DB *&db, const Options &op, const FlushOptions &flush_op) {
	const std::string dbPath = db->GetName();
	Status return_status = Status::Incomplete();
	Status s = db->Flush(flush_op);
	assert(s.ok());
	if (FlushMemTableMayAllComplete(db)
		 && CompactionMayAllComplete(db)) {
		return_status = Status::OK();
	}
	s = db->Close();
	assert(s.ok());
	delete db;
	db = nullptr;
	
	s = DB::Open(op, dbPath, &db);
	assert(s.ok());
	return return_status;
}

Status CloseDB(DB *&db, const FlushOptions &flush_op) {
	Status return_status = Status::Incomplete();
	Status s = db->Flush(flush_op);
	assert(s.ok());
	if (FlushMemTableMayAllComplete(db)
		 && CompactionMayAllComplete(db)) {
		return_status = Status::OK();
	}

	s = db->Close();
	assert(s.ok());
	delete db;
	db = nullptr;
	std::cout << " OK" << std::endl;
	return return_status;
}

// Not working, will trigger SegmentFault
// Wait until the compaction completes
// This function actually does not necessarily
// wait for compact. It actually waits for scheduled compaction
// OR flush to finish.
// Status WaitForCompaction(DB *db, bool waitUnscheduled) {
// 	return (static_cast_with_check<DBImpl, DB>(db->GetRootDB()))
//       		->WaitForCompactAPI(waitUnscheduled);
// }

// Need to select timeout carefully
// Completion not guaranteed
bool CompactionMayAllComplete(DB *db) {
	uint64_t pending_compact;
	uint64_t pending_compact_bytes; 
	uint64_t running_compact;
	bool success = db->GetIntProperty("rocksdb.compaction-pending", &pending_compact)
	 						 && db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pending_compact_bytes)
							 && db->GetIntProperty("rocksdb.num-running-compactions", &running_compact);
	while (pending_compact || pending_compact_bytes || running_compact || !success) {
		sleep_for_ms(200);
		success = db->GetIntProperty("rocksdb.compaction-pending", &pending_compact)
	 						 && db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes", &pending_compact_bytes)
							 && db->GetIntProperty("rocksdb.num-running-compactions", &running_compact);
	}
	return true;
}

// Need to select timeout carefully
// Completion not guaranteed
bool FlushMemTableMayAllComplete(DB *db) {
	uint64_t pending_flush;
	uint64_t running_flush;
	bool success = db->GetIntProperty("rocksdb.mem-table-flush-pending", &pending_flush)
							 && db->GetIntProperty("rocksdb.num-running-flushes", &running_flush);
	while (pending_flush || running_flush || !success) {
		sleep_for_ms(200);
		success = db->GetIntProperty("rocksdb.mem-table-flush-pending", &pending_flush)
							 && db->GetIntProperty("rocksdb.num-running-flushes", &running_flush);
	}
	return ((static_cast_with_check<DBImpl, DB>(db->GetRootDB()))
      		->WaitForFlushMemTable(static_cast<ColumnFamilyHandleImpl*>(db->DefaultColumnFamily())->cfd())) == Status::OK();
}


