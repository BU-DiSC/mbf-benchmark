include ../rocksdb-mbf/make_config.mk

threads=20

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

ifneq ($(USE_RTTI), 1)
	#CXXFLAGS += -fno-rtti
endif

.PHONY: clean librocksdb

all: mbf-benchmark

mbf-benchmark: librocksdb mbf-benchmark.cc emu_environment.cc emu_util.cc workload_stats.cc aux_time.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ emu_environment.cc emu_util.cc workload_stats.cc aux_time.cc ../rocksdb-mbf/librocksdb.a -I../rocksdb-mbf/include -I../rocksdb-mbf/ -O2 -std=c++11 -frtti $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

mbf-benchmark-debug: librocksdb_debug mbf-benchmark.cc emu_environment.cc emu_util.cc workload_stats.cc aux_time.cc
	$(CXX) $(CXXFLAGS) mbf-benchmark.cc emu_environment.cc emu_util.cc workload_stats.cc aux_time.cc -o$@ ../rocksdb-mbf/librocksdb_debug.a -I../rocksdb-mbf/include -I../rocksdb-mbf/ -O0 -ggdb -std=c++11 -frtti $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)





clean:
	rm -rf ./mbf-benchmark 

librocksdb:
	cd ../rocksdb-mbf && $(MAKE) -j ${threads} static_lib

librocksdb_debug:
	cd ../rocksdb-mbf && $(MAKE) -j ${threads} dbg
