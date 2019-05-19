/*
 * Copyright 2012 Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is a implementation of the mapkeeper interface that uses 
 * leveldb.
 *
 * http://leveldb.googlecode.com/svn/trunk/doc/index.html
 */
#include <iostream>
#include <cstdio>
#include "MapKeeper.h"
#include <leveldb/db.h>
#include <leveldb/cache.h>
#include <boost/program_options.hpp>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/filesystem.hpp>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <arpa/inet.h>

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>


using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;
using namespace boost::filesystem;
namespace po = boost::program_options;

using namespace mapkeeper;

int syncmode;
int blindinsert;
int blindupdate;
class LevelDbServer: virtual public MapKeeperIf {
public:
    LevelDbServer(const std::string& directoryName,
                  uint32_t writeBufferSizeMb, uint32_t blockCacheSizeMb) : 
        directoryName_(directoryName),
        writeBufferSizeMb_(writeBufferSizeMb),
        blockCacheSizeMb_(blockCacheSizeMb) {
        cache_ = leveldb::NewLRUCache(blockCacheSizeMb_ * 1024 * 1024);

        // open all the existing databases
        leveldb::DB* db;
        leveldb::Options options;
        options.create_if_missing = false;
        options.error_if_exists = false;
        //options.write_buffer_size = writeBufferSizeMb_ * 1000 * 1000;
        options.write_buffer_size = writeBufferSizeMb_ * 1024 * 1024;
        options.block_cache = cache_;
        options.compression = leveldb::kNoCompression;

        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;

        directory_iterator end_itr;
        for (directory_iterator itr(directoryName); itr != end_itr;itr++) {
            if (is_directory(itr->status())) {
                std::string mapName = itr->path().filename().string();
                leveldb::Status status = leveldb::DB::Open(options, itr->path().string(), &db);
                assert(status.ok()); // DEBUG: occurs error
                maps_.insert(mapName, db);
            }
        }
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
/**/
	std::string mapName_ = mapName;
	boost::ptr_map<std::string, leveldb::DB>::iterator itr;
	boost::unique_lock< boost::shared_mutex > writeLock(mutex_);
	itr = maps_.find(mapName_);
	if (itr != maps_.end())
		return ResponseCode::MapExists;	
/**/
        leveldb::DB* db;
        leveldb::Options options;
        options.create_if_missing = true;
        options.error_if_exists = false;
        options.write_buffer_size = writeBufferSizeMb_ * 1024 * 1024;
        options.block_cache = cache_;
        leveldb::Status status = leveldb::DB::Open(options, directoryName_ + "/" + mapName, &db);
        if (!status.ok()) {
            // TODO check return code
            printf("[status]: %s\n", status.ToString().c_str());
            return ResponseCode::MapExists;
        }
/*
        std::string mapName_ = mapName;
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
*/
        maps_.insert(mapName_, db);
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr;
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        maps_.erase(itr);
        DestroyDB(directoryName_ + "/" + mapName, leveldb::Options());
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr;
        for (itr = maps_.begin(); itr != maps_.end(); itr++) {
            _return.values.push_back(itr->first);
        }
        _return.responseCode = ResponseCode::Success;
    }

    void scan(RecordListResponse& _return, const std::string& mapName, const ScanOrder::type order,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::Success;
            return;
        }
        if (order == ScanOrder::Ascending) {
            scanAscending(_return, itr->second, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
        } else {
            scanDescending(_return, itr->second, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
        }
    }

    void scanAscending(RecordListResponse& _return, leveldb::DB* db, 
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        _return.responseCode = ResponseCode::ScanEnded;
        int numBytes = 0;
        leveldb::Iterator* itr = db->NewIterator(leveldb::ReadOptions());
        _return.responseCode = ResponseCode::ScanEnded;
	//printf("scan Start\n");
	//int i=0;
        for (itr->Seek(startKey); itr->Valid(); itr->Next()) {
	    //printf("scan %d\n", i);
            Record record;
            record.key = itr->key().ToString();
            record.value = itr->value().ToString();
            if (!startKeyIncluded && startKey == record.key) {
                continue;
            }
            if (!endKey.empty()) {
                if (endKeyIncluded && endKey < record.key) {
                  break;
                }
                if (!endKeyIncluded && endKey <= record.key) {
                  break;
                }
            }
            numBytes += record.key.size() + record.value.size();
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                _return.responseCode = ResponseCode::Success;
                break;
            }
	    //i++;
        }

        assert(itr->status().ok()); // occur error
        delete itr;
    }

    void scanDescending(RecordListResponse& _return, leveldb::DB* db,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        int numBytes = 0;
        leveldb::Iterator* itr = db->NewIterator(leveldb::ReadOptions());
        _return.responseCode = ResponseCode::ScanEnded;
        if (endKey.empty()) {
            itr->SeekToLast();
        } else {
            itr->Seek(endKey);
        }
        for (; itr->Valid(); itr->Prev()) {
            Record record;
            record.key = itr->key().ToString();
            record.value = itr->value().ToString();
            if (!endKeyIncluded && endKey == record.key) {
                continue;
            }
            if (startKeyIncluded && startKey > record.key) {
                break;
            }
            if (!startKeyIncluded && startKey >= record.key) {
                break;
            }
            numBytes += record.key.size() + record.value.size();
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                _return.responseCode = ResponseCode::Success;
                break;
            }
        }
        assert(itr->status().ok());
        delete itr;
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        leveldb::Status status = itr->second->Get(leveldb::ReadOptions(), key, &(_return.value));
        if (status.IsNotFound()) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        } else if (!status.ok()) {
            _return.responseCode = ResponseCode::Error;
            return;
        }
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr;
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;

        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }

        leveldb::WriteOptions options;
        options.sync = syncmode ? true : false;
        leveldb::Status status = itr->second->Put(options, key, value);

        if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
	//printf("insert start %d\n", value.size());
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
	if(!blindinsert) {
	  std::string recordValue;
	  leveldb::Status status = itr->second->Get(leveldb::ReadOptions(), key, &recordValue);
	  if (status.ok()) {
            return ResponseCode::RecordExists;
	  } else if (!status.IsNotFound()) {
            return ResponseCode::Error;
	  }
	}
        leveldb::WriteOptions options;
        options.sync = syncmode ? true : false;
	leveldb::Status status = itr->second->Put(options, key, value);
        if (!status.ok()) {
            printf("insert not ok! %s\n", status.ToString().c_str());
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
	//printf("update start %d\n", value.size());
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
	//printf("update 1 \n");
        if (itr == maps_.end()) {
	    printf("Map Not Found '%s'\n", key.c_str());
            return ResponseCode::MapNotFound;
        }
	//printf("update 2 \n");
        std::string recordValue;
	if(!blindupdate) {
	  leveldb::Status status = itr->second->Get(leveldb::ReadOptions(), key, &recordValue);
	  if (status.IsNotFound()) {
	    printf("Update Get notfound '%s'\n", key.c_str());
            return ResponseCode::RecordNotFound;
	  } else if (!status.ok()) {
	    printf("Update Get Error '%s'\n", key.c_str());
            return ResponseCode::Error;
	  }
	}
	//printf("update 3 \n");
        leveldb::WriteOptions options;
        options.sync = syncmode ? true : false;
	// NOTE: Modify value size * 10
	// value size in insertion is 1080, but value size in update = 108
	std::string value_10X;
	for (int i=0; i<10; i++) {
	  value_10X.append(value);
	}
	//printf("update 10X %d\n", value_10X.size());
	//leveldb::Status status = itr->second->Put(options, key, value);
	leveldb::Status status = itr->second->Put(options, key, value_10X);
        if (!status.ok()) {
	    printf("Put Error '%s'\n", key.c_str());
            return ResponseCode::Error;
        }
	//printf("update 4 \n");
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        leveldb::WriteOptions options;
        options.sync = true;
        leveldb::Status status = itr->second->Delete(options, key);
        printf("status: %s %s %s\n", mapName.c_str(), key.c_str(), status.ToString().c_str());
        if (status.IsNotFound()) {
            return ResponseCode::RecordNotFound;
        } else if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

private:
    std::string directoryName_; // directory to store db files.
    uint32_t writeBufferSizeMb_; 
    uint32_t blockCacheSizeMb_; 
    leveldb::Cache* cache_;
    boost::ptr_map<std::string, leveldb::DB> maps_;
    boost::shared_mutex mutex_; // protect map_
};

int main(int argc, char **argv) {
    int port;
    int writeBufferSizeMb;
    int blockCacheSizeMb;
    std::string dir;
    po::variables_map vm;
    po::options_description config("");
    config.add_options()
        ("help,h", "produce help message")
        ("sync,s", "synchronous writes")
        ("blindinsert,i", "skip record existence check for inserts")
        ("blindupdate,u",  "skip record existence check for updates")
        ("port,p", po::value<int>(&port)->default_value(9090), "port to listen to")
        ("datadir,d", po::value<std::string>(&dir)->default_value("data"), "data directory")
        ("write-buffer-mb,w", po::value<int>(&writeBufferSizeMb)->default_value(1024), "LevelDB write buffer size in MB")
        ("block-cache-mb,b", po::value<int>(&blockCacheSizeMb)->default_value(1024), "LevelDB block cache size in MB")
        ;
    po::options_description cmdline_options;
    cmdline_options.add(config);
    store(po::command_line_parser(argc, argv).options(cmdline_options).run(), vm);
    notify(vm);
    if (vm.count("help")) {
        std::cout << config << std::endl; 
        exit(0);
    }
    syncmode = vm.count("sync");
    blindinsert = vm.count("blindinsert");
    blindupdate = vm.count("blindupdate");
    printf("new LevelDBServer \n");
    shared_ptr<LevelDbServer> handler(new LevelDbServer(dir, writeBufferSizeMb, blockCacheSizeMb));
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server (processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
