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
 * kyoto cabinet.
 */
#include <kchashdb.h>
#include <iostream>
#include <cstdio>
#include "MapKeeper.h"
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

using namespace std;
using namespace kyotocabinet;
using boost::shared_ptr;
using namespace boost::filesystem;
namespace po = boost::program_options;

using namespace mapkeeper;

class KyotoCabinetServer: virtual public MapKeeperIf {
public:
    KyotoCabinetServer(const std::string& directoryName, bool sync, int64_t mmapSizeMb) :
        directoryName_(directoryName),
        sync_(sync),
        mmapSizeMb_(mmapSizeMb) {

        // open all the existing databases
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        directory_iterator end_itr;
        for (directory_iterator itr(directoryName); itr != end_itr; itr++) {
            if (is_regular_file(itr->status())) {
                std::string mapName = itr->path().leaf().string();
                std::string fileName = itr->path().string();

                // skip hidden files
                if (mapName[0] == '.') {
                    continue;
                }

                TreeDB* db = new TreeDB();
                db->tune_map(mmapSizeMb_ << 20);
                uint32_t flags = BasicDB::OWRITER | BasicDB::OAUTOTRAN;
                if (sync_) {
                    flags |= BasicDB::OAUTOSYNC;
                }
                if (!db->open(fileName, flags)) {
                    printf("ERROR: failed to open '%s'\n", fileName.c_str());
                    exit(1);
                }
                maps_.insert(mapName, db);
                printf("INFO: opened '%s'\n", fileName.c_str());
            }
        }
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr != maps_.end()) {
            return ResponseCode::MapExists;
        }
        TreeDB* db = new TreeDB();
        db->tune_map(mmapSizeMb_ << 20);

        uint32_t flags = BasicDB::OWRITER | BasicDB::OCREATE | BasicDB::OAUTOTRAN;
        if (sync_) {
            flags |= BasicDB::OAUTOSYNC;
        }
        if (!db->open(directoryName_ + "/" + mapName,  flags)) {
            return ResponseCode::Error;
        }
        std::string mapName_ = mapName;
        maps_.insert(mapName_, db);
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        if (!itr->second->close()) {
          return ResponseCode::Error;
        }
        maps_.erase(itr);
        // TODO error check. portable code.
        unlink((directoryName_ + "/" + mapName).c_str());
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        _return.values.clear();
        boost::ptr_map<std::string, TreeDB>::iterator itr;
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
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        if (order == ScanOrder::Ascending) {
            scanAscending(_return, itr->second, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
        } else {
            scanDescending(_return, itr->second, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
        }
 
    }

    void scanAscending(RecordListResponse& _return, TreeDB* db,  
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        int numBytes = 0;
        DB::Cursor* cursor = db->cursor();
        _return.responseCode = ResponseCode::ScanEnded;
        if (!cursor->jump(startKey)) {
          delete cursor;
          return;
        }
        string key, value;
        while (cursor->get(&key, &value, true /* step */)) {
            if (!startKeyIncluded && key == startKey) {
                continue;
            }
            if (!endKey.empty()) {
                if (endKeyIncluded && endKey < key) {
                  break;
                }
                if (!endKeyIncluded && endKey <= key) {
                  break;
                }
            }
            Record record;
            record.key = key;
            record.value = value;
            numBytes += record.key.size() + record.value.size();
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                _return.responseCode = ResponseCode::Success;
                break;
            }
        }
        delete cursor;
    }

    void scanDescending(RecordListResponse& _return, TreeDB* db,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        int numBytes = 0;
        DB::Cursor* cursor = db->cursor();
        _return.responseCode = ResponseCode::ScanEnded;

        if (endKey.empty()) {
            if (!cursor->jump_back()) {
                delete cursor;
                return;
            }
        } else {
            if (!cursor->jump_back(endKey)) {
                delete cursor;
                return;
            }
        }
        string key, value;
        while (cursor->get(&key, &value, false /* step */)) {
            if (!endKeyIncluded && key == endKey) {
                cursor->step_back();
                continue;
            }
            if (startKeyIncluded && startKey > key) {
                break;
            }
            if (!startKeyIncluded && startKey >= key) {
                break;
            }
            Record record;
            record.key = key;
            record.value = value;
            numBytes += record.key.size() + record.value.size();
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                _return.responseCode = ResponseCode::Success;
                break;
            }
            cursor->step_back();
        }
        delete cursor;
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        if (!itr->second->get(key, &(_return.value))) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        }
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, TreeDB>::iterator itr;
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }

        // add() returns false if the record already exists, and
        // the record value is not modified.
        // http://fallabs.com/kyotocabinet/api/classkyotocabinet_1_1BasicDB.html#a330568e1a92d74bfbc38682cd8604462
        if (!itr->second->add(key, value)) {
            return ResponseCode::RecordExists;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }

        // replace() returns false if the record doesn't exist, and no
        // new record is created.
        // http://fallabs.com/kyotocabinet/api/classkyotocabinet_1_1BasicDB.html#ac1b65f395e4be9e9ef14973f564e3a48
        if (!itr->second->replace(key, value)) {
            return ResponseCode::RecordNotFound;
        }
        return ResponseCode::Success;

    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, TreeDB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        if (!itr->second->remove(key)) {
            return ResponseCode::RecordNotFound;
        }
        return ResponseCode::Success;
    }

private:
    std::string directoryName_; // directory to store db files.
    bool sync_; // synchronous write
    int64_t mmapSizeMb_; // used for DB->tune_map
    boost::ptr_map<std::string, TreeDB> maps_;
    boost::shared_mutex mutex_; // protect map_
};

int main(int argc, char **argv) {
    int port;
    int mmapSizeMb;
    std::string dir;
    po::variables_map vm;
    po::options_description config("");
    config.add_options()
        ("help,h", "produce help message")
        ("sync,s", "synchronous writes")
        ("mmap-size-mb,m", po::value<int>(&mmapSizeMb)->default_value(64), "size of the memory-mapped region in MB")
        ("port,p", po::value<int>(&port)->default_value(9090), "port to listen to")
        ("datadir,d", po::value<std::string>(&dir)->default_value("data"), "data directory")
        ;
    po::options_description cmdline_options;
    cmdline_options.add(config);
    store(po::command_line_parser(argc, argv).options(cmdline_options).run(), vm);
    notify(vm);
    if (vm.count("help")) {
        std::cout << config << std::endl; 
        exit(0);
    }
    bool sync = vm.count("sync") > 0;
    shared_ptr<KyotoCabinetServer> handler(new KyotoCabinetServer(dir, sync, mmapSizeMb));
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
