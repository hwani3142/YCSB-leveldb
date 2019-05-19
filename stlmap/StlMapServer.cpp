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
 * This is a stub implementation of the mapkeeper interface that uses 
 * std::map. Data is not persisted.
 */
#include <map>
#include <string>
#include <arpa/inet.h>
#include "MapKeeper.h"

#include <boost/thread/shared_mutex.hpp>
#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace std;
using namespace mapkeeper;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

class StlMapServer: virtual public MapKeeperIf {
public:
    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const string& mapName) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        map<string, map<string, string> >::iterator itr = maps_.find(mapName);
        if (itr != maps_.end()) {
            return ResponseCode::MapExists;
        }
        map<string, string> newMap;
        pair<string, map<string, string> > entry(mapName, newMap);
        maps_.insert(entry);
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const string& mapName) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        map<string, map<string, string> >::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        maps_.erase(itr);
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        map<string, map<string, string> >::iterator itr;
        for (itr = maps_.begin(); itr != maps_.end(); itr++) {
            _return.values.push_back(itr->first);
        }
        _return.responseCode = ResponseCode::Success;
    }

    void scan(RecordListResponse& _return, const string& mapName, const ScanOrder::type order,
              const string& startKey, const bool startKeyIncluded,
              const string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        map<string, map<string, string> >::iterator itr = maps_.find(mapName);
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

    void scanAscending(RecordListResponse& _return, const map<string, string>& mymap,
              const string& startKey, const bool startKeyIncluded,
              const string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        map<string, string>::const_iterator itr = startKeyIncluded ? 
            mymap.lower_bound(startKey):
            mymap.upper_bound(startKey);
        int numBytes = 0;
        while (itr != mymap.end()) {
            if (!endKey.empty()) {
                if (endKeyIncluded && endKey < itr->first) {
                  break;
                }
                if (!endKeyIncluded && endKey <= itr->first) {
                  break;
                }
            }
            Record record;
            record.key = itr->first;
            record.value = itr->second;
            numBytes += record.key.size() + record.value.size();
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                _return.responseCode = ResponseCode::Success;
                return;
            }
            itr++;
        }
        _return.responseCode = ResponseCode::ScanEnded;
    }
 
    void scanDescending(RecordListResponse& _return, const map<string, string>& mymap,
              const string& startKey, const bool startKeyIncluded,
              const string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        map<string, string>::const_iterator itr;
        if (endKey.empty()) {
            itr = mymap.end();
        } else {
            itr = endKeyIncluded ? mymap.upper_bound(endKey) : mymap.lower_bound(endKey);
        }
        int numBytes = 0;
        while (itr != mymap.begin()) {
            itr--;
            if (startKeyIncluded && startKey > itr->first) {
                break;
            }
            if (!startKeyIncluded && startKey >= itr->first) {
                break;
            }
            Record record;
            record.key = itr->first;
            record.value = itr->second;
            numBytes += record.key.size() + record.value.size();
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                _return.responseCode = ResponseCode::Success;
                return;
            }
        }
        _return.responseCode = ResponseCode::ScanEnded;
    }
 
    void get(BinaryResponse& _return, const string& mapName, const string& key) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        map<string, map<string, string> >::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        map<string, string>::iterator recordIterator = itr->second.find(key);
        if (recordIterator == itr->second.end()) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        }
        _return.responseCode = ResponseCode::Success;
        _return.value = recordIterator->second;
    }

    ResponseCode::type put(const string& mapName, const string& key, const string& value) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        map<string, map<string, string> >::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        itr->second[key] = value;
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const string& mapName, const string& key, const string& value) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        map<string, map<string, string> >::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        if (itr->second.find(key) != itr->second.end()) {
            return ResponseCode::RecordExists;
        }
        itr->second.insert(pair<string, string>(key, value));
        return ResponseCode::Success;
    }

    ResponseCode::type update(const string& mapName, const string& key, const string& value) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        map<string, map<string, string> >::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        if (itr->second.find(key) == itr->second.end()) {
            return ResponseCode::RecordNotFound;
        }
        itr->second[key] = value;
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const string& mapName, const string& key) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        map<string, map<string, string> >::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        map<string, string>::iterator recordIterator = itr->second.find(key);
        if (recordIterator  == itr->second.end()) {
            return ResponseCode::RecordNotFound;
        }
        itr->second.erase(recordIterator);
        return ResponseCode::Success;
    }

private:
    map<string, map<string, string> > maps_;
    boost::shared_mutex mutex_; // protect map_
};

int main(int argc, char **argv) {
    int port = 9090;
    shared_ptr<StlMapServer> handler(new StlMapServer());
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
