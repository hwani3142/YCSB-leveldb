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
#include <algorithm>
#include <arpa/inet.h>
#include <boost/lexical_cast.hpp>
#include <cassert>
#include "MapKeeper.h"
#include <protocol/TBinaryProtocol.h>
#include <transport/TServerSocket.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>

using namespace std;
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

using boost::shared_ptr;

using namespace mapkeeper;

void testScan(mapkeeper::MapKeeperClient& client) {
    mapkeeper::RecordListResponse scanResponse;
    string mapName("scan_test");
    assert(mapkeeper::ResponseCode::Success == client.addMap("scan_test"));

    // test scanning empty map
    client.scan(scanResponse, mapName, ScanOrder::Ascending, "", true, "", true, 1000, 1000);
    assert(scanResponse.responseCode == mapkeeper::ResponseCode::ScanEnded);
    assert(scanResponse.records.size() == 0);
    client.scan(scanResponse, mapName, ScanOrder::Descending, "", true, "", true, 1000, 1000);
    assert(scanResponse.responseCode == mapkeeper::ResponseCode::ScanEnded);
    assert(scanResponse.records.size() == 0);

    for (int i = 0; i < 10; i++) {
        string key = "key" + boost::lexical_cast<string>(i);
        string val = "val" + boost::lexical_cast<string>(i);
        assert(mapkeeper::ResponseCode::Success == client.insert(mapName, key, val));
    }

    client.scan(scanResponse, mapName, ScanOrder::Ascending, "", true, "", true, 1000, 1000);
    assert(scanResponse.responseCode == mapkeeper::ResponseCode::ScanEnded);
    assert(scanResponse.records.size() == 10);
    vector<mapkeeper::Record>::iterator itr = scanResponse.records.begin();
    for (int i = 0; i < 10; i++) {
        string key = "key" + boost::lexical_cast<string>(i);
        string val = "val" + boost::lexical_cast<string>(i);
        assert(key == itr->key);
        assert(val == itr->value);
        itr++;
    }
    assert(itr == scanResponse.records.end());

    client.scan(scanResponse, mapName, ScanOrder::Ascending, "", false, "key5", true, 1000, 1000);
    assert(scanResponse.responseCode == mapkeeper::ResponseCode::ScanEnded);
    assert(scanResponse.records.size() == 6);
    itr = scanResponse.records.begin();
    for (int i = 0; i < 6; i++) {
        string key = "key" + boost::lexical_cast<string>(i);
        string val = "val" + boost::lexical_cast<string>(i);
        assert(key == itr->key);
        assert(val == itr->value);
        itr++;
    }
    assert(itr == scanResponse.records.end());

    client.scan(scanResponse, mapName, ScanOrder::Ascending, "key2", true, "key7", false, 1000, 1000);
    assert(scanResponse.responseCode == mapkeeper::ResponseCode::ScanEnded);
    assert(scanResponse.records.size() == 5);
    itr = scanResponse.records.begin();
    for (int i = 2; i < 7; i++) {
        string key = "key" + boost::lexical_cast<string>(i);
        string val = "val" + boost::lexical_cast<string>(i);
        assert(key == itr->key);
        assert(val == itr->value);
        itr++;
    }
    assert(itr == scanResponse.records.end());

    client.scan(scanResponse, mapName, ScanOrder::Descending, "key3", false, "", true, 1000, 1000);
    assert(scanResponse.responseCode == mapkeeper::ResponseCode::ScanEnded);
    assert(scanResponse.records.size() == 6);
    itr = scanResponse.records.begin();
    for (int i = 9; i > 3; i--) {
        string key = "key" + boost::lexical_cast<string>(i);
        string val = "val" + boost::lexical_cast<string>(i);
        assert(key == itr->key);
        assert(val == itr->value);
        itr++;
    }
    assert(itr == scanResponse.records.end());

    // test record limit
    client.scan(scanResponse, mapName, ScanOrder::Ascending, "key4", true, "", true, 3, 1000);
    assert(scanResponse.responseCode == mapkeeper::ResponseCode::Success);
    assert(scanResponse.records.size() == 3);
    itr = scanResponse.records.begin();
    for (int i = 4; i < 7; i++) {
        string key = "key" + boost::lexical_cast<string>(i);
        string val = "val" + boost::lexical_cast<string>(i);
        assert(key == itr->key);
        assert(val == itr->value);
        itr++;
    }
    assert(itr == scanResponse.records.end());

    // test byte limit
    client.scan(scanResponse, mapName, ScanOrder::Descending, "key4", true, "key9", false, 1000, 16);
    assert(scanResponse.responseCode == mapkeeper::ResponseCode::Success);
    assert(scanResponse.records.size() == 2);
    itr = scanResponse.records.begin();
    for (int i = 8; i > 6; i--) {
        string key = "key" + boost::lexical_cast<string>(i);
        string val = "val" + boost::lexical_cast<string>(i);
        assert(key == itr->key);
        assert(val == itr->value);
        itr++;
    }
    assert(itr == scanResponse.records.end());


    assert(mapkeeper::ResponseCode::Success == client.dropMap("scan_test"));
}

int main(int argc, char **argv) {
    boost::shared_ptr<TSocket> socket(new TSocket("localhost", 9090));
    boost::shared_ptr<TTransport> transport(new TFramedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    mapkeeper::MapKeeperClient client(protocol);
    mapkeeper::BinaryResponse getResponse;
    mapkeeper::StringListResponse listResponse;

    // these methods can throw apache::thrift::TException.
    transport->open();

    // test ping
    assert(mapkeeper::ResponseCode::Success == client.ping());

    // test addMap
    assert(mapkeeper::ResponseCode::Success == client.addMap("db1"));
    assert(mapkeeper::ResponseCode::MapExists == client.addMap("db1"));
    assert(mapkeeper::ResponseCode::Success == client.addMap("db3"));

    // test insert
    assert(mapkeeper::ResponseCode::Success == client.insert("db1", "k1", "v1"));
    assert(mapkeeper::ResponseCode::RecordExists == client.insert("db1", "k1", "v1"));
    assert(mapkeeper::ResponseCode::MapNotFound == client.insert("db2", "k1", "v1"));

    // test get
    client.get(getResponse, "db1", "k1");
    assert(getResponse.responseCode == mapkeeper::ResponseCode::Success);
    assert(getResponse.value == "v1");
    client.get(getResponse, "db2", "k1");
    assert(getResponse.responseCode == mapkeeper::ResponseCode::MapNotFound);
    client.get(getResponse, "db1", "k2");
    assert(getResponse.responseCode == mapkeeper::ResponseCode::RecordNotFound);

    // test update
    assert(mapkeeper::ResponseCode::Success == client.update("db1", "k1", "v2"));
    assert(mapkeeper::ResponseCode::MapNotFound == client.update("db2", "k1", "v1"));
    assert(mapkeeper::ResponseCode::RecordNotFound == client.update("db1", "k2", "v2"));
    client.get(getResponse, "db1", "k1");
    assert(getResponse.responseCode == mapkeeper::ResponseCode::Success);
    assert(getResponse.value == "v2");
 
    // test scan
    testScan(client);

    // test remove
    assert(mapkeeper::ResponseCode::Success == client.remove("db1", "k1"));
    assert(mapkeeper::ResponseCode::RecordNotFound== client.remove("db1", "k1"));
    assert(mapkeeper::ResponseCode::RecordNotFound== client.remove("db1", "k2"));
    assert(mapkeeper::ResponseCode::MapNotFound == client.remove("db2", "k1"));

    // test listMaps and dropMap
    client.listMaps(listResponse);
    assert(listResponse.responseCode == mapkeeper::ResponseCode::Success);
    assert(listResponse.values.size() == 2);
    assert(find(listResponse.values.begin(), listResponse.values.end(), "db1") != listResponse.values.end());
    assert(find(listResponse.values.begin(), listResponse.values.end(), "db3") != listResponse.values.end());

    assert(mapkeeper::ResponseCode::Success == client.dropMap("db1"));
    assert(mapkeeper::ResponseCode::MapNotFound == client.dropMap("db1"));
    client.listMaps(listResponse);
    assert(listResponse.responseCode == mapkeeper::ResponseCode::Success);
    assert(listResponse.values.size() == 1);
    assert(find(listResponse.values.begin(), listResponse.values.end(), "db3") != listResponse.values.end());

    assert(mapkeeper::ResponseCode::Success == client.dropMap("db3"));
    client.listMaps(listResponse);
    assert(listResponse.responseCode == mapkeeper::ResponseCode::Success);
    assert(listResponse.values.size() == 0);
    transport->close();
    return 0;
}
