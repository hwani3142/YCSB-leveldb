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
#ifndef HANDLERSOCKETCLIENT_H
#define HANDLERSOCKETCLIENT_H
#include <boost/ptr_container/ptr_map.hpp>
#include <string>
#include <mysql.h>
#include "MapKeeper.h"
#include "hstcpcli.hpp"
#include "string_util.hpp"

using namespace dena;

class HandlerSocketClient {
public:
    enum ResponseCode {
        Success,
        Error,
        TableExists,
        TableNotFound,
        RecordExists,
        RecordNotFound,
        ScanEnded,
    };

    HandlerSocketClient(const std::string& host, uint32_t mysqlPort, uint32_t hsReadPort, uint32_t hsWritePort);
    ResponseCode createTable(const std::string& tableName);
    ResponseCode dropTable(const std::string& tableName);
    ResponseCode insert(const std::string& tableName, const std::string& key, const std::string& value);
    ResponseCode update(const std::string& tableName, const std::string& key, const std::string& value);
    ResponseCode get(const std::string& tableName, const std::string& key, std::string& value);
    ResponseCode remove(const std::string& tableName, const std::string& key);
    void scan (mapkeeper::RecordListResponse& _return, const std::string& mapName, const mapkeeper::ScanOrder::type order,
            const std::string& startKey, const bool startKeyIncluded,
            const std::string& endKey, const bool endKeyIncluded,
            const int32_t maxRecords, const int32_t maxBytes);

private:
    static const std::string DBNAME;
    static const std::string FIELDS;
    std::string escapeString(const std::string& str);
    ResponseCode getTableId(const std::string& tableName, uint32_t& id);
    MYSQL mysql_;
    hstcpcli_ptr reader_;
    hstcpcli_ptr writer_;
    std::string host_;
    uint32_t mysqlPort_;
    uint32_t hsReaderPort_;
    uint32_t hsWriterPort_;
    uint32_t currentTableId_;
    std::map<std::string, uint32_t> tableIds_;
};

#endif
