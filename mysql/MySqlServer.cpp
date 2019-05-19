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
 * This is a implementation of the mapkeeper interface that uses mysql.
 */
#include <mysql.h>
#include <mysqld_error.h>
#include <arpa/inet.h>
#include "MapKeeper.h"
#include <boost/thread/tss.hpp>
#include <boost/lexical_cast.hpp>

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace mapkeeper;
using boost::shared_ptr;

class MySqlServer: virtual public MapKeeperIf {
public:
    MySqlServer(const std::string& host, uint32_t port) :
        host_(host),
        port_(port),
        mysql_(new boost::thread_specific_ptr<MYSQL>(destroyMySql)) {
    }

    ~MySqlServer() {
      delete mysql_;
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        initMySql();
        std::string query = "create table " + escapeString(mapName) + 
            "(record_key varbinary(512) primary key, record_value longblob not null) engine=innodb";
        int result = mysql_real_query(mysql_->get(), query.c_str(), query.length());
        if (result != 0) {
            uint32_t error = mysql_errno(mysql_->get());
            if (error == ER_TABLE_EXISTS_ERROR) {
                return ResponseCode::MapExists;
            } else {
                fprintf(stderr, "%d %s\n", error, mysql_error(mysql_->get()));
                return ResponseCode::Error;
            }
        }
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        initMySql();
        std::string query = "drop table " + escapeString(mapName);
        int result = mysql_real_query(mysql_->get(), query.c_str(), query.length());
        if (result != 0) {
            uint32_t error = mysql_errno(mysql_->get());
            if (error == ER_BAD_TABLE_ERROR) {
                return ResponseCode::MapNotFound;
            } else {
                fprintf(stderr, "%d %s\n", error, mysql_error(mysql_->get()));
                return ResponseCode::Error;
            }
        }
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        initMySql();
        _return.responseCode = ResponseCode::Success;
        _return.values.clear();
        std::string query = "show tables";
        int result = mysql_real_query(mysql_->get(), query.c_str(), query.length());
        if (result != 0) {
            uint32_t error = mysql_errno(mysql_->get());
            fprintf(stderr, "%d %s\n", error, mysql_error(mysql_->get()));
            return;
        }

        MYSQL_RES* res = mysql_store_result(mysql_->get());
        MYSQL_ROW row;

        while ((row = mysql_fetch_row(res))) {
            uint64_t* lengths = mysql_fetch_lengths(res);
            _return.values.push_back(std::string(row[0], lengths[0]));
        }
        mysql_free_result(res);
    }

    void scan(RecordListResponse& _return, const std::string& mapName, const ScanOrder::type order,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        initMySql();
        std::string query = "select record_key, record_value from " + 
            escapeString(mapName) + " where record_key " + 
            (startKeyIncluded ? ">=" : ">") + " '" + escapeString(startKey) + "'";
        if (!endKey.empty()) {
            query += " and record_key " +
                (endKeyIncluded ? std::string("<=") : std::string("<")) + "'" + escapeString(endKey) + "'";
        }
        query += " order by record_key";
        if (order == mapkeeper::ScanOrder::Descending) {
            query += " desc";
        }
        if (maxRecords > 0) {
            query += " limit " + boost::lexical_cast<std::string>(maxRecords);
        }

        int result = mysql_real_query(mysql_->get(), query.c_str(), query.length());
        if (result != 0) {
            uint32_t error = mysql_errno(mysql_->get());
            if (error == ER_NO_SUCH_TABLE) {
                _return.responseCode = mapkeeper::ResponseCode::MapNotFound;
                return;
            } else {
                fprintf(stderr, "%d %s\n", error, mysql_error(mysql_->get()));
                _return.responseCode = mapkeeper::ResponseCode::Error;
                return;
            }
        }

        MYSQL_RES* res = mysql_store_result(mysql_->get());
        MYSQL_ROW row;

        int32_t numBytes = 0;
        while ((row = mysql_fetch_row(res))) {
            uint64_t* lengths = mysql_fetch_lengths(res);
            mapkeeper::Record record;
            record.key = std::string(row[0], lengths[0]);
            record.value = std::string(row[1], lengths[1]);
            numBytes += lengths[0] + lengths[1];
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                mysql_free_result(res);
                _return.responseCode = mapkeeper::ResponseCode::Success;
                return;
            }
        }
        mysql_free_result(res);
        _return.responseCode = mapkeeper::ResponseCode::ScanEnded;
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        initMySql();

        std::string query = "select record_value from " + escapeString(mapName) + 
            " where record_key = '" + escapeString(key) + "'";
        int result = mysql_real_query(mysql_->get(), query.c_str(), query.length());
        if (result != 0) {
            uint32_t error = mysql_errno(mysql_->get());
            if (error == ER_NO_SUCH_TABLE) {
                _return.responseCode = ResponseCode::MapNotFound;
                return;
            } else {
                fprintf(stderr, "%d %s\n", error, mysql_error(mysql_->get()));
                _return.responseCode = ResponseCode::Error;
                return;
            }
        }
        MYSQL_RES* res = mysql_store_result(mysql_->get());
        uint32_t numRows = mysql_num_rows(res);
        if (numRows == 0) {
            mysql_free_result(res);
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        } else if (numRows != 1) {
            fprintf(stderr, "select returned %d rows.\n", numRows);
            mysql_free_result(res);
            _return.responseCode = ResponseCode::Error;
            return;
        }
        MYSQL_ROW row = mysql_fetch_row(res);
        uint64_t* lengths = mysql_fetch_lengths(res);
        assert(row);
        _return.value = std::string(row[0], lengths[0]);
        mysql_free_result(res);
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        initMySql();
        std::string query = "insert " + escapeString(mapName) + " values('" + 
            escapeString(key) + "', '" + 
            escapeString(value) + "')";
        int result = mysql_real_query(mysql_->get(), query.c_str(), query.length());
        if (result != 0) {
            uint32_t error = mysql_errno(mysql_->get());
            if (error == ER_NO_SUCH_TABLE) {
                return ResponseCode::MapNotFound;
            } else if (error == ER_DUP_ENTRY) {
                return ResponseCode::RecordExists;
            } else {
                fprintf(stderr, "%d %s\n", error, mysql_error(mysql_->get()));
                return ResponseCode::Error;
            }
        }
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        initMySql();
        std::string query = "update " + escapeString(mapName) + " set record_value = '" + 
            escapeString(value) + "' where record_key = '" +  escapeString(key) + "'";
        int result = mysql_real_query(mysql_->get(), query.c_str(), query.length());
        if (result != 0) {
            uint32_t error = mysql_errno(mysql_->get());
            if (error == ER_NO_SUCH_TABLE) {
                return ResponseCode::MapNotFound;
            } else {
                fprintf(stderr, "%d %s\n", error, mysql_error(mysql_->get()));
                return ResponseCode::Error;
            }
        }
        uint64_t numRows = mysql_affected_rows(mysql_->get());
        if (numRows == 0) {
            return ResponseCode::RecordNotFound;
        } else if (numRows != 1) {
            fprintf(stderr, "update affected %ld rows\n", numRows);
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        initMySql();
        std::string query = "delete from " + escapeString(mapName) + 
            " where record_key = '" +  escapeString(key) + "'";
        int result = mysql_real_query(mysql_->get(), query.c_str(), query.length());
        if (result != 0) {
            uint32_t error = mysql_errno(mysql_->get());
            if (error == ER_NO_SUCH_TABLE) {
                return ResponseCode::MapNotFound;
            } else {
                fprintf(stderr, "%d %s\n", error, mysql_error(mysql_->get()));
                return ResponseCode::Error;
            }
        }
        uint64_t numRows = mysql_affected_rows(mysql_->get());
        if (numRows == 0) {
            return ResponseCode::RecordNotFound;
        } else if (numRows != 1) {
            fprintf(stderr, "update affected %ld rows\n", numRows);
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

private:
    std::string escapeString(const std::string& str) {
        initMySql();
        // http://dev.mysql.com/doc/refman/4.1/en/mysql-real-escape-string.html
        char buffer[2 * str.length() + 1];
        uint64_t length = mysql_real_escape_string(mysql_->get(), buffer, str.c_str(), str.length());
        return std::string(buffer, length);
    }

    void initMySql() {
        if (mysql_->get()) {
            return;
        }
        MYSQL* mysqlPtr = (MYSQL*)malloc(sizeof(MYSQL));
        mysql_->reset(mysqlPtr);
        assert(mysql_->get() == mysql_init(mysql_->get()));
        
        // Automatically reconnect if the connection is lost.
        // http://dev.mysql.com/doc/refman/5.0/en/mysql-options.html
        my_bool reconnect = 1;
        assert(0 == mysql_options(mysql_->get(), MYSQL_OPT_RECONNECT, &reconnect));

        assert(mysql_->get() == mysql_real_connect(mysql_->get(), 
            host_.c_str(),  // hostname
            "root",         // user 
            NULL,           // password 
            NULL,           // default database
            port_,          // port 
            NULL,           // unix socket
            0               // flags
        ));
        std::string query = "create database if not exists mapkeeper";
        assert(0 == mysql_real_query(mysql_->get(), query.c_str(), query.length()));
        query = "use mapkeeper";
        assert(0 == mysql_real_query(mysql_->get(), query.c_str(), query.length()));
    }

    static void destroyMySql(MYSQL* mysql) {
        mysql_close(mysql);
        free(mysql);
    }

    std::string host_;
    uint32_t port_;
    boost::thread_specific_ptr<MYSQL>* mysql_;
};

int main(int argc, char **argv) {
    int port = 9090;
    shared_ptr<MySqlServer> handler(new MySqlServer("localhost", 3306));
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
