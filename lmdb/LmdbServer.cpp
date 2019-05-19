/*
 * Copyright 2013 Symas Corp.
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
#include "MapKeeper.h"

#include <iostream>
#include <protocol/TBinaryProtocol.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <boost/program_options.hpp>
#include <lmdb.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;
namespace po = boost::program_options;

using namespace mapkeeper;

int syncmode;
int blindupdate;

class LmdbServer: virtual public MapKeeperIf {
public:
    LmdbServer(const std::string& directoryName,
    size_t maxSize, size_t numThreads, int maxMaps) {
    int rc;
    MDB_txn *txn;
    MDB_cursor *mc;
    MDB_val key;
    MDB_dbi dbi;

    rc = mdb_env_create(&env);
    rc = mdb_env_set_mapsize(env, maxSize);
    numThreads += 4;
    if (numThreads > 126)
        rc = mdb_env_set_maxreaders(env, numThreads);
    rc = mdb_env_set_maxdbs(env, maxMaps);
    rc = mdb_env_open(env, directoryName.c_str(), MDB_WRITEMAP|MDB_MAPASYNC | (syncmode ? MDB_NOMETASYNC:MDB_NOSYNC), 0664);
    if (rc) {
        fprintf(stderr, "env_open returned %s\n", mdb_strerror(rc));
        return;
    }
    rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    rc = mdb_open(txn, NULL, 0, &dbi);
    /* Open all maps */
    rc = mdb_cursor_open(txn, dbi, &mc);
    while ((rc = mdb_cursor_get(mc, &key, NULL, MDB_NEXT)) == 0) {
        char *str = (char *)malloc(key.mv_size+1);
        memcpy(str, key.mv_data, key.mv_size);
        str[key.mv_size] = '\0';
        rc = mdb_open(txn, str, 0, &dbi);
        free(str);
    }
    mdb_cursor_close(mc);
    mdb_txn_commit(txn);
    }

    ~LmdbServer() {
    mdb_env_close(env);
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
    MDB_txn *txn;
    MDB_dbi dbi;
    int rc, exist = 0;
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, mapName.c_str(), 0, &dbi);
    if (rc)
        rc = mdb_open(txn, mapName.c_str(), MDB_CREATE, &dbi);
    else
        exist = 1;
    rc = mdb_txn_commit(txn);
        return exist ? ResponseCode::MapExists : ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
    MDB_txn *txn;
    MDB_dbi dbi;
    int rc, found = 0;
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, mapName.c_str(), 0, &dbi);
    if (!rc) {
        rc = mdb_drop(txn, dbi, 0);
        found = 1;
    }
    rc = mdb_txn_commit(txn);
        return found ? ResponseCode::Success : ResponseCode::MapNotFound;
    }

    void listMaps(StringListResponse& _return) {
    MDB_txn *txn;
    MDB_cursor *mc;
    MDB_dbi dbi;
    MDB_val key;
    std::string str;
    int rc;
    rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    rc = mdb_open(txn, NULL, 0, &dbi);
    rc = mdb_cursor_open(txn, dbi, &mc);
    while ((rc = mdb_cursor_get(mc, &key, NULL, MDB_NEXT)) == 0) {
        str.assign((char *)key.mv_data, key.mv_size);
        _return.values.push_back(str);
    }
    mdb_cursor_close(mc);
    mdb_txn_abort(txn);
        _return.responseCode = ResponseCode::Success;
    }

    void scan(RecordListResponse& _return, const std::string& mapName,
              const ScanOrder::type order, const std::string& startKey,
              const bool startKeyIncluded, const std::string& endKey,
              const bool endKeyIncluded, const int32_t maxRecords,
              const int32_t maxBytes) {
    MDB_txn *txn;
    MDB_cursor *mc;
    MDB_dbi dbi;
    MDB_val key, data, k2;
    Record rec;
    int rc = 0, scanbeg = 0;
    MDB_cursor_op dflag;
    int32_t resultSize = 0;
    int32_t count = 0;

    rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    rc = mdb_open(txn, mapName.c_str(), 0, &dbi);
    if (rc) {
        if (rc == MDB_NOTFOUND)
            _return.responseCode = ResponseCode::MapNotFound;
        else
            _return.responseCode = ResponseCode::Error;
        return;
    }
    rc = mdb_cursor_open(txn, dbi, &mc);
    if (order == ScanOrder::Ascending) {
        dflag = MDB_NEXT;
        if (!startKey.empty()) {
            MDB_val k2;
            key.mv_data = (void *)startKey.data();
            key.mv_size = startKey.size();
            k2 = key;
            rc = mdb_cursor_get(mc, &key, &data, MDB_SET_RANGE);
            if (!rc) {
                scanbeg = 1;
                rc = mdb_cmp(txn, dbi, &key, &k2);
                if (rc || startKeyIncluded) {
                    dflag = MDB_GET_CURRENT;
                    rc = 0;
                }
            }
        }
        k2.mv_data = (void *)endKey.data();
        k2.mv_size = endKey.size();
    } else {
        dflag = MDB_PREV;
        if (!endKey.empty()) {
            MDB_val k2;
            key.mv_data = (void *)endKey.data();
            key.mv_size = endKey.size();
            k2 = key;
            rc = mdb_cursor_get(mc, &key, &data, MDB_SET_RANGE);
            if (!rc) {
                scanbeg = 1;
                rc = mdb_cmp(txn, dbi, &key, &k2);
                if (rc || endKeyIncluded) {
                    dflag = MDB_GET_CURRENT;
                    rc = 0;
                }
            }
        }
        k2.mv_data = (void *)startKey.data();
        k2.mv_size = startKey.size();
    }
    if (rc) {
        if (rc == MDB_NOTFOUND)
            _return.responseCode = ResponseCode::RecordNotFound;
        else
            _return.responseCode = ResponseCode::Error;
        mdb_cursor_close(mc);
        mdb_txn_abort(txn);
        return;
    }
    while ((rc = mdb_cursor_get(mc, &key, &data, dflag)) == 0) {
        scanbeg = 1;
        if (k2.mv_size) {
            rc = mdb_cmp(txn, dbi, &key, &k2);
            if (order == ScanOrder::Ascending) {
                if (!rc && !endKeyIncluded)
                    break;
                if (rc > 0)
                    break;
            } else {
                if (!rc && !startKeyIncluded)
                    break;
                if (rc < 0)
                    break;
            }
        }
        if ((int)key.mv_size + (int)data.mv_size + resultSize > maxBytes)
            break;
        rec.key.assign((char *)key.mv_data, key.mv_size);
        rec.value.assign((char *)data.mv_data, data.mv_size);
        _return.records.push_back(rec);
        resultSize += key.mv_size + data.mv_size;
        count++;
        if (count >= maxRecords)
            break;
        if (dflag == MDB_GET_CURRENT)
            dflag = (order == ScanOrder::Ascending) ? MDB_NEXT : MDB_PREV;
    }
        if (rc == MDB_NOTFOUND) {
            if (scanbeg)
                _return.responseCode = ResponseCode::ScanEnded;
            else
                _return.responseCode = ResponseCode::RecordNotFound;
        } else if (rc) {
            _return.responseCode = ResponseCode::Error;
        } else {
            _return.responseCode = ResponseCode::ScanEnded;
        }
        mdb_cursor_close(mc);
        mdb_txn_abort(txn);
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
    MDB_txn *txn;
    MDB_val k, data;
    MDB_dbi dbi;
    int rc;

    k.mv_data = (void *)key.data();
    k.mv_size = key.size();
    rc = mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    rc = mdb_open(txn, mapName.c_str(), 0, &dbi);
    if (rc) {
        _return.responseCode = ResponseCode::MapNotFound;
    } else {
        rc = mdb_get(txn, dbi, &k, &data);
        if (!rc) {
            _return.value.assign((char *)data.mv_data, data.mv_size);
            _return.responseCode = ResponseCode::Success;
        } else if (rc == MDB_NOTFOUND) {
            _return.responseCode = ResponseCode::RecordNotFound;
        } else {
            _return.responseCode = ResponseCode::Error;
        }
    }
    mdb_txn_abort(txn);
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
    MDB_txn *txn;
    MDB_val k, data;
    MDB_dbi dbi;
    int rc;
    ResponseCode::type rv;

    k.mv_data = (void *)key.data();
    k.mv_size = key.size();
    data.mv_data = (void *)value.data();
    data.mv_size = value.size();
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, mapName.c_str(), 0, &dbi);
    if (rc) {
        rv = ResponseCode::MapNotFound;
    } else {
        rc = mdb_put(txn, dbi, &k, &data, 0);
        if (!rc) {
            rc = mdb_txn_commit(txn);
            txn = NULL;
            rv = ResponseCode::Success;
        }
        if (rc)
            rv = ResponseCode::Error;
    }
    if (txn)
        mdb_txn_abort(txn);
        return rv;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
    MDB_txn *txn;
    MDB_val k, data;
    MDB_dbi dbi;
    int rc;
    ResponseCode::type rv;

    k.mv_data = (void *)key.data();
    k.mv_size = key.size();
    data.mv_data = (void *)value.data();
    data.mv_size = value.size();
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, mapName.c_str(), 0, &dbi);
    if (rc) {
        rv = ResponseCode::MapNotFound;
    } else {
        rc = mdb_put(txn, dbi, &k, &data, MDB_NOOVERWRITE);
        if (!rc) {
            rc = mdb_txn_commit(txn);
            txn = NULL;
            rv = ResponseCode::Success;
        }
        if (rc) {
            if (rc == MDB_KEYEXIST)
                rv = ResponseCode::RecordExists;
            else
                rv = ResponseCode::Error;
        }
    }
    if (txn)
        mdb_txn_abort(txn);
        return rv;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
    MDB_txn *txn;
    MDB_cursor *mc;
    MDB_val k, data;
    MDB_dbi dbi;
    int rc;
    ResponseCode::type rv;

    k.mv_data = (void *)key.data();
    k.mv_size = key.size();
    data.mv_data = (void *)value.data();
    data.mv_size = value.size();
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, mapName.c_str(), 0, &dbi);
    if (rc) {
        rv = ResponseCode::MapNotFound;
    } else {
        rv = ResponseCode::Success;
        rc = mdb_cursor_open(txn, dbi, &mc);
        if (!blindupdate) {
            rc = mdb_cursor_get(mc, &k, NULL, MDB_SET);
            if (rc == MDB_NOTFOUND)
                rv = ResponseCode::RecordNotFound;
        }
        if (rv == ResponseCode::Success) {
            rc = mdb_cursor_put(mc, &k, &data, 0);
            if (!rc) {
                mdb_cursor_close(mc);
                mc = NULL;
                rc = mdb_txn_commit(txn);
                txn = NULL;
                rv = ResponseCode::Success;
            }
            if (rc)
                rv = ResponseCode::Error;
        }
        if (mc)
            mdb_cursor_close(mc);
    }
    if (txn)
        mdb_txn_abort(txn);
        return rv;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
    MDB_txn *txn;
    MDB_val k;
    MDB_dbi dbi;
    int rc;
    ResponseCode::type rv = ResponseCode::Success;

    k.mv_data = (void *)key.data();
    k.mv_size = key.size();
    rc = mdb_txn_begin(env, NULL, 0, &txn);
    rc = mdb_open(txn, mapName.c_str(), 0, &dbi);
    if (rc) {
        rv = ResponseCode::MapNotFound;
    } else {
        rc = mdb_del(txn, dbi, &k, NULL);
        if (!rc) {
            rc = mdb_txn_commit(txn);
            txn = NULL;
        }
        if (rc == MDB_NOTFOUND)
            rv = ResponseCode::RecordNotFound;
        else if (rc)
            rv = ResponseCode::Error;
    }
        return rv;
    }

private:
    MDB_env *env;
};

void usage(char* programName) {
    fprintf(stderr, "%s [nonblocking|threaded|threadpool]\n", programName);
    exit(1);
}

int main(int argc, char **argv) {
    int port;
    size_t maxSizeMb;
    size_t numThreads;
    int maxMaps;
    std::string dir;
    po::variables_map vm;
    po::options_description config("");
    config.add_options()
        ("help,h", "produce help message")
        ("sync,s", "synchronous writes")
        ("blindupdate,u",  "skip record existence check for updates")
        ("port,p", po::value<int>(&port)->default_value(9090), "port to listen to")
        ("datadir,d", po::value<std::string>(&dir)->default_value("data"), "data directory")
        ("maxsize-mb,m", po::value<size_t>(&maxSizeMb)->default_value(1024), "LMDB max size in MB")
        ("maps,q", po::value<int>(&maxMaps)->default_value(256), "LMDB max maps")
        ("threads,t", po::value<size_t>(&numThreads)->default_value(32), "Number of threads")
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
    blindupdate = vm.count("blindupdate");
    maxSizeMb *= 1048576;
    shared_ptr<LmdbServer> handler(new LmdbServer(dir, maxSizeMb, numThreads, maxMaps));
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(numThreads);
    shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
    TThreadPoolServer server (processor, serverTransport, transportFactory, protocolFactory, threadManager);
//    TThreadedServer server (processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
