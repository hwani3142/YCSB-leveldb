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
#ifndef BDB_ITERATOR_H
#define BDB_ITERATOR_H

#include "MapKeeper.h"
#include "Bdb.h"
#include "RecordBuffer.h"

class BdbIterator
{
public:
    enum ResponseCode {
        Success = 0,
        Error,
        ScanEnded,
    };

    BdbIterator();
    ~BdbIterator();

    /**
     * Initializes a scan from a given key. 
     *
     * startKey is supposed to be smaller than or equal to endKey regardless
     * of the scan order. If startKey is larger than endKey, scan result will
     * be empty.
     */
    ResponseCode init(Bdb* bdb, 
                      const std::string& startKey, bool startKeyIncluded,
                      const std::string& endKey, bool endKeyIncluded,
                      mapkeeper::ScanOrder::type order);
    ResponseCode next(RecordBuffer& buffer);

private:
    BdbIterator(const BdbIterator&);
    BdbIterator& operator=(const BdbIterator&);
    static int compareKeys(const char* a, uint32_t alen, const char* b, uint32_t blen);
    ResponseCode initAscendingScan();
    ResponseCode initDescendingScan();
    ResponseCode nextAscending(RecordBuffer& buffer, Dbt& dbkey, Dbt& dbval);
    ResponseCode nextDescending(RecordBuffer& buffer, Dbt& dbkey, Dbt& dbval);
    void initEmptyData(Dbt& data);
    bool inited_;
    bool scanEnded_;
    Bdb* bdb_;
    int32_t flags_;
    Dbc* cursor_;
    mapkeeper::ScanOrder::type order_;
    std::string startKey_;
    bool startKeyIncluded_;
    std::string endKey_;
    bool endKeyIncluded_;
};

#endif /* BDB_ITERATOR_H */
