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
#ifndef RECORD_BUFFER_H
#define RECORD_BUFFER_H

#include <stdint.h>
#include <boost/scoped_array.hpp>

class RecordBuffer {
public:
    RecordBuffer(uint32_t keyBufferSize, uint32_t valueBufferSize);
    char* getKeyBuffer() const;
    char* getValueBuffer() const;
    uint32_t getKeyBufferSize() const;
    uint32_t getValueBufferSize() const;
    uint32_t getKeySize() const;
    uint32_t getValueSize() const;
    void setKeySize(uint32_t keySize);
    void setValueSize(uint32_t valueSize);

private:
    RecordBuffer(const RecordBuffer&);
    RecordBuffer& operator=(const RecordBuffer&);

    boost::scoped_array<char> keyBuffer_;
    boost::scoped_array<char> valueBuffer_;
    uint32_t keyBufferSize_;
    uint32_t valueBufferSize_;
    uint32_t keySize_;
    uint32_t valueSize_;
};

#endif // RECORD_BUFFER_H
