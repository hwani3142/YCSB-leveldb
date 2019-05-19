# YCSB-mapkeeper for LevelDB
Restore YCSB-leveldb

## Requirements
- Java, Maven
- library snappy for LevelDB [https://github.com/google/snappy](https://github.com/google/snappy)
- library event 2.0.12 [https://libevent.org/](https://libevent.org/) 
```
wget http://monkey.org/~provos/libevent-2.0.12-stable.tar.gz
tar xfvz libevent-2.0.12-stable.tar.gz
cd libevent-2.0.12-stable
./configure --prefix=/usr/local
make && sudo make install
```

- library boost 1.60.0 [https://www.boost.org/users/history/version_1_60_0.html](https://www.boost.org/users/history/version_1_60_0.html)
```
// Get from source.tar.gz
tar xvf boost_1_60_0.tar.gz
cd boost_1_60_0
./bootstrap.sh --prefix=/usr/local
sudo ./b2 install
```
- Thrift 0.9.3 [https://thrift.apache.org/](https://thrift.apache.org/)
Before installing thrift, check basic requirements: [https://thrift.apache.org/docs/install/](https://thrift.apache.org/docs/install/)
```
wget http://archive.apache.org/dist/thrift/0.9.3/thrift-0.9.3.tar.gz
tar xvf thrift-0.9.3.tar.gz
cd thrift-0.9.3
./configure --with-boost=/usr/local
make
sudo make install
```
If cannot link boost library, edit make file (THRIFT_DIR/Makefile)
```
//BOOST_SYSTEM_LDADD = /usr/local/lib64/libboost_system.a
BOOST_SYSTEM_LDADD = /usr/local/libboost_system.a
//BOOST_TEST_LDADD = /usr/local/lib64/libboost_unit_test_framework.a
BOOST_TEST_LDADD = /usr/local/libboost_unit_test_framework.a
```
Then, build (ignore cpp/test error)
```
make clean
make
sudo make install
```

## Build Mapkeeper
1. Clone & Build mapkeeper in github repository
```
git clone ${this_repository}
cd mapkeeper/thrift
make
sudo make install
```
2. Move mapkeeper.jar to Maven repository
For example,
```
mkdir -p /home/user/.m2/repository/com/yahoo/mapkeeper/mapkeeper/1.1
cd gen-java/target 
cp mapkeeper-1.1.jar /home/user/.m2/repository/com/yahoo/mapkeeper/mapkeeper/1.1
```

## Build Mapkeeper-LevelDB client
1. Clone & Build LevelDB (or use customized LevelDB)
```
git clone https://github.com/google/leveldb.git
cd leveldb
mkdir build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
```
2. Move LevelDB library and header files to directories 
```
sudo cp libleveldb.a /usr/local/lib
cd ../include
sudo cp -r leveldb /usr/local/include/
```
3. Build mapkeeper-LevelDB client
```
cd mapkeeper/leveldb
make
```
after build, 'mapkeeper_leveldb' is created.

## Run Mapkeeper & YCSB
1. Mapkeeper
```
cd mapkeeper/leveldb
./mapkeeper_leveldb --datadir=${DATA_DIR} --write-bufer-mb=${BUFFER_SIZE} --block-cache-mb=${CACHE_SIZE}
```
Mapkeeper Options (in mapkeeper/leveldb/LevelDbServer.cpp) :  
- sync
- blindinsert
- blindupdate
- port
- datadir
- write-buffer-md
- block-cache-mb

2. YCSB
```
cd mapkeeper/ycsb/YCSB
# load
./bin/ycsb load mapkeeper -P ./workloads/workloada
# run
./bin/ycsb run mapkeeper -P ./workloads/workloada
```


## Reference
- [https://github.com/brianfrankcooper/YCSB](https://github.com/brianfrankcooper/YCSB)
- [https://github.com/baonguyen84/YCSB](https://github.com/baonguyen84/YCSB)
- [https://shingjan.me/wordpress/index.php/2017/03/29/setup-ycsb-mapkeeper-to-benchmark-leveldb/](https://shingjan.me/wordpress/index.php/2017/03/29/setup-ycsb-mapkeeper-to-benchmark-leveldb/)
