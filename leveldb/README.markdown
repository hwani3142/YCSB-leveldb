## Dependencies

### Boost
  
    wget http://superb-sea2.dl.sourceforge.net/project/boost/boost/1.48.0/boost_1_48_0.tar.gz
    tar xfvz boost_1_48_0.tar.gz
    cd boost_1_48_0
    ./bootstrap.sh --prefix=/usr/local
    sudo ./b2 install 

### LevelDB

    svn checkout http://leveldb.googlecode.com/svn/trunk/ leveldb-read-only
    cd leveldb-read-only/
    make
    sudo cp libleveldb.a /usr/local/lib
    sudo cp -r include/leveldb /usr/local/include

## Running LevelDB MapKeeper Server

After installing all the dependencies, run:

    make

to compile. To start the server, execute the command:

    ./mapkeeper_leveldb

You can see the complete list of available options by passing `-h` to `mapkeeper_leveldb` 
command.
  
## Configuration Parameters

There are many tunable parameters that affects LevelDB performance.. 

### `--sync | -s`

Synchronous write is turned off by default. Use this option If you need the write
operations to be durable, 

### `--blindinsert | -i`

By default, MapKeeper server tries to read the record before writing the new record
to LevelDB, and insert fails with `ResponseCode::RecordExists` if the record already
exists. Use this option if you don't care whether the record already exists in the
database.
  
### `--blindupdate | -u`
    
Similar to inserts, MapKeeper server reads the record before applying update by default.
Use this option to bypass the record existence check.

### `--write-buffer-mb | -w`

Write buffer size in megabytes. In general, larger buffer means better performance
and longer recovery time during startup. Default to 1024MB. 

### `--block-cache-mb | -b`

Block Cache size in megabytes (default to 1024MB). Again, bigger the better.

## Related Pages

* [Official LevelDB Documentation](http://leveldb.googlecode.com/svn/trunk/doc/index.html)
* [LevelDB Tuning Tips by Basho](http://wiki.basho.com/LevelDB.html)
