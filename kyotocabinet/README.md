## Dependencies

### Kyoto Cabinet

    wget http://fallabs.com/kyotocabinet/pkg/kyotocabinet-1.2.74.tar.gz
    tar xfvz kyotocabinet-1.2.74.tar.gz
    cd kyotocabinet-1.2.74
    ./configure --prefix=/usr/local
    make && sudo make install 

## Running Kyoto Cabinet MapKeeper Server

After installing all the dependencies, run:

    make

to compile. To start the server, execute the command:

    ./mapkeeper_kyotocabinet

You can see the complete list of available options by passing `-h` to `mapkeeper_kyotocabinet` 
command.
 
## Configuration Parameters

### `--sync | -s`

By default, databases are opened in "transaction" mode, which means writes are 
pushed to the OS, but it doesn't wait for writes to get persisted to disk. Use
this flag to turn on synchronous write.

See <http://fallabs.com/kyotocabinet/spex.html> for details on different
durability guarantees Kyoto Cabinet provides. 

### `--mmap-size-mb | -m`

Sets the size of the internal memory-mapped region in MB. 

## Related Pages

* [Official Kyoto Cabinet Site](http://fallabs.com/kyotocabinet/)
