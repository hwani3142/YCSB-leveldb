## Dependencies

### OpenLDAP LMDB

    git clone git://gitorious.org/mdb/mdb.git
    cd mdb/libraries/liblmdb
    make
    sudo make install

## Running

To start the server, execute the command:

    ./mapkeeper_lmdb

You can see the complete list of available options by passing `-h` to `mapkeeper_lmdb`
command.

## Configuration Parameters

### `--sync | -s`

By default, databases are opened in NOSYNC mode, which means writes are
pushed to the OS, but it doesn't wait for writes to get persisted to disk. Use
this flag to turn on synchronous write.

See <http://symas.com/mdb/doc/> for details on LMDB.

### `--blindupdate | -u`
    
By default, MapKeeper server checks for the key's existence before applying update.
Use this option to bypass the record existence check.

### `--maxsize-mb | -m`

Set the maximum size the database is allowed to occupy. If in doubt, just set
it to the amount of remaining free space on the filesystem.

### `--maxmaps | -q`

Set the maximum number of named maps that are allowed.

## Related Pages

* [Official LMDB Site](http://symas.com/mdb/)

