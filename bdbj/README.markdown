## Configuration 

There are 2 configuration files, `bdb_java_server.ini` and `data/je.properties`.

- `bdb_java_server.ini` contains MapKeeper related configuration parameters.
    - `env_dir` specifies the data directory (default: `data`).
    - `port` specifies the port MapKeeper server listens to (default: `9090`).
    - `num_threads` specifies the number of worker threads (default: `32`).

- `data/je.properties` contains Berkeley DB related configuration parameters.
  See http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/EnvironmentConfig.html
  for the list of all the parameters.

  Note that `je.properties` must be placed in the Berkeley DB data directory. So 
  if you change `env_dir` in `bdb_java_server.ini`, you need to copy `je.properties`
  under `env_dir`. 

## Start the server

To start the server, run:

    make run

This will start the server with 1GB Java heap. If you like to change the heap
size, you can pass `heapsize` parameter to `make`. For example, to use 4GB for
Java heap, do:

    make run heapsize=4g
