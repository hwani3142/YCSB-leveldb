# Requirements:

## cmake

## mysql-connector

    wget http://dev.mysql.com/get/Downloads/Connector-C/mysql-connector-c-6.0.2.tar.gz/from/http://mysql.he.net/
    tar xfvz mysql-connector-c-6.0.2.tar.gz
    cd mysql-connector-c-6.0.2
    cmake -G "Unix Makefiles"
    make
    sudo make install

## MySQL

You need to have MySQL server running on localhost 3306 with password-less root. 

# Running the MapKeeper MySQL server

To start the server, run:

    make run
