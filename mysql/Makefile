include ../Makefile.config

EXECUTABLE = mapkeeper_mysql

all :
	g++ -Wall -o $(EXECUTABLE) *cpp -I $(THRIFT_DIR)/include/thrift -I $(THRIFT_DIR)/include -L$(THRIFT_DIR)/lib \
        -I /usr/local/mysql/include -I /usr/include/mysql -lboost_thread -lthrift \
	-L/usr/local/mysql/lib -lmysqlclient -I ../thrift/gen-cpp -L../thrift/gen-cpp -lmapkeeper \
	-Wl,-rpath,\$$ORIGIN/../thrift/gen-cpp -Wl,-rpath,$(THRIFT_DIR)/lib

run: all
	LD_LIBRARY_PATH=/usr/local/lib:/usr/local/mysql/lib/:../thrift/gen-cpp/ ./$(EXECUTABLE)

clean :
	- rm -rf $(EXECUTABLE) *.o 

wipe :
	- mysql -uroot -e "drop database if exists mapkeeper"
