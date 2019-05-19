include ../Makefile.config

EXECUTABLE = mapkeeper_kyotocabinet

all :
	g++ -Wall -o $(EXECUTABLE) *cpp -I $(THRIFT_DIR)/include/thrift -I $(THRIFT_DIR)/include \
        -lboost_thread -lboost_filesystem -lboost_program_options -lthrift -I ../thrift/gen-cpp \
	-L $(THRIFT_DIR)/lib -l kyotocabinet -L ../thrift/gen-cpp -lmapkeeper \
        -Wl,-rpath,\$$ORIGIN/../thrift/gen-cpp -Wl,-rpath,$(THRIFT_DIR)/lib

run: all
	./$(EXECUTABLE)

clean :
	- rm -rf $(EXECUTABLE) *.o 

wipe:
	- rm -rf data/*
