#
# $ make run mode=threaded      # run TThreadedServer
# $ make run mode=threadpool    # run TThreadPoolServer
# $ make run mode=nonblocking   # run TNonblockingServer
#
EXECUTABLE = mapkeeper_lmdb

all :
	g++ -Wall -DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -O2 \
	-o $(EXECUTABLE) *cpp -I /usr/local/include/thrift \
	-L/usr/local/lib -lthrift -lthriftnb \
        -I ../thrift/gen-cpp -L../thrift/gen-cpp -lmapkeeper -levent -llmdb -lboost_program_options

thrift:
	make -C ../thrift
run : 
	LD_LIBRARY_PATH=/usr/local/lib:../thrift/gen-cpp ./$(EXECUTABLE) $(mode)
clean :
	- rm $(EXECUTABLE) *o 
