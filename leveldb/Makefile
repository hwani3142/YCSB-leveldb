include ../Makefile.config

TARGETS = mapkeeper_leveldb

CXX = g++
SRC = LevelDbServer.cpp
OBJ = $(SRC:.cpp=.o)
CXXFLAGS =  -Wall -g -O2
INCPATH = -I. -I/usr/local/include -I/usr/local/include/thrift -I/usr/local/include/leveldb -I/usr/local/include/mapkeeper -std=c++11
LIBS = -L. -L/usr/local/lib -L/usr/local/lib64 -lmapkeeper -lboost_thread -lboost_system -lboost_filesystem -lboost_program_options -lthrift -lsnappy -lleveldb -lpthread


# Build rule
all : $(TARGETS)
	$(CXX) $(CXXFLAGS) -o $(TARGETS) $(OBJ) $(LIBS)

$(TARGETS) : 
	$(CXX) $(CXXFLAGS) -c $(SRC) $(INCPATH)

#%.o: %.cpp
#	$(CXX) -c $< -o $@ $(CFLAGS)

#$(TARGETS): $(OBJ)
#	$(CXX) -o $@ $< $(LIBS)


run:
	./$(EXECUTABLE) --sync

clean :
	- rm -rf $(THRIFT_SRC) $(TARGETS) *.o 

wipe:
	- rm -rf data/*
