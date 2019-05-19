include ../../Makefile.config

CC=g++
CFLAGS=-c -Wall -I $(HEADER_DIR)/thrift -I $(HEADER_DIR)
LDFLAGS = -L$(LIB_DIR) -lthrift
SOURCES = mapkeeper_constants.cpp \
          MapKeeper.cpp \
          mapkeeper_types.cpp \

OBJECTS=$(SOURCES:.cpp=.o)
LIB = libmapkeeper.so
A = libmapkeeper.a

all: $(SOURCES) $(LIB) $(A)

$(LIB): $(OBJECTS) 
	$(CC) -shared -Wl,-soname,$@ -o $@ $^
$(A): $(OBJECTS)
	cp $(LIB_DIR)/libthrift.a $@
	ar rs $@ $^

%.o : %.cpp
	$(CC) $(CFLAGS) -fPIC -c -o $@ $^
#.cpp.o:

install:
	mkdir -p $(HEADER_DIR)/mapkeeper
	cp *.h $(HEADER_DIR)/mapkeeper
	cp $(A) $(LIB_DIR)

clean:
	rm -rf *[cpp,h,o]
