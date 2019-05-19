include ../Makefile.config

all:
	$(THRIFT_DIR)/bin/thrift --gen cpp mapkeeper.thrift
	make -C gen-cpp
	cd gen-java && mvn clean package

clean:
	make -C gen-cpp clean
	cd gen-java && mvn clean

install:
	make install -C gen-cpp
