
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

CFLAGS += -Wall -Ivendor/leveldb/include -std=c99
CXXFLAGS += -Wall -Ivendor/leveldb/include
LDFLAGS += vendor/libleveldb.a -lsnappy -lm -lev -lprotobuf -lsqlite3

ifeq ($(uname_S),Linux)
  LDFLAGS += -lpthread
endif

ifneq ($(NDEBUG),1)
	CXXFLAGS += -g -DDEBUG
endif

all: harq

SRC=$(sort $(wildcard src/*.cpp))
OBJ=$(patsubst %.cpp,%.o,$(SRC))

harq: vendor/libleveldb.a $(OBJ) 
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $(OBJ)

rebuild_pb:
	protoc -Isrc --cpp_out=src src/wire.proto
	mv src/wire.pb.cc src/wire.pb.cpp

clean:
	-rm harq
	-rm src/*.o

distclean: clean
	-rm vendor/*.a
	cd vendor/leveldb; make clean

vendor/libleveldb.a:
	cd vendor/leveldb; make && cp libleveldb.a ..

dep:
	: > depend
	for i in $(SRC); do $(CC) $(CXXFLAGS) -MM -MT $${i%.cpp}.o $$i >> depend; done

.PHONY: clean distclean dep

-include depend
