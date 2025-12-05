CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -pthread -I..

COMMON_SRC = ../common/network.cpp
BLOCKING_CLIENT_SRC = blocking_client.cpp
BLOCKING_SERVER_SRC = blocking_server.cpp

all: blocking_server

blocking_server: $(BLOCKING_SERVER_SRC) $(BLOCKING_CLIENT_SRC) $(COMMON_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f blocking_server