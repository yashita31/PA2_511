CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -pthread -I..

COMMON_SRC = ../common/network.cpp
ABD_CLIENT_SRC = abd_client.cpp
ABD_SERVER_SRC = abd_server.cpp

all: abd_server

abd_server: $(ABD_SERVER_SRC) $(ABD_CLIENT_SRC) $(COMMON_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f abd_server