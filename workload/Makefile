CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -pthread -I..

COMMON_SRC = ../common/network.cpp
ABD_CLIENT_SRC = ../abd/abd_client.cpp
BLOCKING_CLIENT_SRC = ../blocking/blocking_client.cpp
WORKLOAD_SRC = workload_generator.cpp

all: workload

workload: $(WORKLOAD_SRC) $(ABD_CLIENT_SRC) $(BLOCKING_CLIENT_SRC) $(COMMON_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f workload