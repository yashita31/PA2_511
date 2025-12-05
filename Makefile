CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -pthread

.PHONY: all clean abd blocking workload

all: abd blocking workload

abd:
	$(MAKE) -C abd

blocking:
	$(MAKE) -C blocking

workload:
	$(MAKE) -C workload

clean:
	$(MAKE) -C abd clean
	$(MAKE) -C blocking clean
	$(MAKE) -C workload clean