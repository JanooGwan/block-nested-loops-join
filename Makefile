CXX = g++
CXXFLAGS = -O2 -std=c++17 -Wall -Wextra -pthread

SRCS_APP = main.cpp io.cpp join_simple.cpp join_block.cpp join_block_old.cpp
OBJS_APP = $(SRCS_APP:%.cpp=build/%.o)

all: prep app tbl2dat

prep:
	mkdir -p build output

build/%.o: %.cpp record.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

app: $(OBJS_APP)
	$(CXX) $(CXXFLAGS) $(OBJS_APP) -o build/app

tbl2dat: tbl_to_dat.cpp
	$(CXX) $(CXXFLAGS) tbl_to_dat.cpp -o build/tbl_to_dat

clean:
	rm -rf build output/result.tbl
