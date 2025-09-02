SOURCE = simulate.cc

all: sim sim-v

sim: $(SOURCE)
	clang++ -std=c++20 -O2 $< -lfmt -o $@

sim-v: $(SOURCE)
	clang++ -std=c++20 -O2 -DVERB=1 $< -lfmt -o $@
