SOURCE = simulate.cc

all: uniform poisson expdelay

uniform: $(SOURCE)
	clang++ -std=c++20 -O2 -lfmt -DPROCESS=uniform $< -o $@

poisson: $(SOURCE)
	clang++ -std=c++20 -O2 -lfmt -DPROCESS=poisson $< -o $@

expdelay: $(SOURCE)
	clang++ -std=c++20 -O2 -lfmt -DPROCESS=exp_delay $< -o $@
