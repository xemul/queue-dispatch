Produce->Dispatch->Consome simulator.

To compile run `make`, it will produce two binaries: sim and sim-v. The latter one
is verbose and prints its progress while it runs.

Usage is `sim <producer process> <producer rate> <dispatcher process> <consumer process> <consumer rate>`
where each process can be one of uniform, poisson, expdelay or capdelay.
