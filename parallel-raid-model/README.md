Simulator of parallel dispatch into RAID

To compile run `make`, it will produce sim binary.
Usage:
    sim <duration seconds> <raid nr_disks> <raid chunk_size> <disk rps> <disk queues> <fs extent_size> <cpu nr> <cpu parallelism> <cpu request_size>

duration -- total time to run simulation for (virtual time, real time is usually much shorter)
raid nr_disks -- number of disks to put into raid
raid chunk_size -- chunk size or striped raid
disk rps -- requests per (virtual) second each disk can process
disk queues -- number of internal queues in the disk. Each queue runs at rps/nr_queues rate
fs extent_size -- size of extent a CPU "reserves" for its IO atomically
cpu nr -- number of CPUs
cpu parallelism -- number of requests each CPU sumbits at the same time
cpu request_size -- size of each CPU request
