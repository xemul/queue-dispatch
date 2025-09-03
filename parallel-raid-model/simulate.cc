#include <fmt/core.h>
#include <fmt/ranges.h>
#include <chrono>
#include <deque>
#include <vector>
#include <ranges>
#include <cassert>

#ifndef VERB
#define VERB false
#endif

using namespace std::chrono;

struct request {
    bool _completed = false;
    const uint64_t _offset;
    const unsigned _cpu_id;
    duration<double> _start;
    duration<double> _stop;

public:
    request(duration<double> now, uint64_t off, unsigned cpu) noexcept
            : _offset(off)
            , _cpu_id(cpu)
            , _start(now)
    {}

    void complete(duration<double> now) {
        _completed = true;
        _stop = now;
    }

    bool completed() const noexcept { return _completed; }
    uint64_t offset() const noexcept { return _offset; }
    unsigned cpu() const noexcept { return _cpu_id; }

    duration<double> latency() const noexcept { return _stop - _start; }
};

class disk {
    const unsigned _id;
    std::deque<request*> _queue;
    const duration<double> _lat;
    duration<double> _next;
    uint64_t _requests_processed = 0;

public:
    disk(uint64_t rps, unsigned id)
            : _id(id)
            , _lat(1.0 / rps)
    {
    }

    void make_request(request* rq, duration<double> now) {
        if (_queue.empty()) {
            _next = now + _lat;
        }
        _queue.push_back(rq);
    }

    void tick(duration<double> now) {
        while (!_queue.empty() && now >= _next) {
            request* r = _queue.front();
            _queue.pop_front();
            r->complete(now);
            _requests_processed++;
            _next += _lat;
        }
    }

    uint64_t requests_processed() const noexcept { return _requests_processed; }
};

class raid {
    std::vector<disk> _disks;
    const uint64_t _chunk_size;

public:
    raid(unsigned nr_disks, uint64_t cs, uint64_t rps)
            : _chunk_size(cs)
    {
        _disks.reserve(nr_disks);
        for (unsigned i = 0; i < nr_disks; i++) {
            _disks.emplace_back(rps, i);
        }
    }

    void make_request(request* rq, duration<double> now) {
        unsigned disk = (rq->offset() / _chunk_size) % _disks.size();
        _disks[disk].make_request(rq, now);
    }

    void tick(duration<double> now) {
        for (auto& d : _disks) {
            d.tick(now);
        }
    }

    const std::vector<disk>& disks() const noexcept { return _disks; }
};

struct extent {
    uint64_t offset;
    uint64_t size;
};

class filesystem {
    raid& _raid;
    const uint64_t _extent_size;
    uint64_t _offset = 0;
    unsigned _total_extents = 0;
    std::vector<request*> _queue;

public:
    filesystem(uint64_t xs, raid& r)
            : _raid(r)
            , _extent_size(xs)
    {
    }

    void io(request* rq, duration<double> now) {
        _queue.push_back(rq);
    }

    void tick(duration<double> now) {
        for (auto& rq : _queue) {
            _raid.make_request(rq, now);
        }
        _queue.clear();
    }

    extent allocate() {
        auto ret = extent {
            .offset = _offset,
            .size = _extent_size,
        };
        _offset += _extent_size;
        _total_extents++;
        return ret;
    }

    unsigned total_extents() const noexcept { return _total_extents; }
};

class cpu {
    const unsigned _id;
    filesystem& _fs;
    std::vector<std::optional<request>> _requests;
    const uint64_t _request_size;
    extent _cur;
    uint64_t _processed_requests = 0;
    duration<double> _total_exec_lat = duration<double>(0.0);

public:
    cpu(unsigned id, unsigned parallelism, uint64_t rs, filesystem& fs)
            : _id(id)
            , _fs(fs)
            , _request_size(rs)
    {
        _requests.resize(parallelism);
        _cur = _fs.allocate();
    }

    void tick(duration<double> now) {
        for (auto& rq : _requests) {
            if (rq.has_value() && rq->completed()) {
                _processed_requests++;
                _total_exec_lat += rq->latency();
                rq.reset();
            }
        }
        for (auto& rq : _requests) {
            if (!rq.has_value()) {
                rq.emplace(now, _cur.offset, _id);
                _fs.io(&*rq, now);
                _cur.offset += _request_size;
                _cur.size -= _request_size;
                if (_cur.size < _request_size) {
                    _cur = _fs.allocate();
                }
            }
        }
    }

    uint64_t processed_requests() const noexcept { return _processed_requests; }
    duration<double> total_exec_latency() const noexcept { return _total_exec_lat; }
};

int main (int argc, char **argv)
{
    if (argc < 9) {
        fmt::print("usage: {} <duration seconds> <raid nr_disks> <raid chunk_size> <disk rps> <fs extent_size> <cpu nr> <cpu parallelism> <cpu request_size>\n", argv[0]);
        return 1;
    }

    unsigned long total_sec = atoi(argv[1]);

    unsigned long nr_disks = atoi(argv[2]);
    unsigned long chunk_size = atoi(argv[3]);
    unsigned long disk_rps = atoi(argv[4]);
    fmt::print("RAID: {} disks, {} chunk_size\n", nr_disks, chunk_size);
    fmt::print("DISK: {} rps\n", disk_rps);
    raid r(nr_disks, chunk_size, disk_rps);

    unsigned long extent_size = atoi(argv[5]);
    fmt::print("FS: {} extent\n", extent_size);
    filesystem fs(extent_size, r);

    unsigned long cpu_nr = atoi(argv[6]);
    unsigned long cpu_parallelism = atoi(argv[7]);
    unsigned long cpu_req_size = atoi(argv[8]);
    fmt::print("CPU: {}, {} parallelism, {} req_size\n", cpu_nr, cpu_parallelism, cpu_req_size);
    std::vector<cpu> cpus;
    cpus.reserve(cpu_nr);
    for (unsigned i = 0; i < cpu_nr; i++) {
        cpus.emplace_back(i, cpu_parallelism, cpu_req_size, fs);
    }

    duration<double> now(0.0);
    while (now <= seconds(total_sec)) {
        for (auto& c : cpus) {
            c.tick(now);
        }
        fs.tick(now);
        r.tick(now);
        now += microseconds(1);
    }

    uint64_t processed = 0;
    for (auto& c : cpus) {
        processed += c.processed_requests();
    }
    fmt::print("Processed {} requests (expected {}, result: {}%)\n", processed, nr_disks * disk_rps * total_sec, int(processed * 100 / (nr_disks * disk_rps * total_sec)));
    fmt::print("CPUs requests processed: {}\n", cpus | std::views::transform(&cpu::processed_requests));
    fmt::print("CPUs average request latency: {} us\n", cpus | std::views::transform([] (auto& cpu) {
        return std::chrono::duration_cast<std::chrono::microseconds>(cpu.total_exec_latency() / cpu.processed_requests()).count();
    }));
    fmt::print("Disks requests processed: {}\n", r.disks() | std::views::transform(&disk::requests_processed));
    fmt::print("Total {} extents allocated\n", fs.total_extents());
    return 0;
}
