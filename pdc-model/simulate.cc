#include <fmt/core.h>
#include <fmt/ranges.h>
#include <fmt/chrono.h>
#include <random>
#include <chrono>
#include <list>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/p_square_quantile.hpp>
#include <boost/accumulators/statistics/extended_p_square.hpp>
#include <boost/accumulators/statistics/extended_p_square_quantile.hpp>

#ifndef VERB
#define VERB false
#endif

#ifndef CAP_FACTOR
#define CAP_FACTOR (3.0)
#endif

using namespace std::chrono;
using namespace boost::accumulators;

class process {
public:
    virtual duration<double> get() = 0;
    virtual ~process() = default;
};

class poisson_process : public process {
    std::random_device _rd;
    std::mt19937 _rng;
    std::exponential_distribution<double> _exp;

public:
    poisson_process(duration<double> period)
            : _rng(_rd())
            , _exp(1.0 / period.count())
    {
    }

    virtual duration<double> get() override {
        return duration<double>(_exp(_rng));
    }
};

class exp_delay_process : public process {
    duration<double> _lat;
    std::random_device _rd;
    std::mt19937 _rng;
    std::exponential_distribution<double> _exp;

public:
    exp_delay_process(duration<double> period)
            : _lat(period)
            , _exp(1.0)
    {
    }

    virtual duration<double> get() override {
        return _lat * (1.0 + _exp(_rng));
    }
};

class uniform_process : public process {
    duration<double> _lat;

public:
    uniform_process(duration<double> period)
            : _lat(period)
    {
    }

    virtual duration<double> get() override {
        return _lat;
    }
};

class cap_delay_process : public process {
    duration<double> _lat;
    std::random_device _rd;
    std::mt19937 _rng;
    std::uniform_real_distribution<double> _jit;

public:
    cap_delay_process(duration<double> period)
            : _lat(period)
            , _jit(1.0, CAP_FACTOR)
    {
    }

    virtual duration<double> get() override {
        return _lat * _jit(_rng);
    }
};

static std::unique_ptr<process> make_process(std::string proc, duration<double> lat) {
    if (proc == "uniform") {
        return std::make_unique<uniform_process>(lat);
    }
    if (proc == "poisson") {
        return std::make_unique<poisson_process>(lat);
    }
    if (proc == "expdelay") {
        return std::make_unique<exp_delay_process>(lat);
    }
    if (proc == "capdelay") {
        return std::make_unique<cap_delay_process>(lat);
    }

    throw std::runtime_error(fmt::format("unknown process {}", proc));
}

struct request {
    const duration<double> start;
    duration<double> dispatch;
    request(duration<double> now) : start(now), dispatch(0) { }
};

class collector {
    static constexpr std::array<double, 3> quantiles = { 0.5, 0.95, 0.99 };
    using accumulator_type = accumulator_set<double, stats<tag::extended_p_square_quantile(quadratic), tag::mean, tag::max>>;
    accumulator_type _latencies;
    accumulator_type _x_latencies;

public:
    collector()
            : _latencies(extended_p_square_probabilities = quantiles)
            , _x_latencies(extended_p_square_probabilities = quantiles)
    {
    }

    void collect(duration<double> lat, duration<double> xlat) {
        _latencies(lat.count());
        _x_latencies(xlat.count());
    }

    duration<double> max_lat() const noexcept {
        return duration<double>(max(_latencies));
    }

    duration<double> mean_lat() const noexcept {
        return duration<double>(mean(_latencies));
    }

    duration<double> p95_lat() const noexcept {
        return duration<double>(quantile(_latencies, quantile_probability = 0.95));
    }

    duration<double> p99_lat() const noexcept {
        return duration<double>(quantile(_latencies, quantile_probability = 0.99));
    }

    duration<double> max_xlat() const noexcept {
        return duration<double>(max(_x_latencies));
    }

    duration<double> mean_xlat() const noexcept {
        return duration<double>(mean(_x_latencies));
    }

    duration<double> p95_xlat() const noexcept {
        return duration<double>(quantile(_x_latencies, quantile_probability = 0.95));
    }

    duration<double> p99_xlat() const noexcept {
        return duration<double>(quantile(_x_latencies, quantile_probability = 0.99));
    }
};

class consumer {
    std::list<request> _executing;
    duration<double> _next;
    unsigned long _processed;
    collector& _st;
    const duration<double> _lat;
    std::unique_ptr<process> _pause;

public:
    consumer(unsigned rps, collector& st, std::string proc)
            : _processed(0)
            , _st(st)
            , _lat(1.0 / rps)
            , _pause(make_process(proc, _lat))
    {
    }

    void tick(duration<double> now) {
        while (!_executing.empty() > 0 && now >= _next) {
            _st.collect(now - _executing.front().start, now - _executing.front().dispatch);
            _executing.pop_front();
            _processed++;
            _next += _pause->get();
        }
    }

    void execute(duration<double> now, request rq) {
        if (_executing.empty()) {
            _next = now + _pause->get();
        }
        rq.dispatch = now;
        _executing.push_back(std::move(rq));
    }

    duration<double> latency() const noexcept { return _lat; }
    unsigned long executing() const noexcept { return _executing.size(); }
    unsigned long processed() const noexcept { return _processed; }
};

class dispatcher {
    std::unique_ptr<process> _pause;
    duration<double> _next;
    consumer& _cons;
    std::list<request> _queue;
    unsigned long _dispatched;
    const unsigned long _limit;

public:
    dispatcher(duration<double> lat, consumer& c, std::string proc, float goal_factor)
            : _pause(make_process(proc, lat))
            , _next(0.0)
            , _dispatched(0)
            , _cons(c)
            , _limit(lat * goal_factor / _cons.latency())
    {
#if VERB
        fmt::print("Consumer limit {} requests, goal {}ms factor {}\n", _limit, lat.count() * 1000, goal_factor);
#endif
        if (_limit == 0) {
            throw std::runtime_error("Too low consumer rate");
        }
    }

    void queue(duration<double> now) {
        _queue.emplace_back(now);
    }

    void tick(duration<double> now) {
        if (now >= _next) {
            _next += _pause->get();

            while (!_queue.empty()) {
                if (_cons.executing() >= _limit) {
                    break;
                }

                _cons.execute(now, _queue.front());
                _queue.pop_front();
                _dispatched++;
            }
        }
    }

    unsigned long queued() const noexcept { return _queue.size(); }
    unsigned long dispatched() const noexcept { return _dispatched; }
};

class producer {
    dispatcher& _disp;
    duration<double> _next;
    unsigned long _generated;
    std::unique_ptr<process> _pause;

public:
    producer(unsigned rps, dispatcher& d, std::string proc)
            : _pause(make_process(proc, duration<double>(1.0 / rps)))
            , _disp(d)
            , _next(0.0)
            , _generated(0)
    {
    }

    void tick(duration<double> now) {
        while (now >= _next) {
            _next += _pause->get();
            _disp.queue(now);
            _generated++;
        }
    }

    unsigned long generated() const noexcept { return _generated; }
};


int main (int argc, char **argv)
{
    if (argc < 7) {
        fmt::print("usage: {} <duration seconds> <producer process> <producer rate> <dispatcher process> <consumer process> <consumer rate> [<latency_goal>] [<goal_factor>]\n", argv[0]);
        return 1;
    }

    unsigned long total_sec = atoi(argv[1]);
    std::string prod_proc = argv[2];
    unsigned long prod_rate = atoi(argv[3]);
    std::string disp_proc = argv[4];
    std::string cons_proc = argv[5];
    unsigned long cons_rate = atoi(argv[6]);

    unsigned latency_goal = 500; // as in seastar
    if (argc > 7 && argv[7][0] != '-') {
        latency_goal = atoi(argv[7]);
    }
    float goal_factor = 1.5; // as in seastar
    if (argc > 8 && argv[8][0] != '-') {
        goal_factor = atof(argv[8]);
    }

    collector st;
    consumer cons(cons_rate, st, cons_proc);
    dispatcher disp(microseconds(latency_goal), cons, disp_proc, goal_factor);
    producer prod(prod_rate, disp, prod_proc);
    duration<double> _verb(0.0);
    unsigned long max_queued = 0;
    unsigned long max_executed = 0;

    duration<double> now(0.0);
    while (now <= seconds(total_sec)) {
        cons.tick(now);
        prod.tick(now);
        disp.tick(now);

        max_queued = std::max(max_queued, disp.queued());
        max_executed = std::max(max_executed, cons.executing());
        if (now >= _verb) {
#if VERB
            fmt::print("{:5}   {:10}/{:<10}   g {:<10.0f} d {:<10.0f} c {:<10.0f}\n", now,
                    disp.queued(), max_queued,
                    prod.generated() / now.count(),
                    disp.dispatched() / now.count(),
                    cons.processed() / now.count()
                    );
#endif
            _verb += seconds(1);
        }

        now += microseconds(1);
    }

    fmt::print("producer rate: {} consumer rate: {} maximum queued: {} executing: {}\n", prod_rate, cons_rate, max_queued, max_executed);
    fmt::print("total latencies: mean {:.6f}  p95 {:.6f}  p99 {:.6f}  max {:.6f}\n", st.mean_lat().count(), st.p95_lat().count(), st.p99_lat().count(), st.max_lat().count());
    fmt::print("exec latencies:  mean {:.6f}  p95 {:.6f}  p99 {:.6f}  max {:.6f}\n", st.mean_xlat().count(), st.p95_xlat().count(), st.p99_xlat().count(), st.max_xlat().count());
    return 0;
}
