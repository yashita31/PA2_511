#include "../common/types.h"
#include "../common/network.h"
#include "../abd/abd_client.h"
#include "../blocking/blocking_client.h"
#include <iostream>
#include <vector>
#include <thread>
#include <random>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <mutex>

using namespace std;

// Function pointer types for protocol abstraction
typedef bool (*GetFunc)(const string&, int, const vector<ServerInfo>&, string&);
typedef bool (*PutFunc)(const string&, const string&, int, const vector<ServerInfo>&);

struct WorkerParams {
    int client_id;
    int ops;
    double get_fraction;
    int num_keys;
    const vector<ServerInfo>*servers;
    
    GetFunc get_func;
    PutFunc put_func;
    
    atomic<long long> *succ_get;
    atomic<long long> *succ_put;
    atomic<long long> *fail;
    
    vector<double> *get_latencies;
    vector<double> *put_latencies;
    mutex *lat_lock;
};


// Used some c++ libraries: random_device and mt19937
// to generate random distributions for get and put
void worker_func(WorkerParams p) {
    random_device rd;
    mt19937 rng(rd() ^ (static_cast<unsigned long long>(p.client_id) * 0x9e3779b97f4a7c15ULL));

    uniform_real_distribution<double> r01(0.0, 1.0);
    uniform_int_distribution<int> key_dist(0, p.num_keys - 1);
    uniform_int_distribution<int> val_dist(0, 999999);

    for (int i = 0; i < p.ops; i++) {
        string key = "key" + to_string(key_dist(rng));
        double x = r01(rng);

        if (x < p.get_fraction) {
            // GET operation
            auto start = chrono::steady_clock::now();
            string val;
            bool ok = p.get_func(key, p.client_id, *p.servers, val);
            auto end = chrono::steady_clock::now();
            
            double us = chrono::duration_cast<chrono::microseconds>(end - start).count();

            {
                lock_guard<mutex> guard(*p.lat_lock);
                p.get_latencies->push_back(us);
            }

            if (ok) p.succ_get->fetch_add(1);
            else p.fail->fetch_add(1);

        } else {
            // PUT operation
            string value = "v" + to_string(p.client_id) + "_" + to_string(val_dist(rng));

            auto start = chrono::steady_clock::now();
            bool ok = p.put_func(key, value, p.client_id, *p.servers);
            auto end = chrono::steady_clock::now();

            double us = chrono::duration_cast<chrono::microseconds>(end - start).count();

            {
                lock_guard<mutex> guard(*p.lat_lock);
                p.put_latencies->push_back(us);
            }

            if(ok) p.succ_put->fetch_add(1);
            else p.fail->fetch_add(1);
        }
    }
}

double percentile(vector<double> &v, double p) {
    if (v.empty()) return 0.0;
    sort(v.begin(), v.end());
    size_t idx = (size_t)(p * (v.size() - 1));
    return v[idx];
}

int main(int argc, char *argv[]) {
    if (argc < 7) {
        cout << "Usage:\n";
        cout << "./workload <protocol> <num_clients> <ops_per_client> <get_fraction> <num_keys> <ip:port>...\n";
        cout << "  protocol: 'abd' or 'blocking'\n";
        return 1;
    }

    string protocol = argv[1];
    int num_clients = stoi(argv[2]);
    int ops = stoi(argv[3]);
    double get_frac = stod(argv[4]);
    int num_keys = stoi(argv[5]);

    // Select protocol functions
    GetFunc get_func;
    PutFunc put_func;

    if (protocol == "abd") 
    {
        get_func = ABD::get;
        put_func = ABD::put;
    }
    else if (protocol == "blocking") 
    {
        get_func = Blocking::get;
        put_func = Blocking::put;
    }
    else 
    {
        cout << "Invalid protocol. Use 'abd' or 'blocking'\n";
        return 1;
    }

    vector<ServerInfo>servers;
    for (int i=6; i<argc; i++) {
        servers.push_back(Network::parse_server(argv[i]));
    }

    atomic<long long> succ_get{0}, succ_put{0}, fail{0};
    vector<double> get_latencies;
    vector<double> put_latencies;
    mutex lat_lock;

    vector<thread>threads;

    auto t0 = chrono::steady_clock::now();

    for (int i = 0; i < num_clients; i++) {
        WorkerParams p{i+1, ops, get_frac, num_keys, &servers,
            get_func, put_func,
            &succ_get, &succ_put, &fail,
            &get_latencies, &put_latencies, &lat_lock
        };
        threads.emplace_back(worker_func, p);
    }

    for (auto &t:threads)
        t.join();

    auto t1 = chrono::steady_clock::now();
    double elapsed = chrono::duration<double>(t1-t0).count();
    long long total_ops = (long long)num_clients*ops;

    cout << "[" << protocol << " Workload] Completed.\n";
    cout << "  GET success: " << succ_get << "\n";
    cout << "  PUT success: " << succ_put << "\n";
    cout << "  FAIL count:  " << fail << "\n";
    cout << "  Total ops attempted:      " << total_ops << "\n";
    cout << "  Total ops succeeded:      " << (succ_get + succ_put) << "\n";
    cout << "  Elapsed:     " << elapsed << " sec\n";
    cout << "  Throughput:  " << ((succ_get + succ_put) / elapsed) << " ops/sec\n\n";

    cout << "--- Latency (microseconds) ---\n";
    cout << "GET median: " << percentile(get_latencies, 0.50) << "\n";
    cout << "GET p95:    " << percentile(get_latencies, 0.95) << "\n";
    cout << "PUT median: " << percentile(put_latencies, 0.50) << "\n";
    cout << "PUT p95:    " << percentile(put_latencies, 0.95) << "\n";

    return 0;
}