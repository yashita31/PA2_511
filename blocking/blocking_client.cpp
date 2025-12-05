#include "blocking_client.h"
#include "../common/network.h"
#include <thread>
#include <mutex>
#include <atomic>
#include <sstream>
#include <unistd.h>

using namespace std;

namespace Blocking {

// Acquire locks with early stopping once R locks obtained
static vector<int> acquire_locks(const string &key, int client_id, const vector<ServerInfo> &servers)
{
    int N = servers.size();
    int R = N / 2 + 1;

    vector<int> granted;
    granted.reserve(R);
    mutex mtx;
    atomic<bool> stop{false};

    vector<thread> threads;
    threads.reserve(N);

    for (int i = 0; i < N; i++) {
        threads.emplace_back([&, i]() {
            if (stop.load()) return;

            int sock = Network::connect_to_server(servers[i]);
            if (sock < 0) return;

            ostringstream oss;
            oss << "LOCK_REQ " << key << " " << client_id << "\n";

            if (!Network::send_message(sock, oss.str())) {
                close(sock);
                return;
            }

            string resp = Network::recv_line(sock);
            close(sock);

            if (resp == "LOCK_GRANTED") {
                lock_guard<mutex> guard(mtx);
                if (!stop.load()) {
                    granted.push_back(i);
                    if ((int)granted.size() >= R) {
                        stop.store(true);
                    }
                }
            }
        });
    }

    for (auto &t : threads) t.join();
    return granted;
}

// Read from quorum
static vector<ReadResp> read_quorum(const string &key, const vector<int> &server_idxs, const vector<ServerInfo> &servers)
{
    int R = server_idxs.size();
    vector<ReadResp> out(R);
    vector<thread> threads;
    threads.reserve(R);

    for (int k = 0; k < R; k++) {
        threads.emplace_back([&, k]() {
            int idx = server_idxs[k];
            int sock = Network::connect_to_server(servers[idx]);
            if (sock < 0) {
                out[k].valid = false;
                return;
            }

            string req = "READ_REQ " + key + "\n";
            if (!Network::send_message(sock, req)) {
                close(sock);
                out[k].valid = false;
                return;
            }

            string resp = Network::recv_line(sock);
            close(sock);

            istringstream iss(resp);
            string pfx;
            int t_i, t_c;

            if (!(iss >> pfx >> t_i >> t_c) || pfx != "READ_RESP") {
                out[k].valid = false;
                return;
            }

            string value;
            iss >> ws;
            getline(iss, value);

            out[k] = {t_i, t_c, value, true};
        });
    }

    for (auto &t : threads) t.join();
    return out;
}

// Write to quorum
static bool write_quorum(const string &key, int t_int, int t_client, const string &value, const vector<int> &server_idxs, const vector<ServerInfo> &servers)
{
    int R = server_idxs.size();
    atomic<int> success{0};
    vector<thread> threads;
    threads.reserve(R);

    for (int k = 0; k < R; k++) {
        threads.emplace_back([&, k]() {
            int idx = server_idxs[k];
            int sock = Network::connect_to_server(servers[idx]);
            if (sock < 0) return;

            ostringstream oss;
            oss << "WRITE_REQ " << key << " " << t_int << " "
                << t_client << " " << value << "\n";

            if (!Network::send_message(sock, oss.str())) {
                close(sock);
                return;
            }

            string resp = Network::recv_line(sock);
            close(sock);

            if (resp == "ACK") {
                success.fetch_add(1);
            }
        });
    }

    for (auto &t : threads) t.join();
    return success.load() >= R;
}

// Unlock quorum
static void unlock_quorum(const string &key, int client_id, const vector<int> &server_idxs, const vector<ServerInfo> &servers)
{
    vector<thread> threads;
    threads.reserve(server_idxs.size());

    for (int idx : server_idxs) {
        threads.emplace_back([&, idx]() {
            int sock = Network::connect_to_server(servers[idx]);
            if (sock < 0) return;

            ostringstream oss;
            oss << "UNLOCK " << key << " " << client_id << "\n";

            Network::send_message(sock, oss.str());
            Network::recv_line(sock);
            close(sock);
        });
    }

    for (auto &t : threads) t.join();
}

// Find highest tag
static bool find_highest_tag(const vector<ReadResp> &resps, int R, int &best_ti, int &best_tc, string &best_val)
{
    int best_i = -1;
    int valid = 0;

    for (int i = 0; i < R; i++) {
        if (!resps[i].valid) continue;
        valid++;

        if (best_i == -1 ||
            resps[i].t_int > best_ti ||
            (resps[i].t_int == best_ti && resps[i].t_client > best_tc))
        {
            best_i = i;
            best_ti = resps[i].t_int;
            best_tc = resps[i].t_client;
            best_val = resps[i].value;
        }
    }

    return valid >= R && best_i != -1;
}

bool get(const string &key, int client_id, const vector<ServerInfo> &servers, string &out_value)
{
    int R = servers.size()/2 + 1;

    auto granted = acquire_locks(key, client_id, servers);
    if ((int)granted.size() < R) {
        unlock_quorum(key, client_id, granted, servers);
        return false;
    }

    granted.resize(R);
    auto resps = read_quorum(key, granted, servers);

    int best_ti = -1, best_tc = -1;
    string best_val;

    if (!find_highest_tag(resps, R, best_ti, best_tc, best_val)) {
        unlock_quorum(key, client_id, granted, servers);
        return false;
    }

    out_value = best_val;
    unlock_quorum(key, client_id, granted, servers);
    return true;
}

bool put(const string &key, const string &value, int client_id, const vector<ServerInfo> &servers)
{
    int R = servers.size()/2 + 1;

    auto granted = acquire_locks(key, client_id, servers);
    if ((int)granted.size() < R) {
        unlock_quorum(key, client_id, granted, servers);
        return false;
    }

    granted.resize(R);
    auto resps = read_quorum(key, granted, servers);

    int max_ti = -1, max_tc = -1;
    string dummy;
    if (!find_highest_tag(resps, R, max_ti, max_tc, dummy)) {
        unlock_quorum(key, client_id, granted, servers);
        return false;
    }

    int new_ti = max_ti + 1;
    int new_tc = client_id;

    bool ok = write_quorum(key, new_ti, new_tc, value, granted, servers);
    unlock_quorum(key, client_id, granted, servers);
    return ok;
}

} // namespace Blocking