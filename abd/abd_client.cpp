#include "abd_client.h"
#include "../common/network.h"
#include <thread>
#include <sstream>
#include <numeric>
#include <vector>
#include <unistd.h>

using namespace std;

namespace ABD {

void read_task(
    int i,
    const string &key,
    const vector<int> &servers_idx,
    const vector<ServerInfo> &servers,
    vector<ReadResp> &out)
{
    int idx = servers_idx[i];
    int sock = Network::connect_to_server(servers[idx]);
    if (sock < 0) {
        out[i].valid = false;
        return;
    }

    string req = "READ_REQ " + key + "\n";
    if (!Network::send_message(sock, req)) {
        close(sock);
        out[i].valid = false;
        return;
    }

    string resp = Network::recv_line(sock);
    close(sock);

    istringstream iss(resp);
    string pfx;
    int tlc, tcid;
    string val;

    // parse the server's response
    if (!(iss >> pfx >> tlc >> tcid) || pfx != "READ_RESP") {
        out[i].valid = false;
        return;
    }
    iss >> ws;
    getline(iss, val);

    out[i] = {tlc, tcid, val, true};
}

static vector<ReadResp> read_phase(const string &key, const vector<int> &servers_idx, const vector<ServerInfo> &servers)
{
    int R = servers_idx.size();
    vector<ReadResp>out(R);
    vector<thread>threads;
    threads.reserve(R);

    for (int i = 0; i < R; i++) {
        threads.emplace_back([&, i]() {
            read_task(i, key, servers_idx, servers, out);
        });
    }

    for (auto &t : threads) t.join();
    return out;
}

static void write_phase(const string &key, int tag_lamport, int tag_cid, const string &value, const vector<int> &servers_idx, const vector<ServerInfo> &servers)
{
    vector<thread> threads;
    threads.reserve(servers_idx.size());

    for (int i : servers_idx) {
        threads.emplace_back([&, i]() {
            int sock = Network::connect_to_server(servers[i]);
            if (sock < 0) return;

            ostringstream oss;
            oss << "WRITE_REQ " << key << " " << tag_lamport << " " 
                << tag_cid << " " << value << "\n";

            Network::send_message(sock, oss.str());
            Network::recv_line(sock);
            close(sock);
        });
    }
    for (auto &t : threads) t.join();
}

static bool find_highest_tag(const vector<ReadResp> &resps, int R, int &best_ti, int &best_tc, string &best_val)
{
    int best_i = -1;
    for (int i = 0; i < R; i++) {
        if (!resps[i].valid) continue;

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
    return best_i != -1;
}

bool get(const string &key, int /*client_id*/, const vector<ServerInfo> &servers, string &out)
{
    int N = servers.size();
    int R = N/2 + 1;

    vector<int> idxs;
    for (int i=0; i<R; i++) {
        idxs.push_back(i);
    }

    auto resps = read_phase(key, idxs, servers);

    int best_ti = -1, best_tc = -1;
    string best_val;

    if (!find_highest_tag(resps, R, best_ti, best_tc, best_val)) {
        return false;
    }

    out = best_val;
    write_phase(key, best_ti, best_tc, out, idxs, servers);
    return true;
}

bool put(const string &key, const string &value, int client_id, const vector<ServerInfo> &servers)
{
    int N = servers.size();
    int R = N/2 + 1;

    vector<int> idxs;
    for (int i=0; i<R; i++) {
        idxs.push_back(i);
    }

    auto resps = read_phase(key, idxs, servers);

    int max_ti = -1, max_tc = -1;
    string dummy;
    find_highest_tag(resps, R, max_ti, max_tc, dummy);

    int new_ti = max_ti + 1;
    int new_tc = client_id;

    write_phase(key, new_ti, new_tc, value, idxs, servers);
    return true;
}

}