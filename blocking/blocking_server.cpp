#include "../common/types.h"
#include "../common/network.h"
#include <iostream>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <sstream>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>

using namespace std;

mutex state_lock;
unordered_map<string, KeyState> kv_store;

// Check if lock has expired
static bool lock_expired(const KeyState &ks) {
    return ks.locked_by != -1 &&
           chrono::steady_clock::now() > ks.lock_expiry;
}

void handle_client(int client_sock) {
    string msg = Network::recv_line(client_sock);
    if (msg.empty()) {
        close(client_sock);
        return;
    }

    istringstream iss(msg);
    string cmd;
    iss >> cmd;

    if (cmd == "LOCK_REQ") {
        string key;
        int client_id;
        iss >> key >> client_id;

        bool granted = false;
        {
            lock_guard<mutex> guard(state_lock);
            KeyState &ks = kv_store[key];

            if (lock_expired(ks)) {
                ks.locked_by = -1;
            }

            if (ks.locked_by == -1) {
                ks.locked_by = client_id;
                ks.lock_expiry = chrono::steady_clock::now() +
                                chrono::seconds(Config::LOCK_LEASE_SEC);
                granted = true;
            }
        }

        string resp = granted ? "LOCK_GRANTED\n" : "LOCK_DENIED\n";
        Network::send_message(client_sock, resp);
        close(client_sock);
        return;
    }

    if (cmd == "UNLOCK") {
        string key;
        int client_id;
        iss >> key >> client_id;

        {
            lock_guard<mutex> guard(state_lock);
            KeyState &ks = kv_store[key];

            if (ks.locked_by == client_id || lock_expired(ks)) {
                ks.locked_by = -1;
            }
        }

        Network::send_message(client_sock, "ACK\n");
        close(client_sock);
        return;
    }

    if (cmd == "READ_REQ") {
        string key;
        iss >> key;

        int t_int, t_client;
        string val;

        {
            lock_guard<mutex> guard(state_lock);
            KeyState &ks = kv_store[key];

            if (lock_expired(ks)) {
                ks.locked_by = -1;
            }

            t_int = ks.tag_lamport;
            t_client = ks.tag_cid;
            val = ks.value;
        }

        ostringstream oss;
        oss << "READ_RESP " << t_int << " " << t_client << " " << val << "\n";
        Network::send_message(client_sock, oss.str());
        close(client_sock);
        return;
    }

    if (cmd == "WRITE_REQ") {
        string key;
        int new_t_int, new_t_client;

        iss >> key >> new_t_int >> new_t_client;
        iss >> ws;
        string new_value;
        getline(iss, new_value);

        bool ok = false;
        {
            lock_guard<mutex> guard(state_lock);
            KeyState &ks = kv_store[key];

            if (lock_expired(ks)) {
                ks.locked_by = -1;
            }

            // Must hold lock to write
            if (ks.locked_by == new_t_client) {
                bool newer = (new_t_int > ks.tag_lamport) ||
                            (new_t_int == ks.tag_lamport && 
                             new_t_client > ks.tag_cid);

                if (newer) {
                    ks.tag_lamport = new_t_int;
                    ks.tag_cid = new_t_client;
                    ks.value = new_value;
                }
                ok = true;
            }
        }

        string resp = ok ? "ACK\n" : "WRITE_DENIED\n";
        Network::send_message(client_sock, resp);
        close(client_sock);
        return;
    }

    Network::send_message(client_sock, "ERR\n");
    close(client_sock);
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cerr << "Usage: ./blocking_server <port>\n";
        return 1;
    }

    int port = stoi(argv[1]);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        perror("socket");
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt(SO_REUSEADDR)");
        exit(1);
    }

    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(server_fd);
        exit(1);
    }

    if (listen(server_fd, 50) < 0) {
        perror("listen");
        close(server_fd);
        exit(1);
    }

    cout << "[Blocking Server] Listening on port " << port << "...\n";

    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_sock = accept(server_fd, (sockaddr*)&client_addr, &client_len);
        if (client_sock < 0) {
            perror("accept");
            continue;
        }

        thread(handle_client, client_sock).detach();
    }

    close(server_fd);
    return 0;
}