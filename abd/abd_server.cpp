#include "../common/types.h"
#include "../common/network.h"
#include <iostream>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <sstream>
#include <unistd.h>
#include <arpa/inet.h>

using namespace std;

mutex store_lock;
unordered_map<string, KeyState> kv;

void handle_client(int sock) {
    string msg = Network::recv_line(sock);
    if (msg.empty()) {
        close(sock);
        return;
    }

    istringstream iss(msg);
    string cmd;
    iss >> cmd;

    if (cmd == "READ_REQ") {
        string key;
        iss >> key;

        int ti, tc;
        string val;
        {
            lock_guard<mutex> guard(store_lock);
            KeyState &ks = kv[key];
            ti = ks.tag_lamport;
            tc = ks.tag_cid;
            val = ks.value;
        }

        ostringstream resp;
        resp << "READ_RESP " << ti << " " << tc << " " << val << "\n";
        Network::send_message(sock, resp.str());
        close(sock);
        return;
    }

    if (cmd == "WRITE_REQ") {
        string key;
        int ti, tc;
        string val;

        iss >> key >> ti >> tc;
        iss >> ws;
        getline(iss, val);

        {
            lock_guard<mutex> guard(store_lock);
            KeyState &ks = kv[key];
            bool newer = (ti > ks.tag_lamport) || (ti == ks.tag_lamport && tc > ks.tag_cid);
            if(newer) {
                ks.tag_lamport = ti;
                ks.tag_cid = tc;
                ks.value = val;
            }
        }

        Network::send_message(sock, "ACK\n");
        close(sock);
        return;
    }

    Network::send_message(sock, "ERR\n");
    close(sock);
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        cout<<"Usage: ./abd_server <port>\n";
        return 1;
    }

    int port = stoi(argv[1]);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

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


    cout << "ABD Server Listening on port " << port << "...\n";

    while(1) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int client_sock = accept(server_fd, (sockaddr*)&client_addr, &len);
        if (client_sock < 0) continue;

        thread(handle_client, client_sock).detach();
    }

    return 0;
}