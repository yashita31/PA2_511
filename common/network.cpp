#include "network.h"
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>

namespace Network {

int connect_to_server(const ServerInfo &srv) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    struct timeval tv;
    tv.tv_sec = Config::SOCKET_TIMEOUT_SEC;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(srv.port);

    if (inet_pton(AF_INET, srv.host.c_str(), &addr.sin_addr) <= 0) {
        close(sock);
        return -1;
    }

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    return sock;
}

std::string recv_line(int sock) {
    std::string out;
    char c;
    while (true) {
        ssize_t n = recv(sock, &c, 1, 0);
        if (n <= 0) return "";
        if (c == '\n') break;
        out += c;
    }
    return out;
}

ServerInfo parse_server(const std::string &spec) {
    ServerInfo s;
    auto pos = spec.find(':');
    s.host = spec.substr(0, pos);
    s.port = std::stoi(spec.substr(pos + 1));
    return s;
}

bool send_message(int sock, const std::string &msg) {
    return send(sock, msg.c_str(), msg.size(), 0) >= 0;
}

}