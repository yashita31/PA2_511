#pragma once
#include "types.h"
#include <string>

namespace Network {
    int connect_to_server(const ServerInfo &srv);
    std::string recv_line(int sock);
    ServerInfo parse_server(const std::string &spec);
    bool send_message(int sock, const std::string &msg);
}