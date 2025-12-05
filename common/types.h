#pragma once
#include <string>
#include <chrono>

struct ServerInfo {
    std::string host;
    int port;
};

struct ReadResp {
    int t_int = 0;
    int t_client = 0;
    std::string value = "";
    bool valid = false;
};

struct KeyState {
    int tag_lamport = 0;
    int tag_cid = 0;
    std::string value = "";
    
    int locked_by = -1;
    std::chrono::steady_clock::time_point lock_expiry;
};

namespace Config {
    constexpr int SOCKET_TIMEOUT_SEC = 1;
    constexpr int LOCK_LEASE_SEC = 5;
}