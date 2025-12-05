#pragma once
#include "../common/types.h"
#include <vector>

namespace ABD {
    bool get(const std::string &key, int client_id, const std::vector<ServerInfo> &servers, std::string &out_value);
    
    bool put(const std::string &key, const std::string &value, int client_id, const std::vector<ServerInfo> &servers);
}
