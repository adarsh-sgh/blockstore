#pragma once
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace blockstore {

// --key value / --flag parser for the CLIs. Names in `flags` never take a value.
class Args {
 public:
  Args(int argc, char** argv, std::vector<std::string> flags = {}) {
    const int first = 1;
    for (int i = first; i < argc; i++) {
      std::string a = argv[i];
      if (a.rfind("--", 0) != 0) {
        positional.push_back(a);
        continue;
      }
      a = a.substr(2);
      bool flag = false;
      for (auto& f : flags) flag |= f == a;
      if (!flag && i + 1 < argc && std::strncmp(argv[i + 1], "--", 2) != 0)
        kv_.push_back({a, argv[++i]});
      else
        kv_.push_back({a, ""});
    }
  }
  bool has(const std::string& k) const {
    for (auto& [a, b] : kv_)
      if (a == k) return true;
    return false;
  }
  std::string str(const std::string& k, const std::string& def = "") const {
    for (auto& [a, b] : kv_)
      if (a == k) return b;
    return def;
  }
  uint64_t u64(const std::string& k, uint64_t def) const {
    return has(k) ? std::strtoull(str(k).c_str(), nullptr, 10) : def;
  }
  double f64(const std::string& k, double def) const { return has(k) ? std::atof(str(k).c_str()) : def; }
  std::vector<std::string> positional;

 private:
  std::vector<std::pair<std::string, std::string>> kv_;
};

}  // namespace blockstore
