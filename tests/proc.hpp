#pragma once
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

extern char** environ;

namespace testutil {

// A child process whose stdout we read line by line.
struct Proc {
  pid_t pid = -1;
  FILE* out = nullptr;

  static Proc spawn(const std::vector<std::string>& args) {
    int fds[2];
    if (pipe(fds) != 0) throw std::runtime_error("pipe");
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_adddup2(&fa, fds[1], 1);
    posix_spawn_file_actions_addclose(&fa, fds[0]);
    std::vector<char*> argv;
    for (auto& a : args) argv.push_back(const_cast<char*>(a.c_str()));
    argv.push_back(nullptr);
    Proc p;
    if (posix_spawn(&p.pid, argv[0], &fa, nullptr, argv.data(), environ) != 0) throw std::runtime_error("spawn");
    posix_spawn_file_actions_destroy(&fa);
    close(fds[1]);
    p.out = fdopen(fds[0], "r");
    return p;
  }

  // Blocks until a stdout line starts with `prefix`; returns its last number.
  int wait_port(const std::string& prefix) {
    char line[512];
    while (std::fgets(line, sizeof line, out)) {
      std::string l = line;
      if (l.rfind(prefix, 0) == 0) return std::stoi(l.substr(l.find_last_of(' ') + 1));
    }
    throw std::runtime_error("child exited before printing: " + prefix);
  }

  void signal(int sig) { ::kill(pid, sig); }
  int wait() {
    int st = 0;
    waitpid(pid, &st, 0);
    if (out) std::fclose(out);
    out = nullptr;
    pid = -1;
    return st;
  }
  void kill9() {
    signal(SIGKILL);
    wait();
  }
  ~Proc() {
    if (pid > 0) kill9();
  }
  Proc() = default;
  Proc(Proc&& o) noexcept : pid(o.pid), out(o.out) { o.pid = -1, o.out = nullptr; }
  Proc& operator=(Proc&& o) noexcept {
    std::swap(pid, o.pid), std::swap(out, o.out);
    return *this;
  }
};

}  // namespace testutil
