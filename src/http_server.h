#pragma once
#include <functional>
#include <map>
#include <string>
#include <thread>
#include <cstdint>

#ifdef _WIN32
  #ifndef NOMINMAX
  #define NOMINMAX
  #endif
  #ifndef WIN32_LEAN_AND_MEAN
  #define WIN32_LEAN_AND_MEAN
  #endif
  #include <winsock2.h>
  #include <ws2tcpip.h>
  using socket_t = SOCKET;
  constexpr socket_t kInvalidSocket = INVALID_SOCKET;
#else
  #include <sys/types.h>
  #include <sys/socket.h>
  #include <netinet/in.h>
  #include <arpa/inet.h>
  #include <unistd.h>
  #include <signal.h>
  using socket_t = int;
  constexpr socket_t kInvalidSocket = -1;
#endif

struct HttpRequest {
  std::string method;
  std::string path;   // raw path (may include query string)
  std::map<std::string, std::string> headers;
  std::string body;
};

struct HttpResponse {
  int status = 200;
  std::string contentType = "application/json";
  std::string body;
};

class SimpleHttpServer {
 public:
  using Handler = std::function<void(const HttpRequest&, HttpResponse&)>;

  bool Post(const std::string& path, Handler h);
  bool Get(const std::string& path, Handler h);
  bool Start(uint16_t port);

 private:
  std::map<std::string, Handler> getHandlers_;
  std::map<std::string, Handler> postHandlers_;

  static bool ParseRequest(socket_t client, HttpRequest& req);
  static bool SendResponse(socket_t client, const HttpResponse& resp);
  static void HandleConnection(socket_t client, SimpleHttpServer* server);

#ifdef _WIN32
  static void CloseSocket(socket_t s) { closesocket(s); }
#else
  static void CloseSocket(socket_t s) { ::close(s); }
#endif
};
