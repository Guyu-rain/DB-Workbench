#pragma once
#include <functional>
#include <map>
#include <string>
#include <thread>
#include <winsock2.h>
#pragma comment(lib, "ws2_32.lib")

struct HttpRequest {
  std::string method;
  std::string path;
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

  static bool ParseRequest(SOCKET client, HttpRequest& req);
  static bool SendResponse(SOCKET client, const HttpResponse& resp);
  static void HandleConnection(SOCKET client, SimpleHttpServer* server);
};
