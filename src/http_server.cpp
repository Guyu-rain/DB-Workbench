#include "http_server.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cctype>

namespace {

std::string ToLower(std::string s) {
  for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

// 只做非常轻量的 reason phrase（足够用了）
const char* ReasonPhrase(int status) {
  switch (status) {
    case 200: return "OK";
    case 400: return "Bad Request";
    case 401: return "Unauthorized";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 500: return "Internal Server Error";
    default:  return "OK";
  }
}

std::string StripQuery(const std::string& path) {
  auto pos = path.find('?');
  if (pos == std::string::npos) return path;
  return path.substr(0, pos);
}

bool ReadLine(socket_t client, std::string& out) {
  out.clear();
  char ch = 0;
  while (true) {
#ifdef _WIN32
    int n = recv(client, &ch, 1, 0);
#else
    ssize_t n = ::recv(client, &ch, 1, 0);
#endif
    if (n <= 0) return false;
    if (ch == '\r') continue;
    if (ch == '\n') break;
    out.push_back(ch);
  }
  return true;
}

bool ReadAll(socket_t client, size_t len, std::string& out) {
  out.resize(len);
  size_t received = 0;
  while (received < len) {
#ifdef _WIN32
    int n = recv(client, &out[received], static_cast<int>(len - received), 0);
#else
    ssize_t n = ::recv(client, &out[received], len - received, 0);
#endif
    if (n <= 0) return false;
    received += static_cast<size_t>(n);
  }
  return true;
}

bool SendAll(socket_t client, const char* data, size_t len) {
  size_t sent = 0;
  while (sent < len) {
#ifdef _WIN32
    int n = send(client, data + sent, static_cast<int>(len - sent), 0);
#else
    // macOS/Linux: 避免 SIGPIPE
    ssize_t n = ::send(client, data + sent, len - sent, MSG_NOSIGNAL);
#endif
    if (n <= 0) return false;
    sent += static_cast<size_t>(n);
  }
  return true;
}

} // namespace

bool SimpleHttpServer::Post(const std::string& path, Handler h) {
  postHandlers_[path] = std::move(h);
  return true;
}

bool SimpleHttpServer::Get(const std::string& path, Handler h) {
  getHandlers_[path] = std::move(h);
  return true;
}

bool SimpleHttpServer::ParseRequest(socket_t client, HttpRequest& req) {
  std::string line;
  if (!ReadLine(client, line)) return false;

  // Request line: METHOD PATH HTTP/1.1
  size_t m1 = line.find(' ');
  size_t m2 = line.find(' ', m1 + 1);
  if (m1 == std::string::npos || m2 == std::string::npos) return false;

  req.method = line.substr(0, m1);
  req.path   = line.substr(m1 + 1, m2 - m1 - 1);

  // Headers
  while (true) {
    if (!ReadLine(client, line)) return false;
    if (line.empty()) break;
    size_t colon = line.find(':');
    if (colon != std::string::npos) {
      std::string key = line.substr(0, colon);
      std::string val = line.substr(colon + 1);
      while (!val.empty() && val.front() == ' ') val.erase(val.begin());
      req.headers[ToLower(key)] = val;
    }
  }

  auto it = req.headers.find("content-length");
  size_t contentLen = 0;
  if (it != req.headers.end()) {
    try { contentLen = static_cast<size_t>(std::stoul(it->second)); }
    catch (...) { contentLen = 0; }
  }

  if (contentLen > 0) {
    if (!ReadAll(client, contentLen, req.body)) return false;
  }

  return true;
}

bool SimpleHttpServer::SendResponse(socket_t client, const HttpResponse& resp) {
  std::ostringstream oss;
  oss << "HTTP/1.1 " << resp.status << " " << ReasonPhrase(resp.status) << "\r\n";
  oss << "Content-Type: " << resp.contentType << "\r\n";
  oss << "Content-Length: " << resp.body.size() << "\r\n";
  oss << "Connection: close\r\n";
  oss << "\r\n";
  oss << resp.body;

  const std::string data = oss.str();
  return SendAll(client, data.data(), data.size());
}

void SimpleHttpServer::HandleConnection(socket_t client, SimpleHttpServer* server) {
  HttpRequest req;
  if (!ParseRequest(client, req)) {
    CloseSocket(client);
    return;
  }

  HttpResponse resp;
  resp.status = 404;
  resp.contentType = "application/json";
  resp.body = "{\"ok\":false,\"error\":\"Not found\"}";

  const std::string routePath = StripQuery(req.path);

  if (req.method == "GET") {
    auto it = server->getHandlers_.find(routePath);
    if (it != server->getHandlers_.end()) {
      resp.status = 200;
      resp.body.clear();
      it->second(req, resp);
    }
  } else if (req.method == "POST") {
    auto it = server->postHandlers_.find(routePath);
    if (it != server->postHandlers_.end()) {
      resp.status = 200;
      resp.body.clear();
      it->second(req, resp);
    }
  } else {
    resp.status = 405;
    resp.body = "{\"ok\":false,\"error\":\"Method not allowed\"}";
  }

  (void)SendResponse(client, resp);
  CloseSocket(client);
}

bool SimpleHttpServer::Start(uint16_t port) {
#ifdef _WIN32
  WSADATA wsaData;
  if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
    std::cerr << "WSAStartup failed\n";
    return false;
  }
#else
  // 忽略 SIGPIPE（双保险；SendAll 里也用了 MSG_NOSIGNAL）
  signal(SIGPIPE, SIG_IGN);
#endif

  socket_t serverSock =
#ifdef _WIN32
      socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
#else
      ::socket(AF_INET, SOCK_STREAM, 0);
#endif

  if (serverSock == kInvalidSocket) {
    std::cerr << "socket creation failed\n";
#ifdef _WIN32
    WSACleanup();
#endif
    return false;
  }

  int yes = 1;
  setsockopt(serverSock, SOL_SOCKET, SO_REUSEADDR,
#ifdef _WIN32
             reinterpret_cast<const char*>(&yes),
#else
             &yes,
#endif
             sizeof(yes));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);

  if (
#ifdef _WIN32
      bind(serverSock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR
#else
      ::bind(serverSock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0
#endif
  ) {
    std::cerr << "bind failed\n";
    CloseSocket(serverSock);
#ifdef _WIN32
    WSACleanup();
#endif
    return false;
  }

  if (
#ifdef _WIN32
      listen(serverSock, SOMAXCONN) == SOCKET_ERROR
#else
      ::listen(serverSock, SOMAXCONN) < 0
#endif
  ) {
    std::cerr << "listen failed\n";
    CloseSocket(serverSock);
#ifdef _WIN32
    WSACleanup();
#endif
    return false;
  }

  std::cout << "Server listening on http://localhost:" << port << "\n";

  while (true) {
#ifdef _WIN32
    socket_t client = accept(serverSock, nullptr, nullptr);
    if (client == INVALID_SOCKET) break;
#else
    socket_t client = ::accept(serverSock, nullptr, nullptr);
    if (client < 0) break;
#endif
    std::thread(&SimpleHttpServer::HandleConnection, client, this).detach();
  }

  CloseSocket(serverSock);
#ifdef _WIN32
  WSACleanup();
#endif
  return true;
}
