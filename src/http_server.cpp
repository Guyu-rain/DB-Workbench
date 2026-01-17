#include "http_server.h"
#include <iostream>
#include <sstream>

namespace {
std::string ToLower(std::string s) {
  for (auto& c : s) c = static_cast<char>(::tolower(static_cast<unsigned char>(c)));
  return s;
}

bool ReadLine(SOCKET client, std::string& out) {
  out.clear();
  char ch = 0;
  while (true) {
    int n = recv(client, &ch, 1, 0);
    if (n <= 0) return false;
    if (ch == '\r') continue;
    if (ch == '\n') break;
    out.push_back(ch);
  }
  return true;
}

bool ReadAll(SOCKET client, size_t len, std::string& out) {
  out.resize(len);
  size_t received = 0;
  while (received < len) {
    int n = recv(client, &out[received], static_cast<int>(len - received), 0);
    if (n <= 0) return false;
    received += static_cast<size_t>(n);
  }
  return true;
}
}

bool SimpleHttpServer::Post(const std::string& path, Handler h) {
  postHandlers_[path] = std::move(h);
  return true;
}

bool SimpleHttpServer::Get(const std::string& path, Handler h) {
  getHandlers_[path] = std::move(h);
  return true;
}

bool SimpleHttpServer::ParseRequest(SOCKET client, HttpRequest& req) {
  std::string line;
  if (!ReadLine(client, line)) return false;
  size_t m1 = line.find(' ');
  size_t m2 = line.find(' ', m1 + 1);
  if (m1 == std::string::npos || m2 == std::string::npos) return false;
  req.method = line.substr(0, m1);
  req.path = line.substr(m1 + 1, m2 - m1 - 1);

  // headers
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
  if (it != req.headers.end()) contentLen = static_cast<size_t>(std::stoul(it->second));
  if (contentLen > 0) {
    if (!ReadAll(client, contentLen, req.body)) return false;
  }
  return true;
}

bool SimpleHttpServer::SendResponse(SOCKET client, const HttpResponse& resp) {
  std::ostringstream oss;
  oss << "HTTP/1.1 " << resp.status << "\r\n";
  oss << "Content-Type: " << resp.contentType << "\r\n";
  oss << "Content-Length: " << resp.body.size() << "\r\n";
  oss << "Connection: close\r\n\r\n";
  oss << resp.body;
  auto data = oss.str();
  size_t sent = 0;
  while (sent < data.size()) {
    int n = send(client, data.data() + sent, static_cast<int>(data.size() - sent), 0);
    if (n <= 0) return false;
    sent += static_cast<size_t>(n);
  }
  return true;
}

void SimpleHttpServer::HandleConnection(SOCKET client, SimpleHttpServer* server) {
  HttpRequest req;
  if (!ParseRequest(client, req)) {
    closesocket(client);
    return;
  }

  HttpResponse resp;
  resp.status = 404;
  resp.body = "{\"ok\":false,\"error\":\"Not found\"}";

  if (req.method == "GET") {
    auto it = server->getHandlers_.find(req.path);
    if (it != server->getHandlers_.end()) {
      resp.status = 200;
      resp.body.clear();
      it->second(req, resp);
    }
  } else if (req.method == "POST") {
    auto it = server->postHandlers_.find(req.path);
    if (it != server->postHandlers_.end()) {
      resp.status = 200;
      resp.body.clear();
      it->second(req, resp);
    }
  }

  SendResponse(client, resp);
  closesocket(client);
}

bool SimpleHttpServer::Start(uint16_t port) {
  WSADATA wsaData;
  if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
    std::cerr << "WSAStartup failed\n";
    return false;
  }

  SOCKET server = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (server == INVALID_SOCKET) {
    std::cerr << "socket creation failed\n";
    WSACleanup();
    return false;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);

  if (bind(server, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == SOCKET_ERROR) {
    std::cerr << "bind failed\n";
    closesocket(server);
    WSACleanup();
    return false;
  }

  if (listen(server, SOMAXCONN) == SOCKET_ERROR) {
    std::cerr << "listen failed\n";
    closesocket(server);
    WSACleanup();
    return false;
  }

  std::cout << "Server listening on http://localhost:" << port << "\n";

  while (true) {
    SOCKET client = accept(server, nullptr, nullptr);
    if (client == INVALID_SOCKET) break;
    std::thread(&SimpleHttpServer::HandleConnection, client, this).detach();
  }

  closesocket(server);
  WSACleanup();
  return true;
}
