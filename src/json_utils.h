#pragma once
#include <cctype>
#include <cstring>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// A tiny JSON helper supporting objects, arrays, strings, numbers, booleans, and null.
// Designed only for small payloads in this project.
class JsonValue {
 public:
  enum class Type { Null, Bool, Number, String, Object, Array };

  JsonValue() : type_(Type::Null) {}
  explicit JsonValue(bool b) : type_(Type::Bool), bool_(b) {}
  explicit JsonValue(double n) : type_(Type::Number), number_(n) {}
  explicit JsonValue(std::string s) : type_(Type::String), str_(std::move(s)) {}
  explicit JsonValue(std::map<std::string, JsonValue> o) : type_(Type::Object), object_(std::move(o)) {}
  explicit JsonValue(std::vector<JsonValue> a) : type_(Type::Array), array_(std::move(a)) {}

  bool IsNull() const { return type_ == Type::Null; }
  bool IsBool() const { return type_ == Type::Bool; }
  bool IsNumber() const { return type_ == Type::Number; }
  bool IsString() const { return type_ == Type::String; }
  bool IsObject() const { return type_ == Type::Object; }
  bool IsArray() const { return type_ == Type::Array; }

  bool AsBool(bool def = false) const { return IsBool() ? bool_ : def; }
  double AsNumber(double def = 0.0) const { return IsNumber() ? number_ : def; }
  std::string AsString(const std::string& def = "") const { return IsString() ? str_ : def; }

  const std::map<std::string, JsonValue>& AsObject() const { return object_; }
  const std::vector<JsonValue>& AsArray() const { return array_; }

  const JsonValue* Get(const std::string& key) const {
    if (!IsObject()) return nullptr;
    auto it = object_.find(key);
    return it == object_.end() ? nullptr : &it->second;
  }

  std::string Dump() const {
    std::ostringstream oss;
    Write(oss);
    return oss.str();
  }

  static JsonValue Parse(const std::string& input, std::string& err) {
    size_t idx = 0;
    try {
      JsonValue v = ParseValue(input, idx);
      SkipWs(input, idx);
      if (idx != input.size()) {
        err = "Trailing characters after JSON";
      }
      return v;
    } catch (const std::exception& ex) {
      err = ex.what();
      return JsonValue();
    }
  }

 private:
  Type type_ = Type::Null;
  bool bool_ = false;
  double number_ = 0.0;
  std::string str_;
  std::map<std::string, JsonValue> object_;
  std::vector<JsonValue> array_;
  static void SkipWs(const std::string& s, size_t& i) {
    while (i < s.size() && std::isspace(static_cast<unsigned char>(s[i]))) ++i;
  }

  static JsonValue ParseValue(const std::string& s, size_t& i) {
    SkipWs(s, i);
    if (i >= s.size()) throw std::runtime_error("Unexpected end of JSON");
    char c = s[i];
    if (c == 'n') return ParseLiteral(s, i, "null", JsonValue());
    if (c == 't') return ParseLiteral(s, i, "true", JsonValue(true));
    if (c == 'f') return ParseLiteral(s, i, "false", JsonValue(false));
    if (c == '"') return JsonValue(ParseString(s, i));
    if (c == '{') return JsonValue(ParseObject(s, i));
    if (c == '[') return JsonValue(ParseArray(s, i));
    if (c == '-' || std::isdigit(static_cast<unsigned char>(c))) return JsonValue(ParseNumber(s, i));
    throw std::runtime_error("Invalid JSON value");
  }

  static JsonValue ParseLiteral(const std::string& s, size_t& i, const char* literal, JsonValue v) {
    size_t len = strlen(literal);
    if (s.compare(i, len, literal) != 0) throw std::runtime_error("Invalid literal");
    i += len;
    return v;
  }

  static std::string ParseString(const std::string& s, size_t& i) {
    if (s[i] != '"') throw std::runtime_error("Expected string");
    ++i;
    std::string out;
    while (i < s.size()) {
      char c = s[i++];
      if (c == '"') break;
      if (c == '\\') {
        if (i >= s.size()) throw std::runtime_error("Bad escape");
        char esc = s[i++];
        switch (esc) {
          case '"': out.push_back('"'); break;
          case '\\': out.push_back('\\'); break;
          case '/': out.push_back('/'); break;
          case 'b': out.push_back('\b'); break;
          case 'f': out.push_back('\f'); break;
          case 'n': out.push_back('\n'); break;
          case 'r': out.push_back('\r'); break;
          case 't': out.push_back('\t'); break;
          default: throw std::runtime_error("Unsupported escape");
        }
      } else {
        out.push_back(c);
      }
    }
    return out;
  }

  static double ParseNumber(const std::string& s, size_t& i) {
    size_t start = i;
    if (s[i] == '-') ++i;
    while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) ++i;
    if (i < s.size() && s[i] == '.') {
      ++i;
      while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) ++i;
    }
    if (i < s.size() && (s[i] == 'e' || s[i] == 'E')) {
      ++i;
      if (i < s.size() && (s[i] == '+' || s[i] == '-')) ++i;
      while (i < s.size() && std::isdigit(static_cast<unsigned char>(s[i]))) ++i;
    }
    double value = std::stod(s.substr(start, i - start));
    return value;
  }

  static std::map<std::string, JsonValue> ParseObject(const std::string& s, size_t& i) {
    if (s[i] != '{') throw std::runtime_error("Expected object");
    ++i;
    std::map<std::string, JsonValue> obj;
    SkipWs(s, i);
    if (i < s.size() && s[i] == '}') { ++i; return obj; }
    while (i < s.size()) {
      SkipWs(s, i);
      std::string key = ParseString(s, i);
      SkipWs(s, i);
      if (i >= s.size() || s[i] != ':') throw std::runtime_error("Expected ':'");
      ++i;
      JsonValue val = ParseValue(s, i);
      obj.emplace(std::move(key), std::move(val));
      SkipWs(s, i);
      if (i >= s.size()) throw std::runtime_error("Unterminated object");
      if (s[i] == '}') { ++i; break; }
      if (s[i] != ',') throw std::runtime_error("Expected ',' in object");
      ++i;
    }
    return obj;
  }

  static std::vector<JsonValue> ParseArray(const std::string& s, size_t& i) {
    if (s[i] != '[') throw std::runtime_error("Expected array");
    ++i;
    std::vector<JsonValue> arr;
    SkipWs(s, i);
    if (i < s.size() && s[i] == ']') { ++i; return arr; }
    while (i < s.size()) {
      JsonValue val = ParseValue(s, i);
      arr.push_back(std::move(val));
      SkipWs(s, i);
      if (i >= s.size()) throw std::runtime_error("Unterminated array");
      if (s[i] == ']') { ++i; break; }
      if (s[i] != ',') throw std::runtime_error("Expected ',' in array");
      ++i;
    }
    return arr;
  }

  void Write(std::ostringstream& oss) const {
    switch (type_) {
      case Type::Null: oss << "null"; break;
      case Type::Bool: oss << (bool_ ? "true" : "false"); break;
      case Type::Number: oss << number_; break;
      case Type::String: oss << '"' << Escape(str_) << '"'; break;
      case Type::Array:
        oss << '[';
        for (size_t i = 0; i < array_.size(); ++i) {
          if (i) oss << ',';
          array_[i].Write(oss);
        }
        oss << ']';
        break;
      case Type::Object:
        oss << '{';
        for (auto it = object_.begin(); it != object_.end(); ++it) {
          if (it != object_.begin()) oss << ',';
          oss << '"' << Escape(it->first) << '"' << ':';
          it->second.Write(oss);
        }
        oss << '}';
        break;
    }
  }

  static std::string Escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 4);
    for (char c : s) {
      switch (c) {
        case '"': out += "\\\""; break;
        case '\\': out += "\\\\"; break;
        case '\n': out += "\\n"; break;
        case '\r': out += "\\r"; break;
        case '\t': out += "\\t"; break;
        default:
          out.push_back(c);
      }
    }
    return out;
  }
};

inline JsonValue JsonObject(std::initializer_list<std::pair<const std::string, JsonValue>> list) {
  return JsonValue(std::map<std::string, JsonValue>(list));
}

inline JsonValue JsonArray(std::vector<JsonValue> arr) {
  return JsonValue(std::move(arr));
}
