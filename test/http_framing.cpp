// Framing-validation tests for HTTP::Parser's Content-Length handling and the
// flags the connection-reuse logic depends on:
//   - hasFramingError(): the value is not strictly valid per RFC 9112 (digits
//     only, fits size_t). Message FRAMING deliberately keeps the legacy
//     numeric-prefix semantics atoi provided ("5, 5" frames as 5), so every
//     existing server and client behaves as before; only the crash class
//     (negative/overflow, which previously threw an uncaught length_error from
//     body.reserve() - a remote crash) stays length-unknown.
//   - hasDuplicateContentLength(): case-insensitive duplicate detection (header
//     name trimmed like SetHeader trims it) that the case-sensitive header map
//     cannot expose after parsing.
// The parser only FLAGS strictness violations; refusing reuse, or emitting
// 400/close from servers, stays the caller's responsibility.
#include <cstdio>
#include <mist/http_parser.h>
#include <string>

static int failures = 0;

static void expect(bool cond, const char *what){
  if (!cond){
    printf("FAIL: %s\n", what);
    ++failures;
  }
}

int main(int argc, char **argv){
  {
    // Sanity: a normal message still parses, no flags raised.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
    expect(P.Read(msg), "valid response parses to completion");
    expect(!P.hasFramingError(), "valid response raises no framing error");
    expect(!P.hasDuplicateContentLength(), "valid response raises no duplicate flag");
    expect(P.body == "hello", "valid response body intact");
  }
  {
    // "+1" is not strictly valid (digits only), so it flags - but it must still
    // FRAME exactly as atoi framed it (one body byte), keeping every legacy
    // caller's behavior unchanged.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length: +1\r\n\r\nX";
    bool complete = P.Read(msg);
    expect(P.hasFramingError(), "'+1' Content-Length flags a framing error");
    expect(complete, "'+1' still completes with its one body byte (legacy framing)");
    expect(P.body == "X", "'+1' frames one body byte exactly as atoi did");
  }
  {
    // Comma-separated identical list, the classic proxy-merged shape: flags
    // (not strictly valid) but frames as 5, byte-identical to the atoi era.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length: 5, 5\r\n\r\nhello";
    bool complete = P.Read(msg);
    expect(P.hasFramingError(), "'5, 5' Content-Length flags a framing error");
    expect(complete && P.body == "hello", "'5, 5' frames as 5 (legacy numeric prefix)");
  }
  {
    // "-0": atoi framed it as 0 without a crash, so the fallback must too.
    // Only negative NONZERO values were the crash class.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length: -0\r\n\r\n";
    bool complete = P.Read(msg);
    expect(P.hasFramingError(), "'-0' Content-Length flags a framing error");
    expect(complete && P.body.empty(), "'-0' frames as 0 (legacy atoi result)");
  }
  {
    // "-5": negative nonzero was the crash class on the base code (sign-extended
    // into a huge size_t, uncaught length_error from reserve): length-unknown.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length: -5\r\n\r\n";
    P.Read(msg);
    expect(P.hasFramingError(), "'-5' Content-Length flags a framing error");
  }
  {
    // An EMPTY Content-Length header is present but invalid: must flag.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length:\r\n\r\n";
    P.Read(msg);
    expect(P.hasFramingError(), "empty Content-Length flags a framing error");
  }
  {
    // Trailing garbage.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length: 0x\r\n\r\n";
    P.Read(msg);
    expect(P.hasFramingError(), "'0x' Content-Length flags a framing error");
  }
  {
    // 20 digits: overflows uint64_t. Previously this was the remote-crash input.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length: 99999999999999999999\r\n\r\n";
    P.Read(msg);
    expect(P.hasFramingError(), "overflowing Content-Length flags a framing error");
  }
  {
    // A large but valid length (> 4 GiB) is representable on 64-bit targets:
    // no framing error, message simply incomplete until that much body arrives.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length: 5000000000\r\n\r\n";
    bool complete = P.Read(msg);
    expect(!complete, ">4GiB response is incomplete without its body");
    if (sizeof(size_t) >= 8){
      expect(!P.hasFramingError(), ">4GiB Content-Length is valid on 64-bit targets");
    }else{
      expect(P.hasFramingError(), ">size_t Content-Length flags a framing error on 32-bit");
    }
  }
  {
    // The request path runs the same branch: a malformed request length flags too.
    HTTP::Parser P;
    std::string msg = "PUT /upload HTTP/1.1\r\nHost: x\r\nContent-Length: bogus\r\n\r\n";
    P.Read(msg);
    expect(P.hasFramingError(), "malformed request Content-Length flags a framing error");
  }
  {
    // Case-variant duplicates: the header map cannot show these after parsing.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\ncontent-length: 0\r\n\r\n";
    P.Read(msg);
    expect(P.hasDuplicateContentLength(), "case-variant duplicate Content-Length is flagged");
  }
  {
    // Space before the colon: SetHeader trims the name, so both fields collapse
    // into one map key. The duplicate tracker must trim the same way, or this
    // conflicting pair would read as a single clean Content-Length: 0.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nContent-Length : 5\r\nContent-Length: 0\r\n\r\n";
    P.Read(msg);
    expect(P.hasDuplicateContentLength(), "space-before-colon duplicate Content-Length is flagged");
  }
  {
    // A "close" token in ANY Connection field must be visible, even when a
    // case-variant duplicate hides it from exact-match-first GetHeader.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nconnection: close\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n";
    P.Read(msg);
    expect(P.hasConnectionClose(), "'close' hidden behind a case-variant duplicate is still flagged");
  }
  {
    // Same-case duplicates overwrite in the map: the earlier "close" would be
    // gone from GetHeader entirely. The parse-time flag must still hold it.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nConnection: close\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n";
    P.Read(msg);
    expect(P.hasConnectionClose(), "'close' overwritten by a later same-name field is still flagged");
  }
  {
    // Plain keep-alive must NOT flag.
    HTTP::Parser P;
    std::string msg = "HTTP/1.1 200 OK\r\nConnection: keep-alive\r\nContent-Length: 0\r\n\r\n";
    P.Read(msg);
    expect(!P.hasConnectionClose(), "keep-alive alone does not flag connection close");
  }
  {
    // Clean() must reset all flags.
    HTTP::Parser P;
    std::string bad = "HTTP/1.1 200 OK\r\nContent-Length: 0x\r\ncontent-length: 1\r\nConnection: close\r\n\r\n";
    P.Read(bad);
    expect(P.hasFramingError() && P.hasDuplicateContentLength() && P.hasConnectionClose(), "flags set before Clean");
    P.Clean();
    expect(!P.hasFramingError() && !P.hasDuplicateContentLength() && !P.hasConnectionClose(), "Clean resets all flags");
  }

  if (failures){return 1;}
  printf("OK: framing validation holds (%s-bit size_t)\n", sizeof(size_t) >= 8 ? "64" : "32");
  return 0;
}
