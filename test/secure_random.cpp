// Tests for Util::secureRandomHex, the token source behind
// MIST_HLS_UNIQUE_SEGMENTS session-unique segment naming. The uniqueness
// guarantee rests on this helper, so its contract is pinned here:
// exact length, hex-only charset, and distinct values across calls.
// (The fail-closed empty-string path needs /dev/urandom to be absent and
// cannot be forced portably from a test; it is covered by code review.)
#include <cstdio>
#include <mist/util.h>
#include <set>
#include <string>

int main(int argc, char **argv){
  // Length: N bytes must encode to exactly 2N characters
  for (size_t bytes = 1; bytes <= 16; ++bytes){
    std::string tok = Util::secureRandomHex(bytes);
    if (tok.size() != bytes * 2){
      printf("FAIL: secureRandomHex(%zu) returned %zu chars, expected %zu\n", bytes, tok.size(), bytes * 2);
      return 1;
    }
  }

  // Charset: lowercase hex only (YouTube filename charset requirement)
  std::string tok = Util::secureRandomHex(12);
  for (size_t i = 0; i < tok.size(); ++i){
    char c = tok[i];
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))){
      printf("FAIL: non-hex character '%c' in token %s\n", c, tok.c_str());
      return 1;
    }
  }

  // Distinctness: tokens from repeated calls in one process never collide.
  // 96 bits of entropy makes a collision in 1000 draws astronomically
  // unlikely; a duplicate here means the source is broken, not unlucky.
  std::set<std::string> seen;
  for (int i = 0; i < 1000; ++i){
    std::string t = Util::secureRandomHex(12);
    if (t.empty()){
      printf("FAIL: secureRandomHex returned empty (secure source unavailable?)\n");
      return 1;
    }
    if (!seen.insert(t).second){
      printf("FAIL: duplicate token %s after %d draws\n", t.c_str(), i);
      return 1;
    }
  }

  printf("OK: length, charset, and distinctness hold\n");
  return 0;
}
