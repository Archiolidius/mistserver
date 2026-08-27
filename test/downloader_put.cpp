/// Tests the chunked-PUT upload path used by HLS push targets:
/// Downloader::startPut (100-continue handling, deadline, retries) and
/// Util::finalizePreviousUpload (final-status classification).
///
/// A serving thread answers PUT requests from a scripted list of behaviors, so
/// each case drives one branch of the retry/classification policy. Plain HTTP
/// only - the TLS handshake path has no fixture here and is covered by the
/// external fault-injection rig instead.

#include <mist/downloader.h>
#include <mist/socket.h>
#include <mist/timing.h>
#include <mist/util.h>

#include <fcntl.h>

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

  /// What the fake server does with one incoming PUT request.
  enum ServerBehavior {
    ACCEPT_THEN_200,  ///< 100 Continue, read the body, answer 200, close
    ACCEPT_THEN_403,  ///< 100 Continue, read the body, answer 403, close
    ACCEPT_THEN_500,  ///< 100 Continue, read the body, answer 500, close
    ACCEPT_THEN_QUIET,///< 100 Continue, read the body, never answer
    DENY_500,         ///< answer 500 instead of the 100 Continue
    DENY_403,         ///< answer 403 instead of the 100 Continue
    NEVER_RESPOND,    ///< read the request, send nothing at all
    // Keep-alive behaviors for the persistent-connection (MIST_PUT_PERSISTENT)
    // cases: answer and KEEP the connection open for a next request.
    KEEP_200,         ///< 200 with Content-Length: 0, connection kept open
    KEEP_200_CHUNKED, ///< 200 with a chunked (zero-chunk) final response, kept open
    KEEP_200_CLOSEHDR,///< 200 with "Connection: close" but the socket KEPT OPEN: proves the header check itself
    KEEP_200_DUPCL,   ///< 200 with two Content-Length headers (case variants), kept open
    KEEP_200_SPACECL, ///< 200 with "Content-Length : 5" (space before colon) plus "Content-Length: 0", kept open
    KEEP_200_THEN_GARBAGE,///< 200 with Content-Length: 0, then unsolicited garbage bytes, kept open
    KEEP_200_THEN_DROP,///< 200 kept open, then the server closes it (idle-close)
    KEEP_204,          ///< 204 No Content (no body headers at all), kept open
    KEEP_200_CLOSEDELIM,///< 200 with no framing headers, delimited by close
    KEEP_200_BADCL,    ///< 200 with "Content-Length: 0x" (trailing garbage), kept open
    KEEP_200_BIGCL,    ///< 200 with an oversized Content-Length, kept open
    KEEP_200_EMPTYTE   ///< 200 with an EMPTY Transfer-Encoding header + CL: 0, kept open
  };

  std::condition_variable cv;
  std::mutex mut;
  int boundPort = 0;
  Socket::Server srv;
  std::vector<ServerBehavior> script;
  size_t served = 0;
  std::atomic<bool> stopServing(false);
  std::vector<std::string> requestHeaders; ///< headers of each served request, script order (guarded by mut)
  std::atomic<int> acceptCount(0); ///< distinct TCP connections the server accepted

  /// Serves as many requests as the script has entries, one behavior each.
  void serving(){
    srv = Socket::Server(0, "127.0.0.1", true); // nonblocking: accept() must not block once the script is done
    {
      std::lock_guard<std::mutex> g(mut);
      boundPort = srv.getBoundAddr().port();
      if (!boundPort){boundPort = -1;}
    }
    cv.notify_all();
    if (boundPort == -1){return;}
    std::vector<Socket::Connection> held; // keep quiet connections open
    // Runs until the main thread stops it, not until the script is exhausted:
    // connections held open on purpose (ACCEPT_THEN_QUIET / NEVER_RESPOND) must
    // stay open while the client waits, or the client sees a drop instead of silence.
    while (!stopServing){
      if (served >= script.size()){
        Util::sleep(10);
        continue;
      }
      Socket::Connection C = srv.accept();
      if (!C){
        Util::sleep(10);
        continue;
      }
      ++acceptCount;
      C.setBlocking(false);
      // One accepted connection may serve several requests (keep-alive
      // behaviors); each request consumes one script entry.
      bool keepConn = true;
      bool parked = false; // pushed to `held`: must NOT be closed here (close() shutdowns the shared fd)
      while (keepConn && !stopServing && served < script.size()){
        ServerBehavior behavior = script[served];
        // Read the request headers (they end with a blank line).
        uint64_t giveUp = Util::bootMS() + 5000;
        std::string req;
        while (C && Util::bootMS() < giveUp && req.find("\r\n\r\n") == std::string::npos){
          if (C.spool()){
            while (C.Received().size()){
              req += C.Received().get();
              C.Received().get().clear();
            }
          }else{
            Util::sleep(5);
          }
        }
        if (req.find("\r\n\r\n") == std::string::npos){
          // No request arrived on this connection (died or idle): do not consume
          // the script entry - the client will reconnect for it.
          break;
        }
        served++;
        {
          std::lock_guard<std::mutex> g(mut);
          requestHeaders.push_back(req);
        }
        if (behavior == NEVER_RESPOND){
          held.push_back(C);
          parked = true;
          keepConn = false;
          break;
        }
        if (behavior == DENY_500){
          C.SendNow("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
          C.close();
          break;
        }
        if (behavior == DENY_403){
          C.SendNow("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n");
          C.close();
          break;
        }
        C.SendNow("HTTP/1.1 100 Continue\r\n\r\n");
        // Drain the chunked body until the terminating zero chunk arrives.
        giveUp = Util::bootMS() + 5000;
        std::string body;
        while (C && Util::bootMS() < giveUp && body.find("0\r\n\r\n") == std::string::npos){
          if (C.spool()){
            while (C.Received().size()){
              body += C.Received().get();
              C.Received().get().clear();
            }
          }else{
            Util::sleep(5);
          }
        }
        switch (behavior){
        case ACCEPT_THEN_200: C.SendNow("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"); keepConn = false; break;
        case ACCEPT_THEN_403: C.SendNow("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n"); keepConn = false; break;
        case ACCEPT_THEN_500: C.SendNow("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n"); keepConn = false; break;
        case KEEP_200: C.SendNow("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"); break;
        case KEEP_200_CHUNKED: C.SendNow("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"); break;
        case KEEP_200_CLOSEHDR: C.SendNow("HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"); break;
        case KEEP_200_DUPCL: C.SendNow("HTTP/1.1 200 OK\r\nContent-Length: 0\r\ncontent-length: 0\r\n\r\n"); break;
        case KEEP_200_SPACECL: C.SendNow("HTTP/1.1 200 OK\r\nContent-Length : 5\r\nContent-Length: 0\r\n\r\n"); break;
        case KEEP_200_THEN_GARBAGE: C.SendNow("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"); Util::sleep(300); C.SendNow("BOGUS\r\n"); break;
        case KEEP_200_THEN_DROP: C.SendNow("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"); Util::sleep(50); keepConn = false; break;
        case KEEP_204: C.SendNow("HTTP/1.1 204 No Content\r\n\r\n"); break;
        case KEEP_200_CLOSEDELIM: C.SendNow("HTTP/1.1 200 OK\r\n\r\n"); Util::sleep(50); keepConn = false; break;
        case KEEP_200_BADCL: C.SendNow("HTTP/1.1 200 OK\r\nContent-Length: 0x\r\n\r\n"); break;
        case KEEP_200_BIGCL: C.SendNow("HTTP/1.1 200 OK\r\nContent-Length: 999999999999999999\r\n\r\n"); break;
        case KEEP_200_EMPTYTE: C.SendNow("HTTP/1.1 200 OK\r\nTransfer-Encoding:\r\nContent-Length: 0\r\n\r\n"); break;
        default: break; // ACCEPT_THEN_QUIET answers nothing
        }
        if (behavior == ACCEPT_THEN_QUIET){
          held.push_back(C);
          parked = true;
          keepConn = false;
          break;
        }
      }
      if (!keepConn){
        if (!parked){C.close();}
        continue;
      }
      // Keep-alive connection with no further scripted requests: hold it open so
      // a client that still owns it sees a live socket, not a drop.
      held.push_back(C);
    }
    for (size_t i = 0; i < held.size(); ++i){held[i].close();}
    srv.close();
  }

  bool startServer(const std::vector<ServerBehavior> &behaviors, std::thread &t, std::string &url){
    script = behaviors;
    served = 0;
    acceptCount = 0;
    requestHeaders.clear();
    stopServing = false;
    boundPort = 0;
    t = std::thread(serving);
    std::unique_lock<std::mutex> g(mut);
    cv.wait(g, [](){return boundPort != 0;});
    if (boundPort == -1){
      g.unlock();
      // Never destroy a joinable thread: that calls std::terminate. The serving
      // thread has already returned in this path, so the join is immediate.
      stopServing = true;
      if (t.joinable()){t.join();}
      return false;
    }
    url = "http://127.0.0.1:" + std::to_string(boundPort) + "/upload";
    return true;
  }

  /// The serving thread owns srv and closes it on exit; main only raises the flag.
  /// Closing a socket from another thread while accept() may be using it is a race.
  void stopServer(std::thread &t){
    stopServing = true;
    if (t.joinable()){t.join();}
  }

  const char *finName(Util::PutFinalizeResult r){
    switch (r){
    case Util::PUT_FIN_NONE: return "none";
    case Util::PUT_FIN_OK_2XX: return "ok_2xx";
    case Util::PUT_FIN_TRANSIENT: return "transient";
    case Util::PUT_FIN_PERMANENT: return "permanent";
    case Util::PUT_FIN_NO_RESPONSE: return "no_response";
    }
    return "?";
  }

  bool fail(const std::string &what){
    std::cerr << "FAIL: " << what << std::endl;
    return false;
  }

  /// Uploads one small chunk to the given URL, then finalizes it.
  /// Returns true when the open succeeded; reports the finalize classification.
  bool uploadOnce(const std::string &url, uint64_t deadlineMS, Util::PutFinalizeResult &finResult, bool ytPushIdentity = false){
    Socket::Connection conn;
    if (!Util::openNextUpload(url, conn, false, deadlineMS, ytPushIdentity)){
      finResult = Util::PUT_FIN_NONE;
      return false;
    }
    conn.SendNow("hello", 5);
    finResult = Util::finalizePreviousUpload(conn, url, deadlineMS);
    return true;
  }

  /// Upload using a caller-owned connection and persistent uploader, mirroring
  /// the output.cpp cycle: finalize the previous upload, open the next, send a
  /// small body. finResult is the finalize classification of the PREVIOUS upload.
  bool uploadCycle(const std::string &url, Socket::Connection &conn, Util::PersistentUploader &pu,
                   Util::PutFinalizeResult &finResult, uint64_t finDeadlineMS = 0){
    finResult = Util::finalizePreviousUpload(conn, url, finDeadlineMS, &pu);
    if (!Util::openNextUpload(url, conn, false, 0, false, &pu)){return false;}
    conn.SendNow("hello", 5);
    return true;
  }

  std::string capturedHeaders(size_t idx){
    std::lock_guard<std::mutex> g(mut);
    if (idx >= requestHeaders.size()){return "";}
    return requestHeaders[idx];
  }

  size_t countOf(const std::string &hay, const std::string &needle){
    size_t n = 0, pos = 0;
    while ((pos = hay.find(needle, pos)) != std::string::npos){++n; pos += needle.size();}
    return n;
  }

}// namespace

int main(int argc, char **argv){
  if (argc < 2){
    std::cout << "Usage: " << argv[0] << " <case>" << std::endl;
    std::cout << "Cases: ok403 500 quiet deny500retry deny403 deadline" << std::endl;
    return 1;
  }
  std::string testCase = argv[1];
  // Hermetic env: an ambient MIST_PUT_* in a dev or CI shell must not leak into
  // the legacy cases or make the gate-off cases false-fail. Each persistent
  // case sets exactly what it needs below.
  unsetenv("MIST_PUT_DEADLINE_MS");
  unsetenv("MIST_PUT_PERSISTENT");
  unsetenv("MIST_PUT_USER_AGENT");
  std::thread t;
  std::string url;
  bool ok = true;

  if (testCase == "ok403"){
    // A 2xx final must classify as ok_2xx; a 403 final as permanent.
    if (!startServer({ACCEPT_THEN_200, ACCEPT_THEN_403}, t, url)){return 1;}
    Util::PutFinalizeResult r;
    if (!uploadOnce(url, 0, r)){ok = fail("first upload could not open");}
    else if (r != Util::PUT_FIN_OK_2XX){ok = fail(std::string("200 classified as ") + finName(r));}
    if (ok){
      if (!uploadOnce(url, 0, r)){ok = fail("second upload could not open");}
      else if (r != Util::PUT_FIN_PERMANENT){ok = fail(std::string("403 classified as ") + finName(r));}
    }
  }else if (testCase == "500"){
    // A 5xx final is transient: retryable, never a permanent teardown.
    if (!startServer({ACCEPT_THEN_500}, t, url)){return 1;}
    Util::PutFinalizeResult r;
    if (!uploadOnce(url, 0, r)){ok = fail("upload could not open");}
    else if (r != Util::PUT_FIN_TRANSIENT){ok = fail(std::string("500 classified as ") + finName(r));}
  }else if (testCase == "quiet"){
    // Accepted but never answered: no_response, and the wait must respect the
    // caller's deadline rather than the full 5 s default.
    if (!startServer({ACCEPT_THEN_QUIET}, t, url)){return 1;}
    Util::PutFinalizeResult r;
    uint64_t start = Util::bootMS();
    uint64_t deadline = start + 1500;
    if (!uploadOnce(url, deadline, r)){ok = fail("upload could not open");}
    else if (r != Util::PUT_FIN_NO_RESPONSE){ok = fail(std::string("silence classified as ") + finName(r));}
    uint64_t spent = Util::bootMS() - start;
    // 1.5 s deadline plus one 1 s ev.await tick, plus slack for a loaded CI box.
    if (ok && spent > 4000){
      ok = fail("finalize ignored the deadline, took " + std::to_string(spent) + "ms");
    }
  }else if (testCase == "deny500retry"){
    // A 5xx answered instead of the 100-continue is transient: with a deadline
    // the next attempt runs in place and succeeds.
    if (!startServer({DENY_500, ACCEPT_THEN_200}, t, url)){return 1;}
    Util::PutFinalizeResult r;
    if (!uploadOnce(url, Util::bootMS() + 12000, r)){ok = fail("5xx at open was not retried");}
    else if (r != Util::PUT_FIN_OK_2XX){ok = fail(std::string("retry finalized as ") + finName(r));}
  }else if (testCase == "deny403"){
    // A 4xx answered instead of the 100-continue is permanent: fail without
    // burning the remaining budget on retries.
    if (!startServer({DENY_403, ACCEPT_THEN_200}, t, url)){return 1;}
    Util::PutFinalizeResult r;
    uint64_t start = Util::bootMS();
    if (uploadOnce(url, start + 12000, r)){ok = fail("4xx at open was retried instead of failing");}
    uint64_t spent = Util::bootMS() - start;
    if (ok && spent > 2000){
      ok = fail("4xx at open took " + std::to_string(spent) + "ms, expected an immediate failure");
    }
  }else if (testCase == "persist"){
    // Gate + deadline on, length-delimited keep-alive responses: the second and
    // third uploads must REUSE the first connection (one accept total).
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200, KEEP_200, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 3; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
      else if (i && r != Util::PUT_FIN_OK_2XX){ok = fail(std::string("prior 200 classified as ") + finName(r));}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 1){ok = fail("expected 1 accepted connection, server saw " + std::to_string(acceptCount));}
    }
  }else if (testCase == "persistoff"){
    // Deadline on but the persistence gate off: every upload reconnects (legacy).
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    if (!startServer({KEEP_200, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 2; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("gate off must reconnect per upload, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistnodeadline"){
    // Persistence gate on WITHOUT the deadline: inert by design (the no-deadline
    // startPut path cannot retry a stale socket), so behavior stays legacy.
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    unsetenv("MIST_PUT_DEADLINE_MS");
    if (!startServer({KEEP_200, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 2; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("persistence without deadline must stay inert, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistchunked"){
    // A chunked final response is NEVER reusable, even when the visible buffer
    // looks empty: the parser stops at the zero-size chunk without consuming
    // the terminator. Both uploads must use fresh connections.
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200_CHUNKED, KEEP_200_CHUNKED}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 2; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
      else if (i && r != Util::PUT_FIN_OK_2XX){ok = fail(std::string("chunked 200 classified as ") + finName(r));}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("chunked responses must not be reused, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistclose"){
    // "Connection: close" in the final response forbids reuse.
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200_CLOSEHDR, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 2; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("Connection: close must forbid reuse, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistdupcl"){
    // Duplicate Content-Length headers (case variants) are a framing hazard the
    // case-sensitive header map cannot expose after parsing; reuse must refuse.
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200_DUPCL, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 2; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("duplicate Content-Length must forbid reuse, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persist204"){
    // A 204 without an explicit Content-Length: 0 must NOT be reused: the
    // parser returns at a 204's header end before any body validation, so a
    // malformed 204 with a delayed body would desync the next response. The
    // gate accepts exactly one shape - explicit Content-Length: 0.
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_204, KEEP_204}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 2; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
      else if (i && r != Util::PUT_FIN_OK_2XX){ok = fail(std::string("204 classified as ") + finName(r));}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("204 without Content-Length: 0 must not be reused, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistspacecl"){
    // "Content-Length : 5" (space before the colon) collapses into the same
    // trimmed map key as "Content-Length: 0", so only the parse-time duplicate
    // tracker can see the conflict - and it must, or a certified reuse would
    // read the 5 declared body bytes as the next response.
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200_SPACECL, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 2; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("space-before-colon duplicate Content-Length must forbid reuse, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistdesync"){
    // Unsolicited bytes arriving AFTER a connection was certified for reuse:
    // the next request must abort cleanly (the stale line reads as a denial)
    // instead of attributing any later response to the wrong request. Bounded
    // loss - one failed open - is the contract; desync is the bug.
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200_THEN_GARBAGE, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    if (!uploadCycle(url, conn, pu, r)){ok = fail("first upload could not open");}
    if (ok){
      r = Util::finalizePreviousUpload(conn, url, 0, &pu); // certifies before the garbage lands
      if (r != Util::PUT_FIN_OK_2XX){ok = fail(std::string("clean 200 classified as ") + finName(r));}
    }
    if (ok){
      Util::sleep(600); // let the unsolicited bytes reach our socket
      if (Util::openNextUpload(url, conn, false, 0, false, &pu)){
        ok = fail("stale unsolicited bytes did not abort the reused request");
        Util::finalizePreviousUpload(conn, url, 0, &pu);
      }
      conn.close();
      if (ok && acceptCount != 1){ok = fail("expected no reconnect before the abort, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistclosedelim"){
    // A close-delimited response (no framing headers) is complete only when the
    // peer closes: by definition not reusable.
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200_CLOSEDELIM, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 2; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("close-delimited must not be reused, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistbadcl"){
    // "Content-Length: 0x": atoi would read 0, the exact-string rule must refuse.
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200_BADCL, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 2; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("trailing-garbage Content-Length must not be reused, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistbigcl"){
    // An oversized Content-Length never completes parsing: bounded finalize
    // reports no response, and the connection must not be reused.
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200_BIGCL, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    if (!uploadCycle(url, conn, pu, r)){ok = fail("first upload could not open");}
    if (ok && !uploadCycle(url, conn, pu, r, Util::bootMS() + 1200)){ok = fail("second upload could not open");}
    if (ok && r == Util::PUT_FIN_OK_2XX){ok = fail("oversized Content-Length classified as a completed 2xx");}
    if (ok){
      Util::finalizePreviousUpload(conn, url, Util::bootMS() + 1200, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("oversized Content-Length must not be reused, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistemptyte"){
    // An EMPTY Transfer-Encoding header is still a Transfer-Encoding header:
    // presence disqualifies (hasHeader, not value).
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200_EMPTYTE, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    for (int i = 0; ok && i < 2; ++i){
      if (!uploadCycle(url, conn, pu, r)){ok = fail("upload could not open");}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("empty Transfer-Encoding header must not be reused, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistidle"){
    // The server idle-closes a reusable connection between cycles: the next
    // upload must transparently reconnect and still deliver (no stuck state).
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200_THEN_DROP, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    if (!uploadCycle(url, conn, pu, r)){ok = fail("first upload could not open");}
    if (ok){
      // Wait until the peer's close is actually visible on our side (bounded),
      // rather than a fixed sleep that can race a loaded machine.
      uint64_t waitEnd = Util::bootMS() + 3000;
      while (conn && Util::bootMS() < waitEnd){
        conn.spool();
        Util::sleep(20);
      }
      if (!uploadCycle(url, conn, pu, r)){ok = fail("post-idle-close upload could not open");}
      else if (r != Util::PUT_FIN_OK_2XX){ok = fail(std::string("first 200 classified as ") + finName(r));}
    }
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu);
      conn.close();
      if (acceptCount != 2){ok = fail("idle-close must trigger one reconnect, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "persistswap"){
    // KTD-8 identity gate: after the connection object is redirected elsewhere
    // (the /dev/null segment-fallback shape), reuse must not engage - the
    // generation mismatch forces a clean reconnect instead of writing a request
    // into the wrong descriptor.
    setenv("MIST_PUT_DEADLINE_MS", "3000", 1);
    setenv("MIST_PUT_PERSISTENT", "1", 1);
    if (!startServer({KEEP_200, KEEP_200}, t, url)){return 1;}
    Socket::Connection conn;
    Util::PersistentUploader pu;
    Util::PutFinalizeResult r;
    if (!uploadCycle(url, conn, pu, r)){ok = fail("first upload could not open");}
    if (ok){
      Util::finalizePreviousUpload(conn, url, 0, &pu); // certifies reuse eligibility
      int devNull = open("/dev/null", O_WRONLY);
      if (devNull < 0){ok = fail("could not open /dev/null");}
      else{
        conn.open(devNull); // generation bump: no longer the certified connection
        // Time-bound the reconnect: the generation gate closes the swapped
        // descriptor BEFORE the request, so the fresh connect is immediate.
        // Without the gate the request would go into /dev/null first and only
        // recover through the retry backoff (>=300 ms) - so a fast open is the
        // observable proof the gate (not the fallback) did the work.
        uint64_t swapStart = Util::bootMS();
        if (!Util::openNextUpload(url, conn, false, 0, false, &pu)){ok = fail("post-swap upload could not open");}
        else if (Util::bootMS() - swapStart > 250){
          ok = fail("post-swap open took " + std::to_string(Util::bootMS() - swapStart) + "ms: retry fallback, not the generation gate");
        }
        else{
          conn.SendNow("hello", 5);
          if (Util::finalizePreviousUpload(conn, url, 0, &pu) != Util::PUT_FIN_OK_2XX){
            ok = fail("post-swap upload did not complete on a fresh connection");
          }
        }
      }
      conn.close();
      if (ok && acceptCount != 2){ok = fail("descriptor swap must force reconnect, server saw " + std::to_string(acceptCount) + " accepts");}
    }
  }else if (testCase == "statsmono"){
    // Socket::Connection statistics keep their historical per-connection
    // semantics everywhere (counters reset on reopen, exactly as before this
    // branch), and the reconnect generation - the reuse identity - changes on
    // every reopen and never resets.
    // drop() closes the previous descriptor, so every reopen needs a fresh fd.
    int fd1 = open("/dev/null", O_WRONLY);
    int fd2 = open("/dev/null", O_WRONLY);
    if (fd1 < 0 || fd2 < 0){ok = fail("could not open /dev/null");}
    else{
      Socket::Connection c(fd1, fd1);
      c.SendNow("0123456789", 10);
      if (c.dataUp() != 10){ok = fail("dataUp did not count sent bytes");}
      uint64_t genFirst = c.getGeneration();
      c.open(fd2, fd2); // reopen: historical semantics reset the counters
      if (ok && c.dataUp() != 0){ok = fail("historical semantics changed: dataUp kept bytes across reopen");}
      if (ok && c.getGeneration() == genFirst){ok = fail("generation did not change on reopen");}
      c.SendNow("0123456789", 10);
      c.resetCounter();
      if (ok && c.dataUp() != 0){ok = fail("resetCounter no longer zeroes dataUp");}
      uint64_t genBeforeReset = c.getGeneration();
      c.resetCounter();
      if (ok && c.getGeneration() != genBeforeReset){ok = fail("resetCounter must not touch the generation");}
      c.close();
    }
  }else if (testCase == "ua"){
    // MIST_PUT_USER_AGENT with the ytPushIdentity opt-in must REPLACE the
    // default User-Agent (by name, no duplicate header); without the opt-in
    // the default identity stays even with the gate on. The replace-by-name
    // behavior of extraHeaders is verified here rather than trusted.
    setenv("MIST_PUT_USER_AGENT", "1", 1);
    if (!startServer({ACCEPT_THEN_200, ACCEPT_THEN_200}, t, url)){return 1;}
    Util::PutFinalizeResult r;
    if (!uploadOnce(url, 0, r, true)){ok = fail("opted-in upload could not open");}
    else{
      std::string hdrs = capturedHeaders(0);
      if (hdrs.find("User-Agent: LiveReacting / MistServer / ") == std::string::npos){
        ok = fail("compliant identity missing from push upload headers: " + hdrs);
      }else if (countOf(hdrs, "User-Agent:") != 1){
        ok = fail("User-Agent duplicated instead of replaced: " + hdrs);
      }
    }
    if (ok){
      if (!uploadOnce(url, 0, r, false)){ok = fail("non-push upload could not open");}
      else{
        std::string hdrs = capturedHeaders(1);
        if (hdrs.find("LiveReacting / MistServer") != std::string::npos){
          ok = fail("non-push upload carried the push identity: " + hdrs);
        }else if (hdrs.find("User-Agent: ") == std::string::npos){
          ok = fail("non-push upload lost its User-Agent entirely: " + hdrs);
        }
      }
    }
  }else if (testCase == "deadline"){
    // A server that never answers must not hang the pusher: the open fails
    // within the budget instead of blocking forever.
    if (!startServer({NEVER_RESPOND, NEVER_RESPOND, NEVER_RESPOND, NEVER_RESPOND, NEVER_RESPOND}, t, url)){return 1;}
    uint64_t start = Util::bootMS();
    Util::PutFinalizeResult r;
    if (uploadOnce(url, start + 3000, r)){ok = fail("silent server reported a successful open");}
    uint64_t spent = Util::bootMS() - start;
    if (ok && spent > 4500){
      ok = fail("open blocked " + std::to_string(spent) + "ms past a 3000ms deadline");
    }
    if (ok){std::cerr << "gave up after " << spent << "ms" << std::endl;}
  }else{
    std::cerr << "Unknown case: " << testCase << std::endl;
    return 1;
  }

  stopServer(t);
  if (ok){std::cerr << "PASS: " << testCase << std::endl;}
  return ok ? 0 : 1;
}
