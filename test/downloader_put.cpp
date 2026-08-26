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
    ACCEPT_THEN_200,  ///< 100 Continue, read the body, answer 200
    ACCEPT_THEN_403,  ///< 100 Continue, read the body, answer 403
    ACCEPT_THEN_500,  ///< 100 Continue, read the body, answer 500
    ACCEPT_THEN_QUIET,///< 100 Continue, read the body, never answer
    DENY_500,         ///< answer 500 instead of the 100 Continue
    DENY_403,         ///< answer 403 instead of the 100 Continue
    NEVER_RESPOND     ///< read the request, send nothing at all
  };

  std::condition_variable cv;
  std::mutex mut;
  int boundPort = 0;
  Socket::Server srv;
  std::vector<ServerBehavior> script;
  size_t served = 0;
  std::atomic<bool> stopServing(false);
  std::vector<std::string> requestHeaders; ///< headers of each served request, script order (guarded by mut)

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
      C.setBlocking(false);
      ServerBehavior behavior = script[served++];
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
      {
        std::lock_guard<std::mutex> g(mut);
        requestHeaders.push_back(req);
      }
      if (behavior == NEVER_RESPOND){
        held.push_back(C);
        continue;
      }
      if (behavior == DENY_500){
        C.SendNow("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n");
        C.close();
        continue;
      }
      if (behavior == DENY_403){
        C.SendNow("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n");
        C.close();
        continue;
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
      case ACCEPT_THEN_200: C.SendNow("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"); break;
      case ACCEPT_THEN_403: C.SendNow("HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n"); break;
      case ACCEPT_THEN_500: C.SendNow("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n"); break;
      default: break; // ACCEPT_THEN_QUIET answers nothing
      }
      if (behavior == ACCEPT_THEN_QUIET){
        held.push_back(C);
        continue;
      }
      C.close();
    }
    for (size_t i = 0; i < held.size(); ++i){held[i].close();}
    srv.close();
  }

  bool startServer(const std::vector<ServerBehavior> &behaviors, std::thread &t, std::string &url){
    script = behaviors;
    served = 0;
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
