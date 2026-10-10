"""배경 데몬 스레드 — 버퍼에서 이벤트를 꺼내 API로 전송한다.

동작 원칙:
  - 전송은 daemon 백그라운드 스레드 1개가 담당한다.
  - HTTP 타임아웃은 connect 2s · read 3s (urllib 은 단일 timeout 만 받으므로 합산 5s 로 설정한다).
  - 전송에 실패하면 디스크 스풀에 JSONL 로 append 한 뒤 지수 백오프로 재시도한다.

설계 노트:
  - urllib 은 connect/read 를 분리 설정할 수 없다. 합산값(5s)을 timeout 으로 쓴다.
  - 이벤트 dict 의 _trace_id·_agent_id·_product_id 는 내부 필드다. API 요청 전 제거한다.
  - 응답의 per-event status 는 다음과 같이 처리한다.
      accepted / duplicated       → 스풀에서 제거(정상)
      SEQ_GAP / PREV_HASH_MISMATCH → 스풀 유지 · 재시도
      SEQ_CONFLICT / INVALID_EVENT → 스풀에서 제거(영구 실패)
  - 스풀 재시도는 _SPOOL_CHECK_INTERVAL(10s) 주기로 하며, 실패하면 지수 백오프로 대기한다.
  - api_key 는 로그에 원문을 출력하지 않는다.
"""
from __future__ import annotations

import json
import re
import logging
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from typing import Any

from . import buffer as _buffer
from . import config as _config
from . import signing as _signing
from . import spool as _spool

_logger = logging.getLogger("xsoft_comply")

_SPOOL_CHECK_INTERVAL: float = 10.0   # 스풀 정기 확인 주기(초)

# ★ 단일 타임아웃 상한(Q-07 = "단일 timeout 최대10초"). connect+read 합산이 이보다 크면 잘라낸다.
_MAX_SINGLE_TIMEOUT: float = 10.0

# ★ F-06 = "shutdown 기본 반환 ≤1초". `flush_interval` 을 고객이 크게(예: 60초) 잡으면
#   전송 루프가 `_buffer.get_batch(timeout=flush_interval)` 안에서 그만큼 오래 블로킹한다 —
#   `stop()` 이 `_stop_event` 를 세워도 이 블로킹 호출은 **다음 회전이 와야** 그 신호를 본다.
#   그래서 큐 대기는 실제 `flush_interval` 을 쓰되, **정지 신호를 확인하는 주기**만 짧게
#   잘라 폴링한다(배치 수집 의미는 그대로 — 큐가 비어 있으면 다음 회전에서 다시 잇는다).
_STOP_POLL_INTERVAL: float = 1.0


def _owner(cfg: "_config.Config") -> str:
    """현재 설정(키+엔드포인트)의 소유자 지문. 스풀 세그먼트 이름에 쓴다(GX-02)."""
    return _spool.owner_fingerprint(cfg.api_key, cfg.endpoint)


# ★ S-ACK-CAP(1001-540) + E-04(1001-540 추기1) = 부분 ACK 뒤 미확인 이벤트를 어딘가에
#   다시 보관해야 하는데, 그 자리가 (아직 지워지지 않은) 원본 claim 과 같은 예산을 다시
#   거치면(=`spool.append` 로 새 세그먼트를 시도하면) "원본 + 새 세그먼트" 가 한꺼번에
#   잡혀 가득 찬 스풀에서 영원히 거절된다. 그래서 **재시도 회전 중**(`_retry_spool` 이
#   `_send_batch` 를 부르는 동안)에는 `_spool_append` 가 디스크에 직접 쓰지 않고 이
#   싱크(`_retry_keep_sink`)에 모아 두기만 한다 — `_retry_spool` 이 회전 끝에서
#   `spool.merge_rewrite_claims()` 로 **한 번에** 원본 claim 들의 자리를 갈음한다
#   (예산 검사를 건너뛰므로 추가 용량이 0이다). 재시도 회전 밖(예: 버퍼에서 막 나온
#   첫 전송·atexit 드레인)에서는 평소대로 `spool.append` 로 새 세그먼트를 만든다.
#   단일 daemon 스레드만 이 값을 읽고 쓰므로(`_retry_attempt` 와 같은 전제) 잠금 없이
#   모듈 전역으로 판정한다.
_retry_keep_sink: list[dict[str, Any]] | None = None


def _spool_append(events: list[dict[str, Any]], cfg: "_config.Config") -> bool:
    """공통 스풀 append 래퍼.

    ★ 재시도 회전 중(`_retry_keep_sink is not None`)이면 디스크에 쓰지 않고 싱크에
      모은다 — 실제 기록은 `_retry_spool` 이 회전 끝에서 한 번에 한다(E-04). 그 밖에는
      평소대로 `spool.append` 로 새 세그먼트를 만든다.
    """
    if _retry_keep_sink is not None:
        if events:
            _retry_keep_sink.extend(events)
        return True
    return _spool.append(events, cfg.spool_dir, cfg.max_spool_bytes, owner=_owner(cfg))


# ── 리다이렉트 차단(S-18) ─────────────────────────────────────────────────────
#   urllib 의 기본 리다이렉트 처리기는 3xx 응답을 **원래 헤더(Authorization 포함)를 그대로
#   들고** 새 URL 로 따라간다. 우리 endpoint 가 공격자 제어 URL 로 리다이렉트하도록
#   조작되면(또는 오설정되면) API 키가 다른 origin 으로 유출된다. SDK 는 고정된 자기
#   endpoint 하나만 호출하므로 정상 운영에서 리다이렉트가 필요한 상황 자체가 없다 —
#   그래서 **따라가지 않는다**(3xx 는 그냥 HTTPError 로 떨어져 기존 실패 처리로 흡수된다).
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102
        return None


_opener = urllib.request.build_opener(_NoRedirect)


def _retry_after_seconds(headers: Any) -> float | None:
    """`Retry-After` 헤더(초 단위 정수 또는 HTTP-date)를 초로 해석한다. 없거나 못 읽으면 None."""
    try:
        value = headers.get("Retry-After") if headers is not None else None
        if not value:
            return None
        value = str(value).strip()
        if value.isdigit():
            return max(0.0, float(int(value)))
        # HTTP-date 형식 — 실패하면 무시(초 단위 형식이 절대 다수)
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(value)
        if dt is None:
            return None
        import datetime as _dt
        now = _dt.datetime.now(dt.tzinfo or _dt.timezone.utc)
        return max(0.0, (dt - now).total_seconds())
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return None

# User-Agent 는 반드시 xsoft_comply SDK 로 식별되는 값을 보낸다.
#   기본 UA(`Python-urllib/3.x`)는 당사 수신 서버의 자동화 트래픽 검사에서
#   403 으로 거부될 수 있다. transport 는 403 을 재시도해도 통과할 수 없는
#   오류로 보고 이벤트를 제외하므로, UA 식별값은 항상 포함한다.
#   버전은 하드코딩하지 않는다. 설치된 실제 버전(__version__)이 UA 에 따라가도록 한다.
#   지연 조회하는 이유 = __init__.py 는 이 모듈을 먼저 import 한 뒤에야
#     __version__ 을 정의한다. 모듈 최상단에서 읽으면 순환참조로 실패한다.
#     그래서 첫 전송 시점(=__init__ 로드가 끝난 뒤) 한 번만 읽어 캐시한다.
_USER_AGENT_CACHE: str | None = None


def _user_agent() -> str:
    """User-Agent = xsoft_comply-python/<실제 배포 버전>."""
    global _USER_AGENT_CACHE
    if _USER_AGENT_CACHE is None:
        try:
            from . import __version__ as _v
            _USER_AGENT_CACHE = f"xsoft_comply-python/{_v}"
        except BaseException:
            # 버전을 못 읽어도 UA 자체는 반드시 xsoft_comply SDK 로 식별돼야 한다(없으면 403 거부 가능).
            _USER_AGENT_CACHE = "xsoft_comply-python"
    return _USER_AGENT_CACHE

# 한 POST 에 담는 이벤트 수 상한.
#   API 규격은 한 번에 200건까지 받는다. 그러나 당사 수신 서버의 내부 처리 한도를
#   고려해, SDK 는 API 규격 상한이 아니라 안전 상한으로 나눠 보낸다.
#   (서버 수용 방식이 바뀌면 이 값을 올릴 수 있다.)
_MAX_EVENTS_PER_POST = 25

# ── 스레드 상태 ──────────────────────────────────────────────────────────────
# ★ M-23(1001-543) = `_stop_event` 는 **지금 세대(generation)** 의 중지 신호를 가리킨다.
#   `stop()` → 곧바로 `init()`(=`start()`) 이 다시 불리면, 옛 스레드가 느린 네트워크
#   호출에 블로킹돼 아직 못 끝났을 수 있다 — 그 상태에서 `start()` 가 **새 Event 객체**를
#   만들어 `_stop_event` 를 거기로 옮기면, 옛 스레드는 자신이 들고 있던(이미 set 된)
#   옛 Event 로 계속 루프를 돌다 블로킹이 풀리는 대로 스스로 끝난다 — 새 스레드는
#   새 Event 를 보므로 옛 스레드의 종료 신호에 영향받지 않는다(모듈 전역 하나를
#   공유했다면 "옛 스레드가 끝날 때까지 새 스레드도 못 뜬다" 또는 "새 스레드가 뜨자마자
#   같이 멈춘다" 둘 중 하나로 깨졌다).
_thread: threading.Thread | None = None
_overflow_thread: threading.Thread | None = None
_stop_event = threading.Event()

# ── M-13 = http:// 는 localhost 전용 ──────────────────────────────────────────
#   그 밖의 http:// 평문 엔드포인트로는 API 키·감사기록이 그대로 네트워크에 실려
#   나간다 — 전송하지 않고 스풀에 보관한 뒤 1회만 경고한다.
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
_insecure_http_warned = False
_insecure_http_lock = threading.Lock()


def _is_endpoint_locally_safe(endpoint: str) -> bool:
    try:
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme != "http":
            return True  # https 등은 이 규칙의 대상이 아니다
        host = (parsed.hostname or "").lower()
        return host in _LOCAL_HOSTS
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return False  # 파싱 실패 = 안전하지 않은 쪽으로(전송하지 않는다)


def _warn_insecure_http_once(endpoint: str) -> None:
    global _insecure_http_warned
    with _insecure_http_lock:
        if _insecure_http_warned:
            return
        _insecure_http_warned = True
    try:
        _logger.warning(
            "xsoft_comply: endpoint(%s)가 http:// 이고 localhost/127.0.0.1/::1 이 아니어서 "
            "전송하지 않고 스풀에 보관합니다(평문 전송 위험). localhost 전용 테스트가 "
            "아니라면 https:// 를 쓰세요.", _safe(endpoint, 200))
    except BaseException:
        pass

# 스풀 재시도 상태 (transport 스레드 단독 접근 → 잠금 불필요)
_retry_attempt: int = 0

# «지금 전송 중인 묶음» = 프로세스 종료 시점과 겹치면 누락될 수 있는 구간을 막기 위한 참조.
#   큐에서 꺼낸 순간부터 _send_batch 가 끝날 때까지 여기 담아 둔다.
#   프로세스 종료 시 flush 가 시간 안에 끝나지 못하면 데몬 스레드는 그대로 중단되는데,
#   그때 이 묶음은 대기열에도 임시 보관에도 없어 알림 없이 누락된다 → atexit 이 이 묶음을 임시 보관으로 내린다.
#   (이미 저장에 성공한 뒤였다면 재전송에서 duplicated 로 흡수되어 중복 적재는 발생하지 않는다.)
_inflight_batch: list[dict[str, Any]] | None = None
_next_spool_check: float = 0.0   # monotonic 기준 다음 스풀 확인 시각

# ── in-flight 카운터 ─────────────────────────────────────────────────────────
# 큐에서 꺼냈지만 전송 완료(성공 or 스풀 기록) 전인 배치 수.
# flush_buffer 는 "큐 비었고 AND _inflight==0" 이어야 진짜 비었다고 판단한다.
_inflight: int = 0
_inflight_lock = threading.Lock()
_inflight_cond = threading.Condition(_inflight_lock)


# ── 공개 제어 API ─────────────────────────────────────────────────────────────

def start() -> None:
    """데몬 스레드를 시작한다. 이미 살아있고 멈춤 신호가 없으면 no-op.

    ★ M-23 = 직전 `stop()` 이 아직 진행 중(옛 스레드가 느린 네트워크 호출에
      블로킹돼 있는 등)이어도 다시 뜬다 — 새 세대 Event 를 만들어 새 스레드에
      준다(위 모듈 docstring).
    """
    global _thread, _overflow_thread, _stop_event, _next_spool_check
    if _thread is not None and _thread.is_alive() and not _stop_event.is_set():
        return
    my_stop = threading.Event()
    _stop_event = my_stop
    _next_spool_check = time.monotonic() + _SPOOL_CHECK_INTERVAL
    t = threading.Thread(
        target=_run,
        args=(my_stop,),
        name="xsoft_comply_transport",
        daemon=True,   # 고객 프로세스 종료를 막지 않는다
    )
    t.start()
    _thread = t
    # ★ M-22 = 전송 스레드가 느린 서버 응답을 기다리는 동안에도 대기열이 절반을
    #   넘기면 곧바로 스풀에 내리는 보조 스레드(별도 — 메인 전송 스레드는 네트워크
    #   호출 중에 블로킹되어 있어 자기 자신은 이 역할을 할 수 없다).
    ot = threading.Thread(
        target=_overflow_guard_loop,
        args=(my_stop,),
        name="xsoft_comply_overflow_guard",
        daemon=True,
    )
    ot.start()
    _overflow_thread = ot


def stop(timeout: float = 2.0) -> None:
    """지금 세대 스레드에 종료 신호를 보내고 최대 timeout초 대기한다."""
    _stop_event.set()
    t = _thread
    if t is not None:
        t.join(timeout=timeout)
    ot = _overflow_thread
    if ot is not None:
        ot.join(timeout=min(timeout, 1.0))


def _overflow_guard_loop(stop_evt: threading.Event) -> None:
    """대기열이 절반 넘게 찼으면 곧바로 스풀에 내린다(M-22).

    ★ 왜 별도 스레드인가 = 메인 전송 스레드는 느린(또는 장애) 서버 응답을 기다리는
      동안 네트워크 호출에 블로킹되어 큐를 비울 수 없다. 그 사이 호출 스레드가 계속
      이벤트를 넣으면 `max_queue` 를 넘겨 **신규 이벤트가 거절(dropped)된다** —
      사용자 코드는 멀쩡히 호출했는데 알림 없이 사라지는 셈이다. 이 스레드는 그 창을
      메우려고 절반 넘게 찬 몫을 선제적으로(네트워크를 기다리지 않고) 스풀에 내려
      대기열에 다시 여유를 만든다. 스풀에 내린 이벤트는 다음 스풀 재시도 주기에
      정상적으로 재전송된다 — 유실이 아니라 «우회 경로로 보관» 일 뿐이다.
    ★ `_spool_append` 가 아니라 `_spool.append` 를 직접 쓴다 = 재시도 회전 중에만
      의미가 있는 `_retry_keep_sink` 최적화는 이 스레드가 끼어들 자리가 아니다
      (단일 전송 스레드 전용 가정을 깨지 않는다).
    """
    while not stop_evt.is_set():
        try:
            cfg = _config.get()
            if not cfg.dry_run:
                half = max(1, cfg.max_queue // 2)
                size = _buffer.size()
                if size > half:
                    excess = size - (half // 2 or 1)
                    drained = _buffer.drain_up_to(excess)
                    if drained:
                        _spool.append(drained, cfg.spool_dir, cfg.max_spool_bytes, owner=_owner(cfg))
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass
        stop_evt.wait(0.02)


def flush_buffer(timeout: float = 5.0) -> None:
    """큐가 비고 in-flight 배치가 0이 될 때까지 대기한다. 최대 timeout초 후 반환.

    완료 조건: _buffer.size()==0 AND _inflight==0.
    큐가 비어도 배치를 이미 꺼내 전송 중인 경우 in-flight 가 남아있으므로
    실제 전송 완료까지 대기한다(0.15초 조기반환 불가).

    atexit 핸들러가 5s timeout 으로 호출한다.
    timeout 경과 후 잔여 항목은 호출자(atexit)가 스풀로 내린다.
    """
    deadline = time.monotonic() + timeout
    with _inflight_cond:
        while True:
            if _buffer.size() == 0 and _inflight == 0:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return  # timeout — 호출자가 잔여 처리 담당
            _inflight_cond.wait(timeout=min(0.1, remaining))


# ── 메인 루프 ─────────────────────────────────────────────────────────────────


# 전송 루프에서 예외를 전달하지 않고 처리한 오류 수. 0 이 아니면 확인이 필요하다는 신호다.
#   (세지 않으면 「중단되지는 않지만 아무 일도 되지 않는」 상태를 알아챌 방법이 없다.)
_loop_errors: int = 0
_loop_err_lock = threading.Lock()
_next_loop_warn: int = 1


def _count_loop_error() -> None:
    global _loop_errors, _next_loop_warn
    with _loop_err_lock:
        _loop_errors += 1
        total = _loop_errors
        warn = total >= _next_loop_warn
        if warn:
            _next_loop_warn = max(_next_loop_warn * 10, 1)
    if warn:
        try:
            _logger.warning(
                "xsoft_comply: 전송 루프에서 예상 밖 오류를 처리했습니다 — 누적 %d회 "
                "(전송 스레드는 계속 살아 있습니다)", total, exc_info=True)
        except BaseException:
            pass


def loop_errors() -> int:
    with _loop_err_lock:
        return _loop_errors


def _run(stop_evt: threading.Event) -> None:
    """데몬 스레드 메인 루프.

    1. 버퍼에서 배치를 꺼내 전송한다.
    2. 스풀 재시도 주기가 되면 스풀에서 읽어 재전송한다.
    3. stop_evt 가 설정되면 루프를 빠져나와 잔여 버퍼를 드레인한다.

    ★ M-23 = 이 스레드가 들고 있는 `stop_evt` 는 **자신이 태어날 때 받은 그 객체뿐**이다
      (모듈 전역 `_stop_event` 를 매번 다시 참조하지 않는다) — `start()` 가 재시작하며
      전역을 새 Event 로 바꿔도 이 스레드의 종료 조건에는 영향이 없다.
    """
    global _next_spool_check, _inflight

    while not stop_evt.is_set():
        # 이 루프는 예외로 빠져나가면 안 된다.
        #   예상하지 못한 예외 하나로 전송 스레드가 중단되면 그때부터 모든 감사기록이
        #   전송되지 못한다. 한 회전이 실패해도 다음 회전은 실행되어야 하므로
        #   여기서 예외를 전달하지 않고 처리한다.
        try:
            _run_once(stop_evt)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            _count_loop_error()

    cfg = _config.get()
    # 종료 전 잔여 대기열 → 임시 보관으로 내린다(빠른 로컬 I/O, 네트워크 재시도 없음).
    # 장애 시 디스크에 보존한다.
    remaining = _buffer.drain()
    if remaining:
        _logger.debug("xsoft_comply: 종료 시 미전송 %d건 스풀에 기록", len(remaining))
        _spool_append(remaining, cfg)



def _run_once(stop_evt: threading.Event) -> None:
    """전송 루프 한 회전. 예외는 호출자(_run)가 전달하지 않고 처리한다."""
    global _next_spool_check, _inflight, _inflight_batch

    # ★ 설정은 매 회전마다 다시 읽는다.
    #   한 번만 읽어두면 `init()` 재호출(엔드포인트·서명·토글 변경)이 전송 스레드에 반영되지 않는다
    #   = 변경한 설정이 반영되지 않는 상태. 읽기 비용은 전역 참조 1회다.
    cfg = _config.get()

    # ★ M-05 = dry_run 은 **네트워크·스풀을 건드리지 않는다.** 배치 전송은 물론
    #   스풀 재시도(dry_run 이전에 쌓여 있던 세그먼트 포함)도 쉰다 — "전송 전 확인"이
    #   실제로는 과거에 쌓인 기록을 내보내 버리면 "아무것도 안 나간다"는 약속이 깨진다.
    #   (dry_run 중에는 _emit() 이 애초에 버퍼에 넣지 않으므로 배치는 항상 비어 있다 —
    #   여기서 쉬는 것은 주로 _retry_spool 의 의도치 않은 전송을 막기 위해서다.)
    if cfg.dry_run:
        stop_evt.wait(min(cfg.flush_interval, _STOP_POLL_INTERVAL))
        return

    # 버퍼에서 배치 꺼내기 (flush_interval 동안 블로킹 대기)
    # ★ F-06 = 정지 신호를 놓치지 않도록 대기를 _STOP_POLL_INTERVAL 단위로 자른다.
    poll_timeout = min(cfg.flush_interval, _STOP_POLL_INTERVAL)
    batch = _buffer.get_batch(max_items=200, timeout=poll_timeout)
    if batch:
        # in-flight +1: 큐에서 꺼낸 순간부터 전송 완료(or 스풀 기록)까지 카운트
        with _inflight_cond:
            _inflight += 1
            _inflight_batch = batch
        try:
            _send_batch(batch, cfg)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            # 전송이 예상하지 못한 이유로 실패했다 = 그래도 기록은 보존한다.
            #   임시 보관으로 내려두면 다음 회전 또는 resend 가 회수한다.
            _count_loop_error()
            try:
                _spool_append(batch, cfg)
            except BaseException:
                pass
        finally:
            # 전송 성공 or 스풀 기록 완료 시 -1 후 flush_buffer 에 알림
            with _inflight_cond:
                _inflight -= 1
                _inflight_batch = None
                _inflight_cond.notify_all()

    # 스풀 정기 확인
    now = time.monotonic()
    if now >= _next_spool_check:
        _retry_spool(cfg)
        # S-19 = 계수 영속화도 같은 주기로(호출 스레드가 아니라 이 전송 스레드에서만 디스크에 쓴다).
        try:
            _buffer.persist_counters(cfg.spool_dir)
        except BaseException:
            pass
        # 다음 확인 예약 (성공 시 초기화는 _retry_spool에서 함)
        if _next_spool_check <= now:
            _next_spool_check = time.monotonic() + _SPOOL_CHECK_INTERVAL


def spool_inflight(cfg: "_config.Config | None" = None) -> int:
    """종료 시점에 «전송 중» 이던 묶음을 임시 보관으로 내린다.

    왜 필요한가
      atexit flush 가 제한시간 안에 끝나지 못하면 백그라운드 전송 스레드는 그대로 중단된다.
      그 순간 «대기열에서 꺼내 전송 중이던» 묶음은 대기열에도 임시 보관에도 없어 누락될 수 있다.
    중복 적재는 발생하지 않는다 = 이미 저장된 이벤트는 재전송에서 duplicated 로 흡수된다.
    """
    global _inflight_batch
    try:
        with _inflight_cond:
            batch = _inflight_batch
            _inflight_batch = None
        if not batch:
            return 0
        conf = cfg or _config.get()
        _spool_append(batch, conf)
        _logger.debug("xsoft_comply: 종료 시 전송중 %d건 스풀에 기록", len(batch))
        return len(batch)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return 0


# ── 배치 처리 ─────────────────────────────────────────────────────────────────

def _group_by_trace(
    events: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """이벤트 목록을 trace_id별로 그루핑한다."""
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for evt in events:
        tid = evt.get("_trace_id") or "unknown"
        groups[tid].append(evt)
    return groups


def _strip_internal(evt: dict[str, Any]) -> dict[str, Any]:
    """내부 필드(_로 시작)를 제거하고 API 전송용 dict를 반환한다."""
    return {k: v for k, v in evt.items() if not k.startswith("_")}


def _prepare(evt: dict[str, Any], cfg: "_config.Config") -> None:
    """전송 직전 가공 = 전자서명. **제자리** 수정한다.

    왜 여기(전송 스레드)인가
      서명(ed25519 ≈ 2~6ms)은 호출 스레드가 하면 안 된다(영향 최소화 규칙).
    왜 제자리인가
      전송 실패 시 스풀에 남는 것도 «가공이 끝난» 이벤트여야 재전송이 같은 값을 보낸다.
    """
    try:
        meta = evt.get("metadata")
        already_signed = isinstance(meta, dict) and "_sig" in meta
        if cfg.sign and not already_signed:
            _signing.sign_event(evt)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass  # 가공 실패가 전송을 막지 않는다


def _send_batch(events: list[dict[str, Any]], cfg: "_config.Config") -> None:
    """이벤트 배치를 trace_id별로 묶어 각각 POST한다."""
    groups = _group_by_trace(events)
    for trace_id, trace_events in groups.items():
        # seq 오름차순 정렬 (버퍼는 FIFO지만 멀티스레드 환경에서 역전 가능)
        trace_events.sort(key=lambda e: e.get("seq", 0))

        # 서명 체인은 seq 순서를 따른다 → 정렬 뒤에 가공한다.
        for evt in trace_events:
            _prepare(evt, cfg)

        agent_id = trace_events[0].get("_agent_id") if trace_events else None

        # 안전 상한으로 쪼개 보낸다(근거는 위 _MAX_EVENTS_PER_POST 주석 참고).
        #   seq 순서대로 잘라야 뒷조각이 SEQ_GAP 을 맞지 않는다.
        for start in range(0, len(trace_events), _MAX_EVENTS_PER_POST):
            slice_events = trace_events[start:start + _MAX_EVENTS_PER_POST]
            _post_one(trace_id, agent_id, slice_events, cfg)


# ★ ACK 계약 버전(bundle ⑤). 이 SDK 는 자신이 이해하는 ACK 의미의 버전을 요청에 싣는다.
#   서버는 이 값이 없거나 낮은 요청에는 **1.0.1 이 이미 이해하는 의미만** 돌려주고,
#   새 상태·코드(예: 장래의 retryable)는 이 값을 선언한 클라이언트에만 보낸다.
#   지금 이 판(1.0.2)에는 새 상태를 아직 추가하지 않는다 — 여기서 만드는 것은
#   "다음에 추가할 때 1.0.1 을 깨지 않는" 협상 통로 자체다.
_ACK_VERSION = 2


def _build_body(
    trace_id: str, agent_id: str | None, slice_events: list[dict[str, Any]]
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "trace_id": trace_id,
        "events": [_strip_internal(e) for e in slice_events],
        "ack_version": _ACK_VERSION,
    }
    if agent_id:
        body["agent_id"] = agent_id
    # product_id는 현재 API spec에 없어 메타에만 기록 — 미포함
    return body


# ── 응답 본문 상한 · 로그 중화 ───────────────────────────────────────────

# 압축폭탄·거대응답 방어.
#   서버는 당사가 고른 주소지만, 고객 망 안에서 프록시가 가로채거나 엔드포인트가
#   잘못 설정되면 응답이 SDK 프로세스의 메모리를 과다 소비하는 통로가 될 수 있다.
#   resp.read() 는 상한이 없다 = 100 MB 를 보내면 100 MB 를 받는다.
#   서버 응답은 이벤트 200건 결과라 수백 KB 를 넘을 이유가 없으므로 1 MB 에서 제한한다.
#   Accept-Encoding: identity = 압축을 받지 않는다. 「압축 크기만 보고
#     통과시켰다가 풀면 거대해진다」는 압축폭탄의 전제를 없앤다.
_MAX_RESP_BYTES = 1 * 1024 * 1024


def _read_capped(resp: Any) -> str:
    """응답 본문을 상한까지만 읽는다. 넘치면 잘라서 «JSON 아님» 으로 흘려보낸다."""
    raw = resp.read(_MAX_RESP_BYTES + 1)
    if len(raw) > _MAX_RESP_BYTES:
        _logger.warning("xsoft_comply: 응답 본문이 %d bytes 를 넘어 잘랐습니다", _MAX_RESP_BYTES)
        raw = raw[:_MAX_RESP_BYTES]
    return raw.decode("utf-8", errors="replace")


# log injection 방지.
#   아래 값들은 서버 응답에서 온 문자열(event_id·message)이다 = SDK 가 만든 값이 아니다.
#   CR/LF 가 섞이면 고객의 로그 파일에 «가짜 로그 한 줄» 이 추가될 수 있다(감사기록 위조 위험).
#   개행·제어문자를 지우고 길이를 자른 뒤에만 로그에 싣는다.
_CTRL = re.compile(r"[\x00-\x1f\x7f\u2028\u2029]")


def _safe(value: Any, limit: int = 120) -> str:
    text = _CTRL.sub("_", str(value))
    return text if len(text) <= limit else text[:limit] + "…"


# ── HTTP POST ─────────────────────────────────────────────────────────────────

def _post_one(
    trace_id: str,
    agent_id: str | None,
    original: list[dict[str, Any]],
    cfg: "_config.Config",
) -> None:
    """단일 trace 묶음(슬라이스)을 POST. 실패 패턴에 따라 임시 보관·격리·분할 재시도.

    api_key는 헤더에만 사용. 로그에 원문 출력 안 함.
    urllib은 connect/read timeout을 합산값으로만 지원한다(상한 = Q-07 10초).
    ★ S-18 = 리다이렉트를 따라가지 않는 `_opener` 를 쓴다(모듈 상단 정의).
    ★ M-13 = `http://` 는 localhost/127.0.0.1/::1 로만 보낸다 — 그 밖의 평문 엔드포인트는
      API 키가 네트워크에 그대로 노출되므로 전송하지 않고 스풀에 보관한다(경고 1회).
    """
    if not _is_endpoint_locally_safe(cfg.endpoint):
        _warn_insecure_http_once(cfg.endpoint)
        _spool_append(original, cfg)
        return
    body = _build_body(trace_id, agent_id, original)
    try:
        data = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
        url = f"{cfg.endpoint}/events"
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {cfg.api_key}",  # 원문은 헤더에만
                "User-Agent": _user_agent(),               # ★ 없으면 403 거부 가능 (위 주석)
                "Accept-Encoding": "identity",              # ★ 압축 응답을 받지 않는다(압축폭탄 차단)
            },
            method="POST",
        )
        total_timeout = min(cfg.timeout[0] + cfg.timeout[1], _MAX_SINGLE_TIMEOUT)

        with _opener.open(req, timeout=total_timeout) as resp:
            status_code: int = resp.status
            resp_text = _read_capped(resp)

        try:
            resp_body = json.loads(resp_text)
        except json.JSONDecodeError:
            resp_body = {}

        _handle_response(status_code, resp_body, original, cfg)

    except urllib.error.HTTPError as exc:
        status_code = exc.code
        try:
            err_body = json.loads(_read_capped(exc))
        except Exception:
            err_body = {}
        _handle_http_error(status_code, err_body, exc.headers, trace_id, agent_id, original, cfg)

    except (KeyboardInterrupt, SystemExit):
        raise

    except BaseException:
        # 연결거부 · 타임아웃 · OSError 등 → 스풀에 보관하고 백오프
        _logger.debug("xsoft_comply: 전송 실패 — 스풀에 저장 후 재시도")
        _spool_append(original, cfg)
        _bump_backoff(cfg)


def _handle_http_error(
    status_code: int,
    body: dict[str, Any],
    headers: Any,
    trace_id: str,
    agent_id: str | None,
    original: list[dict[str, Any]],
    cfg: "_config.Config",
) -> None:
    """4xx/5xx HTTP 에러 처리.

    ★ Q-06 계약대로 갈래를 나눈다 =
        402·401·403(키 폐기·교체)·429·5xx·timeout → **보관**(재시도)
        400 형식 오류만 → **격리**(삭제 아님)
        413 → **분할** 후 재시도(슬롯 손실 없이)
    """
    error_code = ""
    if isinstance(body, dict):
        err = body.get("error")
        if isinstance(err, dict):
            error_code = str(err.get("code", ""))

    if status_code == 403 and error_code == "PLAN_REQUIRED":
        # 요금제 거절은 영구 실패로 제외하지 않는다.
        #   ⓐ 요금제 문제라면 = 업그레이드 뒤 resend 로 회수할 수 있어야 한다.
        #   ⓑ 서버가 일시적으로 요금제를 잘못 읽는 경우에도
        #      감사기록이 알림 없이 누락되면 안 된다.
        #   스풀은 max_spool_bytes 로 상한이 있으므로 무한히 불어나지 않는다.
        _logger.warning("xsoft_comply: HTTP 403 PLAN_REQUIRED — 스풀 보관(업그레이드 후 회수)")
        _spool_append(original, cfg)
        _bump_backoff(cfg, force_max=True)

    elif status_code == 402:
        # ★ GX-01 = 402 PAYMENT_REQUIRED(미납) — 서버는 "보관"이라고 알린다. 삭제하면
        #   서버 설명과 반대로 SDK 가 유실을 만든다. 결제 회복 후 resend 로 회수해야 한다.
        _logger.warning("xsoft_comply: HTTP 402 PAYMENT_REQUIRED — 스풀 보관(결제 회복 후 회수)")
        _spool_append(original, cfg)
        _bump_backoff(cfg, force_max=True)

    elif status_code in (401, 403):
        # ★ S-13 = 키 폐기·교체 응답. 예전에는 400·413 과 함께 묶어 **제외**했다 = 키를
        #   바꾸는 순간(짧게는 몇 초) 그 창에 걸린 이벤트가 영구 유실됐다. 키 문제는
        #   "그 키로는 안 된다"는 뜻이지 "이 이벤트가 잘못됐다"는 뜻이 아니므로 보관한다.
        #   (403 PLAN_REQUIRED 는 위에서 이미 갈랐다 — 여기 남는 403 은 인증 실패뿐이다.)
        _logger.warning("xsoft_comply: HTTP %d — 스풀 보관(키 복구 후 회수)", status_code)
        _spool_append(original, cfg)
        _bump_backoff(cfg, force_max=True)

    elif status_code == 400:
        # ★ Q-06 = 형식 오류만 여기 남는다(인증·요금제·미납은 위에서 이미 갈랐다).
        #   재시도해도 통과하지 못하므로 **격리**한다(스풀 회수 대상에서는 빠지되, 삭제는 아니다).
        _buffer.count_failed(len(original))
        _spool.quarantine(original, cfg.spool_dir, reason=f"HTTP 400 {error_code or '형식 오류'}")
        _logger.warning(
            "xsoft_comply: HTTP 400 — 이벤트 %d건 격리(재시도 불가 형식 오류)", len(original))

    elif status_code == 413:
        # ★ Q-06 = "413 분할 후 슬롯손실0". 절반으로 쪼개 각각 재시도한다.
        #   1건짜리 슬라이스가 그래도 413 이면 그 이벤트 자체가 너무 크다는 뜻이므로 격리한다.
        if len(original) > 1:
            mid = len(original) // 2
            _logger.warning(
                "xsoft_comply: HTTP 413 — %d건을 %d/%d 로 분할 재시도",
                len(original), mid, len(original) - mid)
            _post_one(trace_id, agent_id, original[:mid], cfg)
            _post_one(trace_id, agent_id, original[mid:], cfg)
        else:
            _buffer.count_failed(len(original))
            _spool.quarantine(original, cfg.spool_dir, reason="HTTP 413 단일 이벤트 초과")
            _logger.warning("xsoft_comply: HTTP 413 — 단일 이벤트가 너무 커 격리")

    elif status_code == 409:
        # 409 = «배치 전건 거부» 다. 본문에 per-event results[] 와 next_seq 가 그대로 들어 있다.
        #   (장애 뒤 재전송이 순번 누락을 만들면 409 + 전건 SEQ_GAP 이 온다)
        #   200/207 과 같은 per-event 규칙으로 처리한다 = SEQ_GAP 은 임시 보관 유지·재시도.
        _logger.warning("xsoft_comply: HTTP 409 — per-event 결과로 처리(스풀 유지)")
        _handle_response(status_code, body, original, cfg)
        _bump_backoff(cfg)

    elif status_code == 429:
        # 한도 초과 → 스풀에 보관하되 재시도 주기를 최대 백오프로.
        # ★ Q-07 = 서버가 Retry-After 를 주면 그보다 일찍 재시도하지 않는다.
        _logger.warning("xsoft_comply: HTTP 429 QUOTA_EXCEEDED — 스풀 보관")
        _spool_append(original, cfg)
        _bump_backoff(cfg, force_max=True, retry_after=_retry_after_seconds(headers))

    elif status_code >= 500:
        # 서버 에러 → 스풀 보관·재시도
        _logger.warning("xsoft_comply: HTTP %d — 스풀에 저장", status_code)
        _spool_append(original, cfg)
        _bump_backoff(cfg, retry_after=_retry_after_seconds(headers))

    else:
        # 예상 외 코드 → 제외하되 격리한다(그냥 사라지지 않는다). 건수도 그대로 센다.
        _buffer.count_failed(len(original))
        _spool.quarantine(original, cfg.spool_dir, reason=f"HTTP {status_code} 예상 외 코드")
        _logger.warning(
            "xsoft_comply: HTTP %d — 예상 외 코드, %d건 격리", status_code, len(original))


def _handle_response(
    status_code: int,
    resp_body: dict[str, Any],
    original: list[dict[str, Any]],
    cfg: "_config.Config",
) -> None:
    """서버 응답의 per-event status 표에 따라 처리한다.

    | status          | code                  | SDK 동작              |
    |-----------------|----------------------|-----------------------|
    | accepted        | -                    | 스풀에서 제거          |
    | duplicated      | -                    | 스풀에서 제거          |
    | rejected        | SEQ_GAP              | 스풀 유지 · 재시도     |
    | rejected        | PREV_HASH_MISMATCH   | 스풀 유지 · 재시도     |
    | rejected        | SEQ_CONFLICT         | 임시 보관에서 제거 + 경고 |
    | rejected        | INVALID_EVENT        | 스풀에서 제거          |
    | (모르는 status) | -                    | 스풀 유지 · 재시도(E-03 · 1001-540) |
    """
    global _retry_attempt

    data = resp_body.get("data", {}) if isinstance(resp_body, dict) else {}
    results: list[dict[str, Any]] = []
    if isinstance(data, dict):
        results = data.get("results", [])

    if not results:
        # ★ Q-02 = "불명확 ACK 는 보존한다" — 결과 목록이 없다고 «전체 성공»으로
        #   넘겨짚지 않는다(예전 동작). 잘린 JSON·빈 본문처럼 HTTP 는 2xx 인데
        #   본문이 불완전한 경우 서버가 실제로 무엇을 저장했는지 알 수 없다 —
        #   그 상태로 큐에서 지우면 알림 없는 유실이 될 수 있다. 보관해 재시도하면
        #   서버가 이미 저장한 이벤트는 재전송에서 event_id 기준 duplicated 로
        #   흡수되어 중복 적재가 나지 않는다(이미 다른 경로로 검증된 계약).
        _spool_append(original, cfg)
        _bump_backoff(cfg)
        return

    # event_id → 원본 이벤트 매핑
    id_map: dict[str, dict[str, Any]] = {}
    for evt in original:
        eid = evt.get("event_id")
        if eid:
            id_map[eid] = evt

    keep: list[dict[str, Any]] = []
    addressed: set[str] = set()

    for r in results:
        status = r.get("status", "")
        code = r.get("code", "")
        eid = r.get("event_id", "")
        if eid:
            addressed.add(eid)
        evt = id_map.get(eid)

        if status in ("accepted", "duplicated"):
            # 스풀에서 제거 (아무 것도 안 하면 됨)
            _retry_attempt = 0

        elif status == "rejected":
            if code in ("SEQ_GAP", "PREV_HASH_MISMATCH"):
                # 스풀 유지 · 다음 배치에서 재시도
                if evt:
                    keep.append(evt)
            elif code == "SEQ_CONFLICT":
                # 체인 분기 — 제외 + 경고. ★ 삭제가 아니라 격리(감사 대조용으로 남긴다).
                _logger.warning(
                    "xsoft_comply: SEQ_CONFLICT (체인 분기) event_id=%s seq=%s",
                    _safe(eid), _safe(r.get("seq"), 20),
                )
                # ★ M-04(1001-543) = 서버가 이벤트별로 거절(격리)한 건도 failed_events() 에 센다.
                _buffer.count_failed(1)
                if evt:
                    _spool.quarantine([evt], cfg.spool_dir, reason="SEQ_CONFLICT 체인 분기")
            elif code == "INVALID_EVENT":
                # INVALID_EVENT 는 두 가지가 섞여 온다.
                #   ① 형식 오류(타입·metadata 크기) = 다시 보내도 같은 결과이므로 격리한다(삭제 아님).
                #   ② 서버 저장 실패("저장 오류: …")
                #      = 이벤트 잘못이 아니라 그때의 서버 사정이다. 제외하면 알림 없는 누락이 된다.
                #   메시지로 갈라서 ②만 보관·재시도한다.
                message = str(r.get("message", ""))
                if message.startswith("저장 오류"):
                    _logger.warning("xsoft_comply: 서버 저장 실패 — 보관 후 재시도 event_id=%s",
                                    _safe(eid))
                    if evt:
                        keep.append(evt)
                else:
                    _logger.debug("xsoft_comply: INVALID_EVENT 격리 event_id=%s", _safe(eid))
                    # ★ M-04 = 형식 오류로 격리된 건도 failed_events() 에 센다(서버 거절이다).
                    _buffer.count_failed(1)
                    if evt:
                        _spool.quarantine([evt], cfg.spool_dir, reason=f"INVALID_EVENT {message}"[:200])
            else:
                # 알 수 없는 code → 격리(삭제 아님)
                # ★ M-04 = 모르는 code 도 서버가 명시적으로 거절한 것이므로 센다.
                _buffer.count_failed(1)
                if evt:
                    _spool.quarantine([evt], cfg.spool_dir, reason=f"알 수 없는 code={code}")

        else:
            # ★ E-03(1001-540) = 모르는 status(accepted·duplicated·rejected 밖) 는
            #   "말없이 버리지 않는다" — 다음 판이 새 status 를 추가했을 수 있다(ACK
            #   계약 협상 통로 _ACK_VERSION 참고). 뜻을 모르면 안전한 쪽(재시도)으로
            #   보관한다 — rejected 안의 "알 수 없는 code"(위)는 서버가 명시적으로
            #   거절했다는 뜻이라 격리하지만, status 자체가 낯설면 거절인지 수락인지도
            #   모르므로 quarantine 이 아니라 keep 이다.
            _logger.warning(
                "xsoft_comply: 알 수 없는 status=%s event_id=%s — 보관 후 재시도",
                _safe(status, 40), _safe(eid),
            )
            if evt:
                keep.append(evt)

    # ★ S-06/Q-02(0930-539) = "결과가 없는 건은 전부 재보관(모르면 보관 쪽)".
    #   예전에는 이 루프가 `results` 표에 있는 항목만 처리했다 — 요청 3건에 서버가
    #   결과를 1건만 돌려주면(부분 응답) 나머지 2건은 addressed 되지 않은 채 그냥
    #   버려졌다(스풀은 이미 claim 돼 있어 재-append 하지 않으면 사라진다). 결과
    #   표에 아예 없는(=서버가 어떻게 했는지 모르는) 이벤트는 전부 보수적으로
    #   재보관한다 — 서버가 실제로 이미 저장했더라도 재전송 시 event_id 로
    #   duplicated 흡수되어 안전하다(위 no-results 분기와 같은 원칙).
    for evt in original:
        eid = evt.get("event_id")
        if eid and eid not in addressed:
            keep.append(evt)

    if keep:
        _spool_append(keep, cfg)
        _bump_backoff(cfg)


# ── 스풀 재시도 ────────────────────────────────────────────────────────────────

# ── 옛 소유자(키 교체) 경고(E-06 · 1001-540) ─────────────────────────────────
_next_other_owner_warn: int = 1


def _maybe_warn_other_owner(cfg: "_config.Config") -> None:
    """지금 키가 아닌 다른 키로 쌓인 스풀 파일이 있으면 지수 간격으로 경고한다.

    ★ E-06 = 키 교체(KEY-02 유예) 뒤 옛 키 세그먼트는 GX-02 로 영원히 자동 회수되지
      않는다 — 말없이 디스크만 차지하면 아무도 모른다. 지우거나 건드리지 않고 세기만
      한다(`spool.other_owner_pending`).
    """
    global _next_other_owner_warn
    other = _spool.other_owner_pending(cfg.spool_dir, _owner(cfg))
    if other <= 0:
        return
    if other >= _next_other_owner_warn:
        _next_other_owner_warn = max(_next_other_owner_warn * 10, 1)
        try:
            # ★ M-09(1001-543) = resend 의 "다른 소유자 포함" 옵션은 더 이상 "다른 키
            #   기록을 지금 키로 보낸다"는 뜻이 아니다(그 키로 보내면 유출·오귀속이다)
            #   — 그 키의 환경에서 다시 실행하라고 안내한다.
            _logger.warning(
                "xsoft_comply: 스풀에 다른 키(옛 소유자)로 쌓인 파일이 %d개 있습니다 — "
                "이 SDK 는 지금 키로만 자동 재전송합니다. 그 키(옛 소유자)의 환경에서 "
                "python -m xsoft_comply.resend 를 그 키로 다시 실행해 회수하세요.", other)
        except BaseException:
            pass


def _retry_spool(cfg: "_config.Config") -> None:
    """스풀에 저장된 이벤트를 읽어 재전송한다.

    ★ S-04/Q-03(0930-539) = "ACK 받은 뒤에만 디스크에서 지운다". 예전에는
      `read_and_clear` 로 먼저 지우고 나서 전송을 시도했다 — 전송 도중(응답을 받기
      전) 프로세스가 강제 종료되면(kill -9·OOM 등, try/except 로 못 잡는다) 그
      이벤트는 디스크에도 메모리에도 없이 사라졌다. 지금은 `claim_all` 로 claim
      만 하고(디스크에는 `.claim` 파일로 그대로 남는다) `_send_batch` 가 온전히
      끝난 뒤에만 원본 claim 자리를 정리한다. 그 사이 죽으면 claim 파일이 그대로
      남아 다음 재시작의 `claim_all` 이 복구한다(중복 재전송 가능성은 있어도
      event_id 로 멱등 처리되어 유실보다 안전하다 — "모르면 보관 쪽").

    ★ S-ACK-CAP(1001-540) + E-04(1001-540 추기1) = `_send_batch` 가 처리하며 "재시도가
      필요하다"고 판정한 이벤트는 `_spool_append` 를 거쳐 `_retry_keep_sink` 에
      **메모리로만** 모인다(디스크에 즉시 쓰지 않는다) — 회전이 끝난 뒤
      `spool.merge_rewrite_claims(claimed_paths, keep)` 가 한 번에 원본 claim 들의
      자리를 새 세그먼트 하나로 갈음한다. 예전(S-ACK-CAP 초판)에는 이 재보관이
      `spool.append` 의 예산 검사를 거쳐 원본과 동시에 잡혀 가득 찬 스풀에서 영원히
      거절될 수 있었다 — 지금은 예산 검사를 건너뛰므로(부분집합이라 추가 용량 0)
      그 실패 모드 자체가 없다. 쓰기 자체가 디스크 오류로 실패하면(드묾)
      `merge_rewrite_claims` 가 False 를 돌려주고, 원본 claim 은 전부 그대로 남아
      다음 회전이 다시 읽는다.

    in-flight 로 잡아야 하는 이유
      claim 은 됐지만 아직 정리되지 않은 구간은 대기열에도 «새 세그먼트»에도 없을 수
      있으므로, in-flight 로 잡아 flush 와 종료 처리가 이 구간의 완료를 기다리도록 한다.
    """
    global _inflight, _inflight_batch, _retry_keep_sink
    claimed_paths, events = _spool.claim_all(cfg.spool_dir, owner=_owner(cfg))
    _maybe_warn_other_owner(cfg)  # E-06 — claim_all 과 무관하게 매 회전 확인(비용은 glob 뿐)
    if not events:
        return
    _logger.debug("xsoft_comply: 스풀에서 %d개 이벤트 재전송 시도", len(events))
    _retry_keep_sink = []
    with _inflight_cond:
        _inflight += 1
        _inflight_batch = events
    try:
        _send_batch(events, cfg)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        # 전송이 예상하지 못한 이유로 실패했다 → claim 을 지우지 않는다(원본이 그대로
        # 디스크에 남아 다음 회수가 복구한다 · 싱크에 모인 것은 버린다 = 디스크에 쓴 적이
        # 없으므로 지울 것도 없다).
        _count_loop_error()
    else:
        keep = _retry_keep_sink or []
        if _spool.merge_rewrite_claims(claimed_paths, keep):
            pass  # 성공 = keep 이 있었으면 새 세그먼트로, 없었으면 전부 정리됨(함수 내부에서 처리)
        else:
            # ★ E-04 = 재보관 쓰기 자체가 실패했다(디스크 오류) → 원본 claim 을 그대로
            #   둔다(지우면 유실). 다음 스풀 확인 주기(_SPOOL_CHECK_INTERVAL)에 같은
            #   claim 을 다시 읽어 재시도한다.
            _logger.warning(
                "xsoft_comply: 스풀 재보관 실패(디스크 오류) — 원본 claim 보존, 다음 회전 재시도")
    finally:
        _retry_keep_sink = None
        with _inflight_cond:
            _inflight -= 1
            _inflight_batch = None
            _inflight_cond.notify_all()


def _bump_backoff(
    cfg: "_config.Config", *, force_max: bool = False, retry_after: float | None = None
) -> None:
    """재시도 카운터를 올리고 다음 스풀 확인 시각을 갱신한다.

    ★ Q-07 = 서버가 `Retry-After` 를 주면 그 시각보다 일찍 재시도하지 않는다
      (지수 백오프 결과와 비교해 **더 늦은 쪽**을 쓴다 — "서버 지시 대기보다 조기전송 0").
    """
    global _retry_attempt, _next_spool_check
    if force_max:
        _retry_attempt = cfg.max_retries
    else:
        _retry_attempt = min(_retry_attempt + 1, cfg.max_retries)
    delay = _spool.backoff(_retry_attempt)
    if retry_after is not None and retry_after > delay:
        delay = retry_after
    _next_spool_check = time.monotonic() + delay
