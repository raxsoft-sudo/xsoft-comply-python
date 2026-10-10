"""논블로킹 큐 — 호출 스레드가 O(1)로 이벤트를 넣는다.

동작 원칙:
  - 기록은 queue.put_nowait만 (O(1)). 큐가 차면 가장 오래된 것을 버린다.
  - 블로킹 금지 = 호출 스레드가 네트워크를 만지지 않는다.
  - init() 전에 put() 호출 시 no-op (중단되지 않는다).
"""
from __future__ import annotations

import json
import logging
import queue
import threading
from pathlib import Path
from typing import Any, Optional

_logger = logging.getLogger("xsoft_comply")

# _q = None → init() 전 상태 = put() 은 no-op
_q: Optional[queue.Queue] = None
_q_lock = threading.Lock()

# bounded queue drop 계측.
#   중요한 운영 위험은 알림 없는 이벤트 누락이다.
#   그래서 제외한 건수를 세고, 처음과 그 뒤 10배씩 지날 때마다 경고를 띄운다
#   (매 건 경고하면 그 로그가 다시 DoS 가 된다).
_dropped_total: int = 0
_drop_lock = threading.Lock()
_next_warn: int = 1

# 전송실패(서버 거부) 계측.
#   대기열 초과와 **원인도 조치도 다르다**. 섞어 세면 확인할 자리를 찾기 어렵다.
#     - dropped = SDK 대기열이 용량을 초과해 제외한 것 → max_queue 를 키우거나 flush_interval 을 줄인다.
#     - failed  = 서버가 거부해 전송되지 않은 것 → API 키·요금제·본문 크기를 확인한다.
#   두 값을 함께 확인해야 누락 가능성을 판단할 수 있다.
_failed_total: int = 0
_failed_lock = threading.Lock()
_next_failed_warn: int = 1


def init(maxsize: int = 10_000) -> None:
    """transport 시작 전에 반드시 호출한다. maxsize = 큐 상한(max_queue).

    ★ GX-04 = 재호출(재초기화)이 큐를 통째로 새로 만들면 이미 대기 중이던 미전송
      이벤트가 «잔여 0» 으로 사라진다(구 API 형식은 유지 — 여러 번 불러도 되는
      init() 이지 새 프로세스를 뜻하지 않는다). 기존 큐에 남아 있던 항목은 새 큐로
      그대로 옮겨 받는다. 새 max_queue 가 더 작아 다 못 옮기면, 옮기지 못한 몫만
      (옛 항목이므로 앞쪽부터 보존하고) 대기열 초과로 계측한다.
    """
    global _q
    with _q_lock:
        old_q = _q
        new_q: queue.Queue = queue.Queue(maxsize=maxsize)
        if old_q is not None:
            while True:
                try:
                    item = old_q.get_nowait()
                except queue.Empty:
                    break
                try:
                    new_q.put_nowait(item)
                except queue.Full:
                    _count_drop()
        _q = new_q


def put(item: Any) -> bool:
    """O(1) 비블로킹 put. 큐 가득 참 → **신규 거절**. 반환 = 실제로 큐에 들어갔는가.

    ★ S-14 = 예전에는 가득 차면 **가장 오래된 항목**(이미 seq 를 배정받아 앞쪽에 있는 이벤트)을
      버렸다 — 그 seq 는 영원한 SEQ_GAP 이 되어 서버가 **그 뒤 모든 이벤트까지** 거부한다
      (구멍 하나가 trace 전체를 막는다). 계약(Q-05)은 "용량 초과 = 신규 거절"이다 —
      즉 대기열에 **이미 있던(더 앞선 seq)** 이벤트는 그대로 두고, **지금 들어오는** 이벤트만
      거절한다. 오래된 이벤트는 결국 먼저 빠져나가므로 trace 중간에 영구 구멍이 생기지 않는다.
    ★ M-01(1001-543) = 반환값을 **호출자(session._emit)가 seq 확정 여부**를 판단하는 데 쓴다.
      거절됐는데도 seq 가 이미 확정돼 있으면 그 번호가 영원한 SEQ_GAP 이 된다 — put() 이
      성공했을 때만 True 를 돌려줘 호출자가 그때만 seq 를 커밋하게 한다.
    """
    q = _q
    if q is None:
        return False  # init() 전 = no-op
    try:
        q.put_nowait(item)
        return True
    except queue.Full:
        _count_drop()
        return False


def _count_drop() -> None:
    """누락 1건 계측 + 지수 간격 경고. (계측 자체가 부하가 되면 안 된다)"""
    global _dropped_total, _next_warn
    with _drop_lock:
        _dropped_total += 1
        total = _dropped_total
        warn = total >= _next_warn
        if warn:
            _next_warn = max(_next_warn * 10, 1)
    if warn:
        try:
            _logger.warning(
                "xsoft_comply: 대기열이 가득 차 이벤트가 누락되었습니다 — 누적 %d건. "
                "max_queue 를 늘리거나 flush_interval 을 줄이세요.", total)
        except BaseException:
            pass


def dropped() -> int:
    """지금까지 **대기열 초과**로 누락된 이벤트 수(서버 거부는 failed() 로 따로 센다).

    0 이 아니면 감사기록에 누락 가능성이 있다는 뜻이다.
    """
    with _drop_lock:
        return _dropped_total


def reset_dropped() -> None:
    global _dropped_total, _next_warn
    with _drop_lock:
        _dropped_total = 0
        _next_warn = 1


def count_failed(n: int = 1) -> None:
    """서버 거부로 전송되지 않은 이벤트 n건 계측 + 지수 간격 경고.

    ★ n = **이벤트 건수**다(배치 1건이 아니라). 한 POST 에 최대 25건이 실리므로
      묶음 단위로 세면 누락을 25배 축소 보고하게 된다.
    (계측 자체가 부하가 되면 안 된다 = _count_drop 과 같은 규칙 · 로깅은 락 밖에서)
    """
    global _failed_total, _next_failed_warn
    if n <= 0:
        return
    with _failed_lock:
        _failed_total += n
        total = _failed_total
        warn = total >= _next_failed_warn
        if warn:
            _next_failed_warn = max(_next_failed_warn * 10, 1)
    if warn:
        try:
            _logger.warning(
                "xsoft_comply: 서버가 거부해 이벤트가 전송되지 않았습니다 — 누적 %d건. "
                "API 키·요금제·본문 크기를 확인하세요.", total)
        except BaseException:
            pass


def failed() -> int:
    """지금까지 서버 거부로 전송되지 않은 이벤트 수. 0 이 아니면 감사기록에 누락 가능성이 있다."""
    with _failed_lock:
        return _failed_total


def reset_failed() -> None:
    global _failed_total, _next_failed_warn
    with _failed_lock:
        _failed_total = 0
        _next_failed_warn = 1


# ── 계수 영속화 (S-19) ────────────────────────────────────────────────────────
#   dropped()·failed() 는 지금까지 «프로세스 메모리에만» 있었다 — 재시작하면 0으로
#   돌아가 과거의 누락 가능성이 사라진 것처럼 보인다(알림 없는 재설정).
#   ★ 이 함수는 **전송 스레드**(transport._run_once 의 정기 확인 주기)에서만 부른다.
#     호출 스레드(=고객 스레드)의 put()/count_failed() 경로에는 디스크 I/O를 넣지 않는다
#     (F-03 = 호출 스레드 네트워크·블로킹 I/O 0).
_COUNTERS_PREFIX = "counters"
_COUNTERS_GLOB = "counters*.json"
_counters_unknown = False  # 저장/복원에 실패하면 True — "정확한 손실 0" 대신 "불명" 을 노출한다


def counters_unknown() -> bool:
    """카운터 영속화가 실패해 «정확한 값» 을 보장할 수 없으면 True."""
    return _counters_unknown


def _own_counters_path(spool_dir: str) -> Path:
    import os as _os
    d = Path(spool_dir).expanduser().resolve()
    return d / f"{_COUNTERS_PREFIX}.{_os.getpid()}.json"


def load_persisted_counters(spool_dir: str) -> None:
    """init() 시 이전 실행(들)이 남긴 계수를 불러와 이어서 센다(재시작 후 유지).

    ★ M-25(1001-543) = 계수 파일을 **프로세스별로** 둔다(`counters.<pid>.json`).
      예전에는 파일 하나를 여러 프로세스가 같은 이름으로 덮어써 — 프로세스 A가 쓴 값을
      프로세스 B가 그대로 지워버렸다(마지막에 쓴 프로세스만 살아남는다). 지금은 이
      디렉터리의 모든 `counters*.json` 을 **합산**해 "지금까지 어느 프로세스에서든
      누락된 전체 건수"를 복원한다.
    """
    global _dropped_total, _failed_total, _counters_unknown
    try:
        d = Path(spool_dir).expanduser().resolve()
        if not d.exists():
            return
        total_dropped = 0
        total_failed = 0
        found = False
        for p in sorted(d.glob(_COUNTERS_GLOB)):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                continue
            found = True
            total_dropped += int(data.get("dropped", 0))
            total_failed += int(data.get("failed", 0))
        if not found:
            return
        with _drop_lock:
            _dropped_total = total_dropped
        with _failed_lock:
            _failed_total = total_failed
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        # 이전 계수를 못 읽었다 = 지금부터는 "정확한 손실 0" 을 주장할 수 없다.
        _counters_unknown = True


def persist_counters(spool_dir: str) -> None:
    """현재 프로세스의 계수를 **이 프로세스 전용 파일**에 저장한다(원자적 교체).

    실패해도 예외를 전달하지 않는다. 다른 프로세스의 계수 파일은 건드리지 않는다
    (M-25 = 여러 프로세스가 같은 스풀 폴더를 공유해도 서로 덮어쓰지 않는다).
    """
    global _counters_unknown
    try:
        d = Path(spool_dir).expanduser().resolve()
        d.mkdir(parents=True, exist_ok=True)
        final = _own_counters_path(spool_dir)
        tmp = final.with_name(final.name + ".tmp")
        payload = json.dumps({"dropped": dropped(), "failed": failed()})
        with tmp.open("w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            try:
                import os as _os
                _os.fsync(f.fileno())
            except BaseException:
                pass
        tmp.replace(final)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        _counters_unknown = True


def get_batch(max_items: int = 200, timeout: float = 1.0) -> list[Any]:
    """transport 스레드가 배치로 꺼낸다. 최대 max_items개.

    첫 번째 아이템은 timeout 초 블로킹 대기 → CPU 낭비 없이 flush_interval을 구현한다.
    나머지는 논블로킹으로 즉시 꺼낸다.
    """
    q = _q
    if q is None:
        return []
    batch: list[Any] = []
    # 첫 아이템: 블로킹 대기 (flush_interval 동안 잠든다)
    try:
        item = q.get(timeout=timeout)
        batch.append(item)
    except queue.Empty:
        return []
    # 나머지: 논블로킹으로 최대치까지
    while len(batch) < max_items:
        try:
            batch.append(q.get_nowait())
        except queue.Empty:
            break
    return batch


def size() -> int:
    """현재 큐 깊이. flush 대기 판단에 사용."""
    q = _q
    return q.qsize() if q is not None else 0


def drain_up_to(n: int) -> list[Any]:
    """큐에서 논블로킹으로 최대 n개만 꺼낸다(M-22 과부하 방지 스레드 전용).

    큐가 없거나 n<=0 이면 빈 목록.
    """
    q = _q
    if q is None or n <= 0:
        return []
    items: list[Any] = []
    for _ in range(n):
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            break
    return items


def drain() -> list[Any]:
    """큐에 남은 모든 항목을 논블로킹으로 꺼내 반환한다.

    종료 시 스풀 기록 안전망으로 사용한다. 큐가 없으면 빈 목록 반환.
    """
    q = _q
    if q is None:
        return []
    items: list[Any] = []
    while True:
        try:
            items.append(q.get_nowait())
        except queue.Empty:
            break
    return items


def reset() -> None:
    """테스트/shutdown 용. 큐를 새로 만든다."""
    global _q
    with _q_lock:
        _q = None
