"""디스크 스풀 — 프로세스별 세그먼트 파일 · 원자적 교체 · 소유자(키) 결속 · 지수 백오프.

동작 원칙(재작성 · 528 보강C · S-03·S-05·S-14·S-15·GX-02·GX-05):
  - 파일은 **append 호출마다 새로 만든다**(세그먼트). 같은 파일을 열어 두고 여러 번
    이어 쓰지 않는다 = 두 프로세스가 같은 파일을 동시에 열고 지우는 경합(S-05·S-15)이
    구조적으로 생기지 않는다.
  - 각 세그먼트는 **임시 파일에 다 쓰고 fsync 한 뒤 최종 이름으로 rename** 한다(원자적 교체).
    rename 이 끝나기 전에 프로세스가 죽으면 최종 이름의 파일은 아예 존재하지 않는다
    = 회수자가 «반쯤 쓰인 파일」을 절대 보지 않는다(불완전 tail 문제 자체가 없어진다).
  - 파일 이름 접두사는 `xsoft_comply_events.` 로 못박는다(GX-05) — 같은 폴더의
    다른 애플리케이션 JSONL 을 절대 건드리지 않는다.
  - 파일 이름에 **소유자 지문**(api_key+endpoint 의 sha256 앞 12자)을 넣는다(GX-02) —
    키를 바꿔 재초기화해도 **이전 키로 쌓인 세그먼트를 새 키로 자동 전송하지 않는다.**
    회수(read_and_clear)는 지금 설정의 소유자와 일치하는 세그먼트만 가져간다.
  - 회수(claim)는 rename 으로 한다 — 여러 회수자(transport 스레드·수동 resend)가 동시에
    같은 폴더를 봐도 한쪽만 그 세그먼트를 가져간다(rename 은 같은 파일시스템에서 원자적).
    회수 도중 죽으면 `.claim` 파일이 남는데, 이 파일은 rename 이 끝난 뒤에만 존재하므로
    내용은 항상 온전하다 — 다음 회수 때 그대로 다시 읽어(=격리에서 자동 복구) 처리한다.
"""
from __future__ import annotations

import datetime
import hashlib
import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Any

_logger = logging.getLogger("xsoft_comply")

_MAX_BACKOFF: float = 60.0
_SEGMENT_PREFIX = "xsoft_comply_events"
_CLAIM_SUFFIX = ".claim"
_TMP_SUFFIX = ".tmp"

_seg_lock = threading.Lock()
_seg_counter = 0

# ── claim 만료(S-15·Q-04 · 0930-539) ──────────────────────────────────────────
#   ★ "claim 은 한 회수자만 · 만료 전 재회수 금지" — claim_segment 가 rename 직후
#     mtime 을 «지금»으로 찍어 두므로(아래), 그 claim 이 "얼마나 됐는지"를 파일
#     mtime 하나로 판정할 수 있다. list_stale_claims 는 이 나이가 _CLAIM_STALE_SECONDS
#     이상인 것만 «죽은 회수자가 남긴 것»으로 보고 회수 대상에 올린다 — 방금 다른
#     회수자가 가져가 아직 전송·ACK 처리 중인 claim 을 곧바로 훔쳐가지 않는다.
_CLAIM_STALE_SECONDS: float = 120.0

# ── 용량 초과 계측(Q-05) ──────────────────────────────────────────────────────
#   ★ "용량 초과 = 신규 거절" 이다 — 이미 디스크에 확정(fsync)된 기존 세그먼트를
#     지워서 자리를 만들면, 서버 확인 전 기록을 삭제하지 않는다는 원칙이 깨진다.
#     그래서 자리를 못 만들면 **새로 들어오는 몫만** 거절하고 기존은 그대로 둔다.
_rejected_total = 0
_rejected_lock = threading.Lock()
_next_rejected_warn = 1


def _count_rejected(n: int) -> None:
    global _rejected_total, _next_rejected_warn
    if n <= 0:
        return
    with _rejected_lock:
        _rejected_total += n
        total = _rejected_total
        warn = total >= _next_rejected_warn
        if warn:
            _next_rejected_warn = max(_next_rejected_warn * 10, 1)
    if warn:
        try:
            _logger.warning(
                "xsoft_comply: 스풀 용량(max_spool_bytes) 초과로 신규 기록이 거절되었습니다 — "
                "누적 %d건. 기존 확정 기록은 지우지 않습니다.", total)
        except BaseException:
            pass


def rejected() -> int:
    """Q-05 = 스풀 용량 초과로 **신규** 기록을 거절한 누적 건수(기존 확정분은 삭제하지 않는다)."""
    with _rejected_lock:
        return _rejected_total


def reset_rejected() -> None:
    global _rejected_total, _next_rejected_warn
    with _rejected_lock:
        _rejected_total = 0
        _next_rejected_warn = 1


# ── 직렬화 실패 계측(E-07 · 1001-540) ──────────────────────────────────────────
#   ★ "직렬화 불가 객체는 본문을 잃는다(식별자만 남긴다)" — 말없이 넘어가면 그
#     이벤트가 사실상 유실된 것을 아무도 알 수 없다. 건수를 세고 지수 간격으로 경고한다.
_serialize_failed_total = 0
_serialize_failed_lock = threading.Lock()
_next_serialize_warn = 1


def _count_serialize_failed(n: int = 1) -> None:
    global _serialize_failed_total, _next_serialize_warn
    if n <= 0:
        return
    with _serialize_failed_lock:
        _serialize_failed_total += n
        total = _serialize_failed_total
        warn = total >= _next_serialize_warn
        if warn:
            _next_serialize_warn = max(_next_serialize_warn * 10, 1)
    if warn:
        try:
            _logger.warning(
                "xsoft_comply: 이벤트 직렬화 실패로 본문이 유실됐습니다(식별자만 스풀에 "
                "남습니다) — 누적 %d건.", total)
        except BaseException:
            pass


def serialize_failed() -> int:
    """E-07(1001-540) = 직렬화 실패로 본문을 잃고 식별자만 남긴 누적 건수."""
    with _serialize_failed_lock:
        return _serialize_failed_total


def reset_serialize_failed() -> None:
    global _serialize_failed_total, _next_serialize_warn
    with _serialize_failed_lock:
        _serialize_failed_total = 0
        _next_serialize_warn = 1


# ── 소유자 지문(GX-02) ────────────────────────────────────────────────────────

def owner_fingerprint(api_key: str, endpoint: str) -> str:
    """이 설정(키+엔드포인트)의 짧은 지문. 세그먼트 파일 이름에 넣어 소유를 결속한다.

    ★ 원문(api_key)은 파일 이름에도, 어디에도 그대로 남지 않는다 — sha256 앞 12자만 쓴다.
    """
    try:
        material = f"{api_key}|{endpoint}".encode("utf-8", errors="replace")
        return hashlib.sha256(material).hexdigest()[:12]
    except BaseException:
        return "unknown"


def owner_of_segment(path: Path) -> str:
    """세그먼트(또는 claim) 파일 **이름 자체**에서 소유자 지문을 뽑아낸다(E-04·E-06 · 1001-540).

    이름 형식 = `xsoft_comply_events.<owner>.<pid>.<tid>.<ts>.<n>.<uniq>.jsonl[.claim]`.
    ★ 왜 필요한가 = `resend.py --all-owners` 처럼 "지금 설정된 키"와 무관하게 여러
      소유자의 세그먼트를 한꺼번에 다룰 때, 다시 보관할 자리(owner)를 **그 세그먼트가
      원래 속했던 소유자**로 정확히 돌려줘야 한다 — 호출자가 들고 있는 CLI 수준의
      "현재 owner" 값(None 또는 다른 값일 수 있다)을 쓰면 다른 키의 이벤트가
      "unknown" 소유자로 섞여 GX-02 격리가 깨진다.
    이름 형식과 안 맞으면(예상 밖 파일) "unknown" 을 돌려준다.
    """
    try:
        name = path.name
        prefix = f"{_SEGMENT_PREFIX}."
        if not name.startswith(prefix):
            return "unknown"
        rest = name[len(prefix):]
        owner = rest.split(".", 1)[0]
        return owner or "unknown"
    except BaseException:
        return "unknown"


# ── 디렉터리 관리 ─────────────────────────────────────────────────────────────

def _ensure_dir(spool_dir: str) -> Path | None:
    """스풀 디렉터리를 만든다. 실패하면 None 을 반환한다(예외를 전달하지 않는다)."""
    try:
        p = Path(spool_dir).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        return p
    except BaseException:
        return None


def _segment_glob(d: Path, owner: str | None = None) -> list[Path]:
    """SDK 소유 세그먼트만 나열한다(GX-05 = 다른 파일은 이름 패턴 자체가 다르다)."""
    try:
        pattern = f"{_SEGMENT_PREFIX}.{owner}.*.jsonl" if owner else f"{_SEGMENT_PREFIX}.*.jsonl"
        return sorted(d.glob(pattern), key=lambda p: p.stat().st_mtime)
    except BaseException:
        return []


# ★ M-26(1001-543) = 10분 넘은 `.tmp` 는 죽은 쓰기 시도(쓰다가 죽은 프로세스)로 본다.
#   정상 쓰기는 fsync+rename 까지 수 ms~수백 ms 안에 끝나므로, 그보다 훨씬 오래 `.tmp`
#   로 남아 있는 파일은 다시 완성될 길이 없다 — 예산에서 빼고(새 기록을 막지 않는다)
#   치운다(디스크를 계속 차지하지 않는다).
_STALE_TMP_SECONDS: float = 600.0


def _budget_glob(d: Path) -> list[Path]:
    """용량 상한 계산용 — 아직 지워지지 않은 SDK 세그먼트 전부(claim·tmp 포함).

    ★ Q-05 = "상한에 claim 포함" — claim_segment 는 파일을 지우지 않고 이름만
      `.claim` 으로 바꾼다(전송·ACK 처리 중). `_segment_glob`(끝이 `.jsonl`)은 이
      이름을 못 잡아서 예전에는 claim 된 만큼이 예산 계산에서 통째로 빠졌다 —
      그 사이 새 append 가 허용돼 실제 디스크 사용량이 max_spool_bytes 를 넘겼다.
      `.jsonl*` 로 넓혀 `.jsonl`·`.jsonl.claim`·`.jsonl.tmp` 를 전부 예산에 넣는다.
    ★ M-26 = 단, `_STALE_TMP_SECONDS` 보다 오래된 `.tmp` 는 죽은 쓰기 시도로 보고
      예산에서 빼며 그 자리에서 지운다(「용량 계산에서 빼고 치운다」).
    """
    try:
        out: list[Path] = []
        now = time.time()
        for p in sorted(d.glob(f"{_SEGMENT_PREFIX}.*.jsonl*")):
            if p.name.endswith(_TMP_SUFFIX):
                try:
                    age = now - p.stat().st_mtime
                except BaseException:
                    age = 0.0
                if age >= _STALE_TMP_SECONDS:
                    try:
                        p.unlink(missing_ok=True)
                    except BaseException:
                        pass
                    continue
            out.append(p)
        return out
    except BaseException:
        return []


def _would_exceed(d: Path, max_bytes: int, incoming_bytes: int) -> bool:
    """지금 쓰려는 세그먼트(incoming_bytes)까지 더하면 max_bytes 를 넘기는가.

    ★ Q-05 = 용량 초과는 **신규 거절**이다. 예전에는(S-03 이전 버전) 자리를 만들려고
      «오래된 세그먼트 1개» 를 지웠다 — 그건 서버가 아직 확인하지 않은 기존 확정
      기록을 SDK 스스로 삭제하는 것이라 원칙(ACK 전 삭제 금지)에 어긋난다.
      그래서 기존 세그먼트는 절대 건드리지 않고, **들어오는 몫만** 거절한다.
    ★ GX-05 = 이 폴더의 다른 애플리케이션 파일은 애초에 glob 패턴에 걸리지 않는다.
    """
    try:
        files = _budget_glob(d)  # 모든 소유자 · claim·tmp 포함 — 디스크 예산은 전체 공용
        total = sum(f.stat().st_size for f in files)
        return (total + incoming_bytes) > max_bytes
    except BaseException:
        return False  # 크기를 못 재면(디스크 오류) 거절하지 않고 시도한다(기존 fail-open)


def _new_segment_name(owner: str) -> str:
    global _seg_counter
    with _seg_lock:
        _seg_counter += 1
        n = _seg_counter
    pid = os.getpid()
    tid = threading.get_ident()
    ts = time.monotonic_ns()
    uniq = uuid.uuid4().hex[:8]
    return f"{_SEGMENT_PREFIX}.{owner}.{pid}.{tid}.{ts}.{n}.{uniq}.jsonl"


# ── 쓰기 ─────────────────────────────────────────────────────────────────────

def _serialize_events(events: list[dict[str, Any]]) -> list[str]:
    """이벤트 dict 목록을 JSONL 줄 목록으로 직렬화한다.

    ★ E-07(1001-540) = 개별 이벤트 직렬화가 실패하면(직렬화 불가 객체 등) 본문을
      통째로 버리지 않고 식별자(event_id·seq)만 남긴 대체 줄을 쓴다 — 그리고 그 사실을
      **센다·경고한다**(`_count_serialize_failed`). 예전에는 말없이 본문만 사라졌다.
      대체 줄조차 못 만들면(그 경우도 셈에 넣는다) 그 이벤트는 완전히 버린다.
    """
    out: list[str] = []
    for e in events:
        try:
            out.append(json.dumps(e, ensure_ascii=False))
        except BaseException:
            try:
                out.append(json.dumps({
                    "event_id": str(e.get("event_id", "")) if isinstance(e, dict) else "",
                    "seq": e.get("seq") if isinstance(e, dict) else None,
                    "_spool_error": "직렬화 실패 — 원본을 적을 수 없어 식별자만 남깁니다",
                }, ensure_ascii=False))
                _count_serialize_failed(1)
            except BaseException:
                _count_serialize_failed(1)
                continue
    return out


def append(events: list[dict[str, Any]], spool_dir: str, max_bytes: int, owner: str = "unknown") -> bool:
    """이벤트 목록을 **새 세그먼트**로 원자적으로 기록한다. 실패 시 예외 없음.

    ★ 원자적 교체(임시 → fsync → rename) = 쓰다 만 파일이 최종 이름으로 보이는 일이 없다.
    ★ Q-05 = 용량 초과는 이번 몫 전체를 거절한다(부분 기록 0 · 기존 확정분 삭제 0).
    ★ S-ACK-CAP(1001-540) = 반환값 = 이번 이벤트들이 디스크에 실제로 기록됐는지(True/False).
      호출자(transport._retry_spool)가 "재보관 성공이 확인된 뒤에만 원본 claim 삭제"를
      판정하는 데 쓴다. events 가 비어 있으면(=쓸 것이 없음) True — 실패가 아니다.
    """
    if not events:
        return True
    try:
        d = _ensure_dir(spool_dir)
        if d is None:
            return False

        out = _serialize_events(events)
        if not out:
            return False

        payload = ("\n".join(out) + "\n").encode("utf-8")
        if _would_exceed(d, max_bytes, len(payload)):
            _count_rejected(len(events))
            return False

        name = _new_segment_name(owner)
        final = d / name
        tmp = d / (name + _TMP_SUFFIX)
        with tmp.open("wb") as f:
            f.write(payload)
            f.flush()
            try:
                os.fsync(f.fileno())
            except BaseException:
                pass
        os.replace(tmp, final)  # 같은 파일시스템에서 원자적
        return True
    except BaseException:
        return False  # 디스크 쓰기 실패도 예외를 전달하지 않지만, 실패는 보고한다


# ── 읽기(회수) ────────────────────────────────────────────────────────────────

def list_segments(spool_dir: str, owner: str | None = None) -> list[Path]:
    """이 소유자(또는 owner=None 이면 전체)의 아직 claim 되지 않은 세그먼트 목록."""
    d = _ensure_dir(spool_dir)
    if d is None:
        return []
    return _segment_glob(d, owner)


def list_stale_claims(
    spool_dir: str, owner: str | None = None, *, min_age_seconds: float = _CLAIM_STALE_SECONDS
) -> list[Path]:
    """**진짜로 방치된** claim 파일만 돌려준다(내용은 항상 온전하다).

    ★ S-15/Q-04 = claim_segment 가 rename 직후 mtime 을 지금 시각으로 찍어 두므로,
      `min_age_seconds` 보다 어린 claim 은 "방금 다른 회수자가 가져가 아직 처리
      중"으로 보고 이 목록에서 뺀다 — 두 번째 회수자가 첫 번째의 진행 중인 claim 을
      곧바로 훔쳐 중복 삭제·유실을 만드는 경합을 막는다. 프로세스가 실제로 죽어
      `min_age_seconds` 이상 방치된 claim 만 «죽은 회수자가 남긴 것»으로 복구한다.
    """
    d = _ensure_dir(spool_dir)
    if d is None:
        return []
    try:
        pattern = (
            f"{_SEGMENT_PREFIX}.{owner}.*.jsonl{_CLAIM_SUFFIX}"
            if owner else f"{_SEGMENT_PREFIX}.*.jsonl{_CLAIM_SUFFIX}"
        )
        now = time.time()
        out = []
        for p in sorted(d.glob(pattern)):
            try:
                age = now - p.stat().st_mtime
            except BaseException:
                continue
            if age >= min_age_seconds:
                out.append(p)
        return out
    except BaseException:
        return []


def claim_segment(seg: Path) -> Path | None:
    """세그먼트를 rename 으로 claim 한다(원자적). 다른 회수자가 먼저 가져갔으면 None.

    ★ rename 뒤 mtime 을 «지금»으로 찍는다(S-15/Q-04) — list_stale_claims 가
      "이 claim 이 얼마나 방치됐는지"를 판정하는 기준이 claim 시각이 되게 한다
      (원본 세그먼트가 생성된 시각이 아니라).
    """
    claimed = seg.with_name(seg.name + _CLAIM_SUFFIX)
    try:
        os.rename(seg, claimed)
    except OSError:
        return None
    try:
        os.utime(claimed, None)
    except BaseException:
        pass  # mtime 갱신 실패해도 claim 자체는 유효하다(보수적으로 더 이르게 stale 판정될 뿐)
    return claimed


def read_claimed(claimed: Path) -> list[dict[str, Any]]:
    """claim 된 세그먼트 파일을 읽어 파싱한다(**지우지 않는다** — 호출자가 처리 완료 후 지운다).

    손상된 줄만 건너뛴다(줄 단위 파싱이라 한 줄의 손상이 나머지를 잃지 않는다).
    """
    try:
        raw = claimed.read_text(encoding="utf-8", errors="replace")
    except BaseException:
        return []
    out: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                out.append(obj)
        except json.JSONDecodeError:
            pass  # 손상된 줄 무시(격리 — 나머지 줄은 그대로 회수된다)
    return out


def discard_claim(claimed: Path) -> None:
    """처리가 끝난 claim 파일을 지운다. 실패해도 예외를 전달하지 않는다."""
    try:
        claimed.unlink(missing_ok=True)
    except BaseException:
        pass


def read_and_clear(spool_dir: str, owner: str = "unknown") -> list[dict[str, Any]]:
    """이 소유자(현재 설정된 키+엔드포인트)의 세그먼트만 모두 읽고 삭제한다.

    ★ GX-02 = 다른 소유자(옛 키) 의 세그먼트는 **건드리지 않는다** — 그대로 디스크에 남아
      그 키로 다시 초기화됐을 때만 회수된다.
    ★ rename 으로 먼저 «claim» 한다 — 동시에 여러 회수자가 있어도 한쪽만 가져간다.
      claim 된 뒤 삭제 전에 죽으면 `.claim` 파일이 남는데, 이 내용은 rename 이 끝난 뒤의
      상태이므로 항상 온전하다 — 다음 호출이 그대로 다시 읽어 복구한다(격리·복구 자동화).
    ★ 이 함수는 **전송 데몬 스레드**(transport.py)전용이다. 그 in-flight 추적이 "claim
      직후·send 이전에 프로세스가 죽는" 창을 atexit 안전망으로 덮는다. 사람이 직접 돌리는
      `resend.py` 는 그 안전망이 없으므로 claim_segment/read_claimed/discard_claim 을
      직접 조합해 "keep 을 먼저 새 세그먼트로 쓴 뒤에만" claim 을 지운다(S-05 재발 방지).
    """
    d = _ensure_dir(spool_dir)
    if d is None:
        return []

    events: list[dict[str, Any]] = []
    for claimed in list_stale_claims(spool_dir, owner):
        events.extend(read_claimed(claimed))
        discard_claim(claimed)
    for seg in list_segments(spool_dir, owner):
        claimed = claim_segment(seg)
        if claimed is None:
            continue
        events.extend(read_claimed(claimed))
        discard_claim(claimed)
    return events


# ── 회전당 메모리 상한(E-05 · 1001-540) ────────────────────────────────────────
#   ★ "한 회전에 스풀 전체를 메모리로 읽지 않는다" — max_spool_bytes 기본값(32MB)까지
#     찬 스풀에서 claim_all 을 부르면 32MB 를 한꺼번에 프로세스 메모리로 읽어 들였다.
#     회전당 상한을 두고, 넘는 만큼은 다음 회전(_SPOOL_CHECK_INTERVAL 뒤)으로 미룬다.
#     상한을 넘겨도 **최소 1개는 반드시 claim** 한다(그래야 빈 회전으로 영원히 멈추지
#     않는다 — 세그먼트 1개가 상한보다 커도 진행된다).
_CLAIM_ROUND_MAX_BYTES: int = 2 * 1024 * 1024  # 2 MiB


def claim_all(
    spool_dir: str, owner: str = "unknown", *, max_round_bytes: int = _CLAIM_ROUND_MAX_BYTES
) -> tuple[list[Path], list[dict[str, Any]]]:
    """이 소유자의 세그먼트를 **claim 만 하고 지우지 않은 채** 이벤트를 반환한다.

    ★ S-04/Q-03(0930-539) = "ACK 받은 뒤에만 디스크에서 지운다". `read_and_clear`
      는 claim 과 동시에 discard 까지 해 버려, 그 뒤(호출자가 전송을 채 끝내기도
      전에) 프로세스가 강제 종료되면 이벤트가 디스크에도 메모리에도 없이 사라진다
      (강제종료는 try/except 로 못 잡는다). 이 함수는 claim 까지만 하고 반환된
      경로를 그대로 둔다 — 호출자가 전송·ACK 처리를 다 끝낸 뒤에만
      `discard_claims()` 로 지운다. 그 전에 죽으면 `.claim` 파일이 원본 그대로
      남아 다음 회수(재시작 뒤 이 함수 재호출)가 복구한다.
    ★ E-05(1001-540) = 누적 claim 바이트가 `max_round_bytes` 를 넘기면 그 다음
      세그먼트부터는 이번 회전에서 건드리지 않는다(다음 회전이 이어서 가져간다).
    """
    d = _ensure_dir(spool_dir)
    if d is None:
        return [], []

    claimed_paths: list[Path] = []
    events: list[dict[str, Any]] = []
    total_bytes = 0

    def _under_budget() -> bool:
        return not claimed_paths or total_bytes < max_round_bytes

    for claimed in list_stale_claims(spool_dir, owner):
        if not _under_budget():
            break
        try:
            size = claimed.stat().st_size
        except BaseException:
            size = 0
        claimed_paths.append(claimed)
        total_bytes += size
        events.extend(read_claimed(claimed))
    for seg in list_segments(spool_dir, owner):
        if not _under_budget():
            break
        claimed = claim_segment(seg)
        if claimed is None:
            continue
        try:
            size = claimed.stat().st_size
        except BaseException:
            size = 0
        claimed_paths.append(claimed)
        total_bytes += size
        events.extend(read_claimed(claimed))
    return claimed_paths, events


def discard_claims(paths: list[Path]) -> None:
    """`claim_all` 이 돌려준 claim 파일들을 지운다 — 전송·ACK 처리가 끝난 뒤에만 부른다."""
    for p in paths:
        discard_claim(p)


def merge_rewrite_claims(claimed_paths: list[Path], events: list[dict[str, Any]]) -> bool:
    """`claim_all` 로 회수한 claim 들을 처리한 뒤, 아직 못 보낸 `events` 만 **세그먼트
    하나로 갈음**하고 원본 claim 들은 지운다(E-04 · 1001-540).

    ★ 왜 필요한가 = 예전(0930-539) 수리는 남은 이벤트를 `append()`(예산 검사
      `_would_exceed` 를 거치는 **새** 세그먼트)로 재보관했다. 그런데 원본 claim 이
      아직 지워지지 않은 채로 있으니 "원본 + 새 세그먼트" 가 한꺼번에 예산에 잡혀,
      가득 찬 스풀에서는 재보관이 **영원히** 거절됐다(용량이 실질적으로 2배 필요).
      이 함수는 예산 검사를 건너뛴다 — `events` 는 이미 claim 해 예산에 잡혀 있던
      내용의 부분집합이므로(재전송 중 새로 생기는 바이트가 없다) 추가 용량이 0이다.
    ★ 순서 = **먼저 새 파일에 다 쓰고 fsync·rename 한 뒤에만** 원본들을 지운다(원자적
      교체와 같은 순서 원칙) — 쓰다 실패하면 원본은 전부 그대로 남는다(부분 성공을
      만들지 않는다 · claimed_paths 가 비어도 지우지 않는다).
    events 가 비었으면(전부 accepted/duplicated 등으로 처리 완료) claimed_paths 를
    전부 지운다. 반환값 True = "이제 claimed_paths 를 잊어도 안전하다".
    """
    if not claimed_paths:
        return True
    if not events:
        for p in claimed_paths:
            discard_claim(p)
        return True

    out = _serialize_events(events)
    if not out:
        for p in claimed_paths:
            discard_claim(p)
        return True

    d = claimed_paths[0].parent
    owner = owner_of_segment(claimed_paths[0])
    name = _new_segment_name(owner)
    target = d / name
    tmp = d / (name + _TMP_SUFFIX)
    try:
        payload = ("\n".join(out) + "\n").encode("utf-8")
        with tmp.open("wb") as f:
            f.write(payload)
            f.flush()
            try:
                os.fsync(f.fileno())
            except BaseException:
                pass
        os.replace(tmp, target)  # 같은 파일시스템에서 원자적
    except BaseException:
        try:
            tmp.unlink(missing_ok=True)
        except BaseException:
            pass
        return False  # 원본 claimed_paths 는 손대지 않았다 — 다음 회전이 그대로 다시 읽는다

    for p in claimed_paths:
        discard_claim(p)
    return True


def other_owner_pending(spool_dir: str, owner: str) -> int:
    """이 소유자(owner)가 아닌 **다른** 키로 쌓인 세그먼트·claim 개수(E-06 · 1001-540).

    ★ 키 교체(KEY-02 유예) 뒤 옛 키 세그먼트는 지금 소유자 지문과 달라
      `claim_all`·`read_and_clear` 가 절대 가져가지 않는다(GX-02 = 의도된 격리).
      자동으로는 다시 보내지지 않고 용량만 차지하므로, 최소한 "몇 건 있다"는 걸
      알 수 있게 **센다**(지우거나 건드리지 않는다) — 운영자가
      `python -m xsoft_comply.resend --all-owners` 로 회수할 신호를 준다.
    """
    d = _ensure_dir(spool_dir)
    if d is None:
        return 0
    try:
        prefix = f"{_SEGMENT_PREFIX}."
        count = 0
        for p in d.glob(f"{_SEGMENT_PREFIX}.*.jsonl*"):
            name = p.name
            if not name.startswith(prefix):
                continue
            seg_owner = name[len(prefix):].split(".", 1)[0]
            if seg_owner and seg_owner != owner:
                count += 1
        return count
    except BaseException:
        return 0


# ── 격리(quarantine) ──────────────────────────────────────────────────────────
#   재시도해도 통과할 수 없는 형식 오류(HTTP 400)는 **삭제하지 않고** 별도 위치에 남긴다
#   (Q-06 = "400 형식 오류만 격리(삭제 아님)"). 스풀 예산·회수 대상 밖이다.

_QUARANTINE_DIRNAME = "quarantine"


def quarantine(events: list[dict[str, Any]], spool_dir: str, reason: str) -> None:
    """형식 오류로 영구 제외된 이벤트를 삭제 대신 격리 보관한다(운영자 사후 확인용)."""
    if not events:
        return
    try:
        d = _ensure_dir(spool_dir)
        if d is None:
            return
        q = d / _QUARANTINE_DIRNAME
        q.mkdir(parents=True, exist_ok=True)
        ts = time.monotonic_ns()
        name = f"quarantine.{os.getpid()}.{ts}.{uuid.uuid4().hex[:8]}.jsonl"
        lines = []
        for e in events:
            try:
                lines.append(json.dumps({"reason": reason, "event": e}, ensure_ascii=False))
            except BaseException:
                continue
        if not lines:
            return
        tmp = q / (name + _TMP_SUFFIX)
        final = q / name
        with tmp.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except BaseException:
                pass
        os.replace(tmp, final)
    except BaseException:
        pass


def list_quarantine_files(spool_dir: str) -> list[Path]:
    """격리 보관 파일 목록(M-16 = `resend --include-quarantine` 전용)."""
    d = _ensure_dir(spool_dir)
    if d is None:
        return []
    q = d / _QUARANTINE_DIRNAME
    if not q.exists():
        return []
    try:
        return sorted(q.glob("quarantine.*.jsonl"))
    except BaseException:
        return []


def read_quarantine_file(path: Path) -> list[dict[str, Any]]:
    """격리 파일 한 줄(`{"reason":..., "event": {...}}`)씩 그대로 돌려준다."""
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except BaseException:
        return []
    out: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and isinstance(obj.get("event"), dict):
                out.append(obj)
        except json.JSONDecodeError:
            pass
    return out


def rewrite_quarantine_file(path: Path, remaining: list[dict[str, Any]]) -> None:
    """`remaining`(성공분을 뺀 나머지)만 같은 파일에 다시 쓴다. 비었으면 파일을 지운다."""
    try:
        if not remaining:
            path.unlink(missing_ok=True)
            return
        lines = []
        for item in remaining:
            try:
                lines.append(json.dumps(item, ensure_ascii=False))
            except BaseException:
                continue
        if not lines:
            path.unlink(missing_ok=True)
            return
        tmp = path.with_name(path.name + _TMP_SUFFIX)
        with tmp.open("w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except BaseException:
                pass
        os.replace(tmp, path)
    except BaseException:
        pass


# ── 1.0.1 하위호환 보존(U-10·U-11 · 1002-546 · Y1·Y2 재정정 · 546-이어) ─────────
#   1.0.1 은 스풀 디렉터리 바로 아래 단일 파일 `events.jsonl` 하나에 이어 썼다 — 그
#   파일에는 **어느 키의 기록인지 표식이 없다**. 예전(0930-539·M-29) 수리는 이 파일을
#   "지금 설정된 키" 소유 세그먼트로 자동 이관했는데, 그러면 1.0.1 때의 키(제품·회사)와
#   지금 키가 다를 때 **오귀속**(남의 제품·회사로 전송)이 난다(GPT 버전업 검수 V-10).
#   그래서 지금은 **어느 키로도 자동 귀속시키지 않는다** — 원자적으로 `legacy_unowned/`
#   아래로 옮겨 보존만 한다.
#   ★ Y1(A-546-01·546-이어) = **보존분을 SDK 가 다시 보내는 길 자체를 없앴다.**
#   1.0.1 파일에는 원래 키 표식이 없어서 "지금 입력한 키 앞자리와 같은지" 확인은
#   **그 파일을 쓴 원래 키를 증명하지 못한다** — 앞자리가 같아도 다른 회사·제품의
#   키일 수 있고, 다르면 자명하게 남의 키다. 둘 다 오귀속 위험이라 자동/반자동
#   재전송 경로를 아예 지운다. `legacy_unowned/` 는 이제 SDK 가 **읽어서 다시 보내지도,
#   지우지도 않는 영구 보존소**다 — 복구는 1.0.1 때 쓰던 키(제품·회사)를 확실히 아는
#   사람이 그 파일을 오프라인으로 확인한 뒤 수동으로 이전하는 절차로만 한다.
_LEGACY_FILENAME = "events.jsonl"
_LEGACY_UNOWNED_DIRNAME = "legacy_unowned"


def _count_jsonl_dicts(path: Path) -> int:
    """JSONL 파일에서 유효한 object(dict) 줄 수만 센다(형식 오류 줄은 건너뛴다)."""
    count = 0
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except BaseException:
        return 0
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            if isinstance(json.loads(line), dict):
                count += 1
        except json.JSONDecodeError:
            pass
    return count


def _warn_preserved(count: int, path: Path) -> None:
    """★ Y2 = 명령을 적지 않는다 — 보존 사실과 파일 위치만 알린다(복구 절차는 README·안내 몫)."""
    try:
        _logger.warning(
            "xsoft_comply: 1.0.1 형식 미전송 기록 %d건을 보존했습니다"
            "(어느 키로도 보내지 않습니다 · 파일 = %s).", count, path)
    except BaseException:
        pass


def migrate_legacy_events_file(spool_dir: str, owner: str) -> bool:
    """1.0.1 형식 `events.jsonl` 이 있으면 **이름을 바꿔(rename) 보존 폴더로 옮기기만** 한다.

    ★ U-11 = 이관은 **이름 바꾸기(os.replace) 먼저** — 그 뒤에만 내용을 읽는다(개수 계측·
      경고용). 예전에는 먼저 읽고(read_text) 그 다음에 지웠다(unlink) — 그 사이에 1.0.1
      프로세스가 아직 살아 있어 같은 파일에 또 append 하면, rename 없이 통째로 덮어써지는
      게 아니라 **읽은 뒤 내용이 사라지는 창**이 생겨 동시 추가분이 유실됐다
      (GPT `migration_race_probe.py` 재현). rename 은 원자적이라 그 창이 없다 — rename
      이후 1.0.1 이 다시 쓰는 내용은 **새 `events.jsonl`** 로 들어가고(기존 파일이 아니라),
      다음 호출이 그 새 파일을 또 그대로 옮긴다. 유실 0.
    ★ owner 인자는 **이제 세그먼트 소유 지정에 쓰지 않는다**(1.0.1 파일에 원래 키 표식이
      없으므로 아무 키로도 자동 귀속시키지 않는다) — 호출부 호환을 위해 시그니처만 남긴다.
    ★ 1.0.2 는 1.0.1 파일을 **지우지 않는다** — os.replace 는 삭제가 아니라 이름 변경이다.
    ★ Y2(546-이어) = 이동 자체가 **실패하면**(예 = 권한 거부) 원본은 그 자리에 그대로
      남고 **False** 를 돌려준다 — 그래도 유실은 아니다(`legacy_unowned_count()` 가
      이 미이동 원본도 함께 센다 — 아래) — 그 자리에서 경고도 1회 낸다.
    옮길 것이 없으면(파일 없음) True(실패가 아니다).
    """
    d = _ensure_dir(spool_dir)
    if d is None:
        return False
    legacy = d / _LEGACY_FILENAME
    if not legacy.exists():
        return True
    try:
        preserve_dir = d / _LEGACY_UNOWNED_DIRNAME
        preserve_dir.mkdir(parents=True, exist_ok=True)
    except BaseException:
        return False
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    target = preserve_dir / f"events_{ts}_{os.getpid()}.jsonl"
    try:
        os.replace(legacy, target)  # 원자적 rename — 읽기 전에 먼저(U-11)
    except OSError:
        if legacy.exists():
            # ★ Y2 = 이동 자체가 실패했다(다른 프로세스가 먼저 옮긴 것과 다르다 —
            #   그 경우라면 legacy 가 이미 사라져 있었을 것) — 원본 그대로, 경고 1회.
            _warn_preserved(_count_jsonl_dicts(legacy), legacy)
            return False
        # 다른 프로세스가 이미 옮겼다면(동시 init) 정상 상태다 — 더 할 일 없음.
        return True

    _warn_preserved(_count_jsonl_dicts(target), target)
    return True


def legacy_unowned_files(spool_dir: str) -> list[Path]:
    """보존된(U-10) 1.0.1 레거시 파일 목록(`legacy_unowned/`) — claim 없이 그대로 나열."""
    d = _ensure_dir(spool_dir)
    if d is None:
        return []
    q = d / _LEGACY_UNOWNED_DIRNAME
    if not q.exists():
        return []
    try:
        return sorted(q.glob("events_*.jsonl"))
    except BaseException:
        return []


def legacy_unowned_count(spool_dir: str) -> int:
    """보존된 1.0.1 레거시 이벤트 누적 건수(공개 `xsoft_comply.legacy_unowned_events()` 받침).

    ★ Y2(546-이어) = `legacy_unowned/` 로 이미 옮겨진 건수 **+ 이동에 실패해 아직
      원래 자리(`events.jsonl`)에 남은 건수**를 함께 센다 — 이동 실패(권한 등) 때도
      고객이 "0건"으로 잘못 믿지 않게 한다.
    """
    total = 0
    d = _ensure_dir(spool_dir)
    if d is not None:
        legacy = d / _LEGACY_FILENAME
        if legacy.exists():
            total += _count_jsonl_dicts(legacy)
    for p in legacy_unowned_files(spool_dir):
        total += _count_jsonl_dicts(p)
    return total


def read_legacy_unowned_file(path: Path) -> list[dict[str, Any]]:
    """보존 파일 한 줄(원본 이벤트 dict, 내부 필드 포함)씩 그대로 돌려준다(읍기 전용 조회).

    ★ Y1(546-이어) = SDK 의 **어느 운영 경로도 이 함수를 보내거나 지우는 데 쓰지 않는다**
      (보존분을 다시 보내는 길 자체가 없다) — 보존 건수·내용을 사람이 들여다볼 때만 쓴다.
    """
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except BaseException:
        return []
    out: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                out.append(obj)
        except json.JSONDecodeError:
            pass
    return out


# ── 지수 백오프(Q-07) ──────────────────────────────────────────────────────────

def backoff(attempt: int) -> float:
    """attempt(1-based) → 대기 초수. 1→2→4→…→60 상한 + jitter.

    attempt=0 이면 0 반환(즉시 재시도).
    ★ Q-07 = "재시도 최소1초 최대60초 지수증가+jitter" — jitter 는 여러 프로세스가
      동시에 장애를 만났을 때 재시도가 한 순간에 몰리는 것(thundering herd)을 막는다.
    """
    if attempt <= 0:
        return 0.0
    base = min(float(2 ** (attempt - 1)), _MAX_BACKOFF)
    try:
        import random
        jitter = 0.8 + random.random() * 0.4  # base 의 80%~120%
    except BaseException:
        jitter = 1.0
    return min(base * jitter, _MAX_BACKOFF)
