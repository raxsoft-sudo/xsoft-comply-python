"""디스크 스풀 재전송 모듈.

사용법(★ M-13·1001-543 = --endpoint 는 필수 · 기본값 없음 — 운영 환경에서 조용히 로컬
전용 주소로 떨어지는 사고를 막는다):
    python -m xsoft_comply.resend \\
        --api-key-env XSOFT_API_KEY \\
        --endpoint https://your-domain.com/api/v1 \\
        --spool ~/.xsoft_comply/spool

역할 = API 장애 복구 후 스풀 세그먼트를 일괄 재전송하고 성공한 것을 지운다.
처리 방식(528 보강C 재작성 — spool.py 의 세그먼트 모델과 짝을 맞춘다):
   - 세그먼트를 **claim** 한 뒤 읽는다(지우지 않는다) — 처리 결과가 확정될 때까지
     원본 내용이 디스크에 남아 있다(S-05 = "먼저 지우고 다시 쓰기" 재발 방지).
   - trace_id 기준으로 그룹핑해 trace별 envelope 으로 전송.
   - 요청 본문 = {"trace_id": ..., "agent_id": ..., "events": [...], "ack_version": N}
   - 봉투 필드(_trace_id·_agent_id·_product_id)는 events[] 안에서 제거.
   - results[] 를 줄(이벤트) 단위로 처리:
       accepted·duplicated       → 처리 완료(재기록 없음)
       SEQ_GAP·PREV_HASH_MISMATCH → 새 세그먼트로 재기록(재시도)
       SEQ_CONFLICT               → 격리(quarantine) — 삭제해 사라지지 않는다
       INVALID_EVENT              → 메시지로 가른다.
           "저장 오류: …"(서버가 저장에 실패) → 새 세그먼트로 재기록(재시도)
           그 외(형식 오류)                    → 격리(quarantine)
   - keep 이 확정되어 **새 세그먼트로 안전하게 쓰인 뒤에만** claim 원본을 지운다.
   - HTTP 413 은 절반으로 나눠 재시도한다(삭제하지 않는다).
   - 기본은 **현재 설정된 키(owner)에 결속된 세그먼트만** 처리한다(GX-02).
     다른 키로 쌓인 세그먼트까지 보고 싶으면 `--all-owners` 를 명시한다.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

from . import spool as _spool

# ★ U-13(1002-546) = resend 의 User-Agent 도 고정 "0.1" 이 아니라 실제 배포 버전을 따라간다
#   (transport.py `_user_agent()` 와 같은 지연조회 — resend.py 는 __init__.py 가 import
#   하지 않으므로 순환참조 위험은 없지만, 캐시 패턴을 그대로 맞춘다).
_RESEND_UA_CACHE: str | None = None


def _resend_user_agent() -> str:
    global _RESEND_UA_CACHE
    if _RESEND_UA_CACHE is None:
        try:
            from . import __version__ as _v
            _RESEND_UA_CACHE = f"xsoft-comply-resend/{_v}"
        except BaseException:
            _RESEND_UA_CACHE = "xsoft-comply-resend"
    return _RESEND_UA_CACHE

# 스풀 줄에 있지만 API payload 에서는 제거할 내부 봉투 필드
_INTERNAL_PREFIX = "_"

# 서버 응답에서 온 문자열을 그대로 출력하지 않는다(log injection 방지).
#   `event_id`·`message` 는 SDK 가 만든 값이 아니다. CR/LF 가 섞여 있으면
#   운영자 로그에 «가짜 한 줄» 이 추가될 수 있다(감사기록 위조 위험).
#   transport 와 같은 함수를 사용해 규칙을 한 벌로 유지한다.
from .transport import _read_capped as _read_capped   # noqa: E402
from .transport import _safe as _safe                 # noqa: E402
from .transport import _opener as _opener             # noqa: E402  ★ S-18 = 리다이렉트 차단 opener 공유
from .transport import _MAX_EVENTS_PER_POST as _MAX_EVENTS_PER_POST  # noqa: E402  ★ M-24

_ACK_VERSION = 2  # bundle ⑤ — transport.py 와 같은 값

# per-event reject code 분류
_RETRY_CODES: frozenset[str] = frozenset({"SEQ_GAP", "PREV_HASH_MISMATCH"})
_DROP_CODES: frozenset[str] = frozenset({"SEQ_CONFLICT"})

# 서버가 «저장에 실패했다» 고 알릴 때도 code 는 INVALID_EVENT 로 온다.
#   예 = `"저장 오류: Error: Too many subrequests by single Worker invocation."`
#   이를 «형식 오류» 로 보고 제외하면 복구 재전송에서 감사기록이 알림 없이 누락된다
#   (transport.py 와 같은 규칙으로 처리한다).
_STORE_FAIL_PREFIX = "저장 오류"


# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

def _strip_internal(evt: dict[str, Any]) -> dict[str, Any]:
    """봉투 필드(_로 시작)를 제거하고 API 전송용 dict 반환."""
    return {k: v for k, v in evt.items() if not k.startswith(_INTERNAL_PREFIX)}


def _send_envelope(
    endpoint: str,
    api_key: str,
    trace_id: str,
    agent_id: str | None,
    events: list[dict[str, Any]],
    timeout: float = 10.0,
) -> tuple[int, dict[str, Any], Any]:
    """trace 그룹 하나를 envelope 형태로 POST. (status_code, resp_body, headers) 반환.

    api_key 는 헤더에만 사용. 로그에 원문 출력 안 함.
    ★ S-18 = 리다이렉트를 따라가지 않는 opener 를 쓴다(transport.py 와 공유).
    """
    body: dict[str, Any] = {"trace_id": trace_id, "events": events, "ack_version": _ACK_VERSION}
    if agent_id:
        body["agent_id"] = agent_id

    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{endpoint.rstrip('/')}/events",
        data=data,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": _resend_user_agent(),
            "Accept-Encoding": "identity",
        },
        method="POST",
    )
    try:
        with _opener.open(req, timeout=min(timeout, 10.0)) as resp:
            status_code: int = resp.status
            resp_text = _read_capped(resp)
        try:
            resp_body = json.loads(resp_text)
        except json.JSONDecodeError:
            resp_body = {}
        return status_code, resp_body, resp.headers
    except urllib.error.HTTPError as exc:
        try:
            err_body = json.loads(_read_capped(exc))
        except Exception:
            err_body = {}
        return exc.code, err_body, exc.headers
    except Exception:
        return 0, {}, None  # 연결 실패


# ── results[] 처리 ────────────────────────────────────────────────────────────

def _process_results(
    resp_body: dict[str, Any],
    original: list[dict[str, Any]],
    spool_dir: Path,
) -> tuple[int, int, list[dict[str, Any]]]:
    """서버 응답의 per-event status 표에 따라 결과를 처리한다.

    Returns:
        (resent, quarantined, keep_list)
        - resent      : accepted·duplicated 이벤트 수
        - quarantined : SEQ_CONFLICT·INVALID_EVENT(형식 오류) 로 격리한 이벤트 수(삭제 아님)
        - keep_list   : SEQ_GAP·PREV_HASH_MISMATCH·저장 오류·결과 없음/부분응답 → 재시도 대상(내부 필드 포함)

    ★ E-01(1001-543) = "불명확 ACK 는 보존한다" — 결과 목록이 없다고(빈 본문·잘린 JSON)
      **전체 accepted** 로 넘겨짚지 않는다(예전 동작 · transport.py `_handle_response`
      와 짝이 안 맞았다). 서버가 실제로 무엇을 저장했는지 모르면 보관해 재시도한다 —
      이미 저장된 이벤트는 재전송에서 event_id 기준 duplicated 로 흡수되어 중복 적재가
      나지 않는다.
    """
    data = resp_body.get("data", {}) if isinstance(resp_body, dict) else {}
    results: list[dict[str, Any]] = []
    if isinstance(data, dict):
        results = data.get("results", [])

    if not results:
        # ★ E-01 = 결과 목록 없음 → 전체 **보관**(accepted 간주 금지).
        return 0, 0, list(original)

    # event_id → 원본 이벤트(내부 필드 포함) 매핑
    id_map: dict[str, dict[str, Any]] = {
        evt["event_id"]: evt
        for evt in original
        if "event_id" in evt
    }

    resent = 0
    quarantined = 0
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
            resent += 1

        elif status == "rejected":
            message = str(r.get("message", ""))
            if code in _RETRY_CODES or (
                code == "INVALID_EVENT" and message.startswith(_STORE_FAIL_PREFIX)
            ):
                # 재시도 대상(내부 필드 포함해 보관 — 다음 회차에 다시 trace 그룹핑해야 하므로)
                if evt:
                    keep.append(evt)
                if code == "INVALID_EVENT":
                    print(
                        f"[resend]   서버 저장 실패 event_id={_safe(eid)} → 재시도 대상",
                        flush=True,
                    )
            elif code in _DROP_CODES or code == "INVALID_EVENT":
                # ★ Q-06 = 영구 실패는 **격리**한다(삭제 아님) — quarantine 은 원본(봉투 필드 포함)을 남긴다.
                quarantined += 1
                if evt:
                    _spool.quarantine([evt], str(spool_dir), reason=f"{code} {message}"[:200])
                print(
                    f"[resend]   영구실패 {_safe(code, 40)} event_id={_safe(eid)} → 격리",
                    flush=True,
                )
            else:
                quarantined += 1
                if evt:
                    _spool.quarantine([evt], str(spool_dir), reason=f"알 수 없는 code={code}")
                print(
                    f"[resend]   알 수 없는 reject code={_safe(code, 40)} event_id={_safe(eid)} → 격리",
                    flush=True,
                )
        else:
            # 모르는 status — rejected 도 accepted 도 아니면 보관(모르면 보관 쪽).
            if evt:
                keep.append(evt)

    # ★ E-01 = results 표에 아예 없는(서버가 어떻게 했는지 모르는) 이벤트도 전부 보관한다
    #   (부분 응답 — 요청 N건에 서버가 결과를 M<N 건만 돌려준 경우).
    for evt in original:
        eid = evt.get("event_id")
        if eid and eid not in addressed:
            keep.append(evt)

    return resent, quarantined, keep


def _post_slice(
    endpoint: str,
    api_key: str,
    trace_id: str,
    agent_id: str | None,
    trace_events: list[dict[str, Any]],
    spool_dir: Path,
) -> tuple[int, int, list[dict[str, Any]]]:
    """한 조각을 전송하고, HTTP 레벨 실패(413·429·5xx 등)까지 포함해 (resent, quarantined, keep) 로 정규화한다.

    ★ Q-06 = 413 은 **분할 후 재시도**한다(삭제하지 않는다). 1건짜리도 413 이면 그 이벤트
      자체가 너무 크다는 뜻이므로 격리한다.
    """
    api_events = [_strip_internal(e) for e in trace_events]
    status_code, resp_body, _headers = _send_envelope(endpoint, api_key, trace_id, agent_id, api_events)

    if status_code == 0:
        print(f"[resend]   연결 실패 — trace {trace_id[:12]}... 재시도 대상 유지", flush=True)
        return 0, 0, list(trace_events)

    if status_code in (200, 202, 207, 409):
        return _process_results(resp_body, trace_events, spool_dir)

    if status_code in (401, 403, 402, 429) or status_code >= 500:
        # 키·요금제·미납·한도·서버 오류 = 전부 회복 가능 → 재시도 대상 유지(격리 아님)
        print(f"[resend]   HTTP {status_code} — 재시도 대상 유지", flush=True)
        return 0, 0, list(trace_events)

    if status_code == 413:
        if len(trace_events) > 1:
            mid = len(trace_events) // 2
            print(
                f"[resend]   HTTP 413 — {len(trace_events)}건을 분할 재시도", flush=True)
            r1, q1, k1 = _post_slice(endpoint, api_key, trace_id, agent_id, trace_events[:mid], spool_dir)
            r2, q2, k2 = _post_slice(endpoint, api_key, trace_id, agent_id, trace_events[mid:], spool_dir)
            return r1 + r2, q1 + q2, k1 + k2
        print("[resend]   HTTP 413 — 단일 이벤트 초과, 격리", flush=True)
        _spool.quarantine(list(trace_events), str(spool_dir), reason="HTTP 413 단일 이벤트 초과")
        return 0, 1, []

    if status_code == 400:
        print(f"[resend]   HTTP 400 형식 오류 — {len(trace_events)}건 격리", flush=True)
        _spool.quarantine(list(trace_events), str(spool_dir), reason="HTTP 400")
        return 0, len(trace_events), []

    print(f"[resend]   HTTP {status_code} 예상 외 코드 — 격리", flush=True)
    _spool.quarantine(list(trace_events), str(spool_dir), reason=f"HTTP {status_code}")
    return 0, len(trace_events), []


# ── 메인 재전송 로직 ──────────────────────────────────────────────────────────

def resend_spool(
    endpoint: str,
    api_key: str,
    spool_dir: Path,
    dry_run: bool = False,
    all_owners: bool = False,
    include_quarantine: bool = False,
) -> dict:
    """스풀의 세그먼트를 claim 해 순서대로 재전송한다.

    ★ dry-run = **아무 것도 지우거나 바꾸지 않는다**(GX-12 재발 방지). claim 조차 하지 않는다
      — 목록만 읽어 몇 건인지 출력한다.
    ★ M-09(1001-543) = `all_owners=True` 는 "다른 키의 기록을 지금 키로 보낸다"는 뜻이
      **아니다**(그러면 오귀속·유출이다) — 다른 소유자 세그먼트가 보이면 **보내지 않고**
      그 키로 다시 실행하라고 알린 뒤 건너뛴다(claim 은 지우지 않아 다음 회차가 다시 본다).
    """
    current_owner = _spool.owner_fingerprint(api_key, endpoint)
    owner = None if all_owners else current_owner

    if not spool_dir.exists():
        print(f"[resend] 스풀 디렉터리 없음: {spool_dir}", flush=True)
        return {"resent": 0, "quarantined": 0, "failed": 0, "remaining": 0, "legacy_unowned": 0}

    if dry_run:
        segments = _spool.list_segments(str(spool_dir), owner)
        stale = _spool.list_stale_claims(str(spool_dir), owner)
        total = 0
        for seg in segments + stale:
            try:
                total += sum(1 for ln in seg.read_text(encoding="utf-8", errors="replace").splitlines() if ln.strip())
            except BaseException:
                pass
        # ★ Z1(546-이어2) = dry-run 경로도 legacy_unowned 건수를 함께 돌려준다(보존소는
        #   claim 과 무관하게 조회 전용이라 dry-run 이라고 계산을 건너뛸 이유가 없다).
        legacy_unowned = _spool.legacy_unowned_count(str(spool_dir))
        print(
            f"[resend] (dry-run) 세그먼트 {len(segments) + len(stale)}개 · 이벤트 약 {total}건 · "
            f"레거시 보존 {legacy_unowned}건 — 아무 것도 바꾸지 않았습니다.",
            flush=True,
        )
        return {"resent": 0, "quarantined": 0, "failed": 0, "remaining": total, "legacy_unowned": legacy_unowned}

    # ★ M-29 재정정(Y1·Z2·546-이어2) = 1.0.1 형식 단일 스풀 파일(events.jsonl)이 있으면
    #   지금 키로 귀속시키지 않고 legacy_unowned/ 로 보존만 한다(1.0.1 파일에는 원래 키
    #   표식이 없다 — spool.py 703절) — 아래 일반 처리 경로는 이 보존분을 집어들지 않는다.
    try:
        _spool.migrate_legacy_events_file(str(spool_dir), current_owner)
    except BaseException:
        pass

    total_resent = 0
    total_quarantined = 0
    total_remaining = 0

    # ① 지난 회수자가 claim 만 하고 끝내지 못한 파일 — 온전한 내용이므로 그대로 이어 처리한다.
    pending = list(_spool.list_stale_claims(str(spool_dir), owner))
    # ② 아직 claim 되지 않은 세그먼트 — claim 해서 소유를 확정한다.
    for seg in _spool.list_segments(str(spool_dir), owner):
        claimed = _spool.claim_segment(seg)
        if claimed is not None:
            pending.append(claimed)

    for claimed in pending:
        if all_owners:
            seg_owner = _spool.owner_of_segment(claimed)
            if seg_owner != current_owner:
                print(
                    f"[resend] {claimed.name}: 다른 키(owner={seg_owner}) 소속입니다 — "
                    "그 키의 환경에서 다시 실행하세요(건너뜀 · 지금 키로 보내지 않습니다).",
                    flush=True,
                )
                continue  # claim 을 지우지 않는다 — 다음 stale 회수에서 그 키가 다시 본다

        events = _spool.read_claimed(claimed)
        if not events:
            _spool.discard_claim(claimed)
            continue

        print(f"[resend] {claimed.name}: {len(events)}건 처리 중...", flush=True)

        groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for evt in events:
            tid = evt.get("_trace_id") or "unknown"
            groups[tid].append(evt)

        file_keep: list[dict[str, Any]] = []
        file_resent = 0
        file_quarantined = 0

        for trace_id, trace_events in groups.items():
            trace_events.sort(key=lambda e: e.get("seq", 0))
            agent_id: str | None = trace_events[0].get("_agent_id") if trace_events else None
            short_tid = trace_id[:12] if len(trace_id) > 12 else trace_id
            print(f"[resend]   trace={short_tid}... {len(trace_events)}건 전송 중", flush=True)

            # ★ M-24 = transport.py 와 같은 분할기(요청당 ≤ _MAX_EVENTS_PER_POST 건).
            for start in range(0, len(trace_events), _MAX_EVENTS_PER_POST):
                slice_events = trace_events[start:start + _MAX_EVENTS_PER_POST]
                resent, quarantined, keep = _post_slice(
                    endpoint, api_key, trace_id, agent_id, slice_events, spool_dir
                )
                file_resent += resent
                file_quarantined += quarantined
                file_keep.extend(keep)
                time.sleep(0.1)  # Rate limit 회피

        # ★ S-05 재발 방지 = keep 을 **새 세그먼트로 먼저 안전하게 쓴 뒤에만** claim 원본을 지운다.
        #   (예전에는 원본을 먼저 지우고 같은 이름으로 다시 썼다 — 그 사이 실패하면 전량 유실.)
        # ★ E-02(1001-543) = 재보관(append) 자체가 실패하면(디스크 오류 등) **원본 claim 을
        #   지우지 않는다** — 지우면 새 세그먼트도 없고 원본도 없어 완전히 유실된다.
        reappended_ok = True
        if file_keep:
            reappended_ok = _spool.append(
                file_keep, str(spool_dir), max_bytes=1 << 62, owner=owner or current_owner
            )
        if reappended_ok:
            _spool.discard_claim(claimed)
        else:
            print(
                f"[resend] {claimed.name}: 재보관 실패(디스크 오류) — 원본 claim 보존, "
                "다음 실행에서 다시 시도합니다.",
                flush=True,
            )

        total_resent += file_resent
        total_quarantined += file_quarantined
        total_remaining += len(file_keep)

        print(
            f"[resend] {claimed.name} 완료: resent={file_resent} quarantined={file_quarantined} remaining={len(file_keep)}",
            flush=True,
        )

    if not pending:
        print("[resend] 재전송할 세그먼트 없음", flush=True)

    # ★ M-16 = `--include-quarantine` = 격리 보관분도 다시 보낸다(성공분만 격리에서 지운다).
    if include_quarantine:
        for qf in _spool.list_quarantine_files(str(spool_dir)):
            items = _spool.read_quarantine_file(qf)
            if not items:
                continue
            events = [it.get("event") for it in items if isinstance(it.get("event"), dict)]
            groups = defaultdict(list)
            for evt in events:
                tid = evt.get("_trace_id") or "unknown"
                groups[tid].append(evt)
            still_quarantined: list[dict[str, Any]] = []
            qf_resent = 0
            qf_quarantined = 0
            for trace_id, trace_events in groups.items():
                trace_events.sort(key=lambda e: e.get("seq", 0))
                agent_id = trace_events[0].get("_agent_id") if trace_events else None
                for start in range(0, len(trace_events), _MAX_EVENTS_PER_POST):
                    slice_events = trace_events[start:start + _MAX_EVENTS_PER_POST]
                    resent, quarantined, keep = _post_slice(
                        endpoint, api_key, trace_id, agent_id, slice_events, spool_dir
                    )
                    qf_resent += resent
                    qf_quarantined += quarantined
                    still_quarantined.extend(
                        {"reason": "resend --include-quarantine 재시도 보류", "event": e}
                        for e in keep
                    )
            _spool.rewrite_quarantine_file(qf, still_quarantined)
            total_resent += qf_resent
            total_quarantined += qf_quarantined
            total_remaining += len(still_quarantined)
            print(
                f"[resend] (격리){qf.name} 완료: resent={qf_resent} "
                f"quarantined={qf_quarantined} remaining={len(still_quarantined)}",
                flush=True,
            )

    # ★ U-12(1002-546) = 1.0.1 공개 계약의 `failed` 키를 복원한다(1.0.2 가 `quarantined`
    #   로 이름을 바꾼 뒤 `result['failed']` 를 읽는 1.0.1 고객 코드가 깨졌다 — GPT C2).
    #   값은 `quarantined` 과 같다(영구 실패 = 격리) — 두 키 모두 그대로 제공한다.
    # ★ Z1(546-이어2 · A-546-02) = `remaining`(이번 회차에서 다시 보낼 것)과 보존된
    #   레거시 미전송 건수는 서로 다른 뜻이다 — `remaining=0` 이어도 legacy_unowned 가
    #   남아 있으면 "전부 처리됨"이 아니다. 그래서 별도 키로 항상 함께 돌려준다.
    return {
        "resent": total_resent,
        "quarantined": total_quarantined,
        "failed": total_quarantined,
        "remaining": total_remaining,
        "legacy_unowned": _spool.legacy_unowned_count(str(spool_dir)),
    }


def main() -> int:
    for _stream in (sys.stdout, sys.stderr):
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(
        description="xsoft comply SDK 스풀 재전송",
        prog="python -m xsoft_comply.resend",
    )
    parser.add_argument(
        "--api-key-env",
        default="XSOFT_COMPLY_API_KEY",
        help="API Key 를 담은 환경변수 이름 (원문을 인자로 넘기지 않는다)",
    )
    parser.add_argument(
        "--endpoint",
        required=True,
        # ★ M-13(1001-543) = 기본 엔드포인트를 두지 않는다. 예전 기본값(로컬 전용 주소)이
        #   운영 환경에서 그대로 남아 조용히 로컬로만 보내는 사고를 막는다 — 매번 명시한다.
        help="Event API 기본 URL (필수 — 기본값 없음)",
    )
    parser.add_argument(
        "--spool",
        default=str(Path.home() / ".xsoft_comply" / "spool"),
        help="스풀 디렉터리 경로",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="실제 전송 없이 파일 목록만 출력(아무 것도 지우거나 바꾸지 않는다)",
    )
    parser.add_argument(
        "--all-owners",
        action="store_true",
        help="현재 키가 아닌 다른 키로 쌓인 세그먼트까지 살펴본다(그 키로는 여전히 보내지 않는다 — 안내만).",
    )
    parser.add_argument(
        "--include-quarantine",
        action="store_true",
        help="격리(quarantine) 보관분도 다시 보낸다(성공분만 격리에서 지운다).",
    )
    args = parser.parse_args()

    api_key = os.environ.get(args.api_key_env, "")
    if not api_key and not args.dry_run:
        print(
            f"[resend] 오류: 환경변수 {args.api_key_env} 가 비어 있습니다.",
            file=sys.stderr,
        )
        return 1

    spool = Path(args.spool).expanduser().resolve()
    print(f"[resend] 스풀={spool} endpoint={args.endpoint}", flush=True)

    result = resend_spool(
        args.endpoint, api_key, spool, dry_run=args.dry_run, all_owners=args.all_owners,
        include_quarantine=args.include_quarantine,
    )
    legacy_unowned = result.get("legacy_unowned", 0)
    # CLI 출력 형식: resent=N quarantined=M failed=M remaining=K legacy_unowned=L
    print(
        f"resent={result['resent']} quarantined={result['quarantined']} "
        f"failed={result['failed']} remaining={result['remaining']} "
        f"legacy_unowned={legacy_unowned}",
        flush=True,
    )
    # ★ Z1(546-이어2 · A-546-02) = 명령을 적지 않는다 — 보존 사실과 안내 창구만.
    if legacy_unowned >= 1:
        print(
            f"[resend] 1.0.1 형식 미전송 기록 {legacy_unowned}건이 보존돼 있습니다 · "
            "어느 키의 기록인지 알 수 없어 자동·수동 전송하지 않습니다 · "
            "원래 키를 확인한 복구는 xsoft comply 지원 창구로 문의하세요",
            flush=True,
        )
    # ★ M-27(1001-543) = 남은 건(재시도 대상 · 연결 실패 포함)이 있으면 종료코드 1.
    #   dry-run 은 "약 N건" 을 remaining 에 그대로 담아 보고용이므로 실패로 보지 않는다.
    if args.dry_run:
        return 0
    if result.get("remaining", 0) > 0:
        return 1
    # ★ Z1 = `remaining=0`(이번 회차 재시도 대상 없음)을 "전부 처리됨"으로 읽지 못하게
    #   레거시 보존분이 남아 있으면 **별도 종료코드 3**으로 구분한다(A-546-02 재발 방지).
    if legacy_unowned >= 1:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
