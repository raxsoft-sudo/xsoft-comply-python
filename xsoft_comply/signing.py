"""전자서명 — 순서 무결성 기록에 고객키 서명을 더한다.

★ 무엇을 증명하는가
  서버 hash-chain은 «서버에 들어온 뒤로 변조되지 않았다» 를 증명한다.
  그러나 «그 이벤트를 정말 그 고객 에이전트가 만들었는가» 는 증명하지 못한다.
  전자서명은 그 서명 주체 확인을 지원한다 = 개인키를 가진 SDK 만 만들 수 있는 값을 이벤트에 붙인다.

★ 서명 대상 (canonical) — 서버가 손대는 값은 넣지 않는다
    trace_id | seq | event_id | type | occurred_at |
    model | tool | action | decision | human_review | policy_result |
    sha256(metadata 직렬화) | prev_sig
  metadata 는 서명 필드(_sig·_pk·_psig·_mh·_sig_alg)와 **서버가 붙이는 필드**
  (_event_id·_pii_masked·_evidence)를 뺀 나머지를 키 정렬 직렬화해 sha256 한다.
  → 서버가 _event_id 를 붙여도 서명 검증은 실패하지 않고, metadata 본문이 바뀌면 검증에 실패한다.

★ 주의 = 옛 이벤트 타입의 «하위호환 변환» 과 서명은 함께 쓰지 않아야 한다.
  이전 판 SDK 가 `decision`·`human_review` 를 서명해 보내면 서버는 `action` 으로 바꿔 저장한다
  → canonical 의 event_type 이 달라져 **그 이벤트만** 서명 검증이 어긋난다.
  서명(`sign=True`)을 쓰는 고객은 SDK 를 현재 판으로 올려야 한다(순서 무결성 기록은 영향 없다).

★ prev_sig 로 클라이언트 체인을 만든다. 프로세스가 재시작하면 그 자리에서 새 구간이 시작된다
  (검증기는 «구간 시작» 으로 표시한다 — 구간 경계를 그대로 드러낸다).

★ 서명 위치 = 백그라운드 전송 작업(transport). 호출 스레드의 부담을 늘리지 않는다.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import ed25519

SIG_ALG = "ed25519"

# 서명 계산에서 제외하는 metadata 키
#   앞 5개 = 서명 자신 · 뒤 2개 = 서버가 저장 시 덧붙이는 값
# ★ 서명 대상에서 빼는 metadata 키 = **서버가 넣거나 바꾸는 것들**이다.
#   클라이언트가 서명한 뒤에 서버가 추가하는 값을 해시에 넣으면, 정상 서명이 전건 «변조» 로 보인다.
#   `_evidence` = 조항 증적 블록. 서버가 화이트리스트로 거르고 PII 마스킹까지 한
#   **정규화 산물**이라 클라 서명 시점의 값과 같다는 보장이 없다.
#   ★ 그렇다고 무결성 밖에 있는 게 아니다 = `_evidence` 는 서버 hash-chain(payload_hash · append-only)이
#     덮는다. ed25519 서명은 «누가 보냈나», hash-chain 은 «바뀌지 않았나» 를 맡는다.
EXCLUDED_META_KEYS = frozenset({
    "_sig", "_pk", "_psig", "_mh", "_sig_alg", "_event_id", "_pii_masked", "_evidence",
})

_lock = threading.Lock()
_seed: bytes | None = None
_pub: bytes | None = None
_prev_sig: dict[str, str] = {}   # trace_id → 직전 서명(base64)


# ── 키 관리 ───────────────────────────────────────────────────────────────────

def _key_path(spool_dir: str) -> Path:
    return Path(spool_dir).expanduser().parent / "signing_key"


def load_or_create(seed_b64: str | None, spool_dir: str) -> bytes:
    """개인키 시드를 확보한다.

    우선순위 = ① init(signing_key=...) 로 준 값 ② 로컬 키파일 ③ 새로 생성해 저장(0600).
    ★ 시드 원문은 반환만 하고 로그·예외에 싣지 않는다.
    """
    if seed_b64:
        raw = base64.b64decode(seed_b64 + "=" * (-len(seed_b64) % 4))
        if len(raw) != 32:
            raise ValueError("signing_key 는 base64 로 인코딩한 32바이트여야 합니다.")
        return raw
    path = _key_path(spool_dir)
    if path.exists():
        raw = base64.b64decode(path.read_text(encoding="utf-8").strip() + "==")
        if len(raw) == 32:
            return raw
    seed = os.urandom(32)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(base64.b64encode(seed).decode().rstrip("="), encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    return seed


def init(seed_b64: str | None, spool_dir: str) -> str:
    """서명기를 준비하고 공개키(base64)를 반환한다."""
    global _seed, _pub
    with _lock:
        _seed = load_or_create(seed_b64, spool_dir)
        _pub = ed25519.public_key(_seed)
        _prev_sig.clear()
        return b64(_pub)


def public_key_b64() -> str | None:
    return b64(_pub) if _pub else None


def reset() -> None:
    global _seed, _pub
    with _lock:
        _seed = None
        _pub = None
        _prev_sig.clear()


# ── canonical / 해시 ──────────────────────────────────────────────────────────

def b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode().rstrip("=")


def unb64(text: str) -> bytes:
    return base64.b64decode(text + "=" * (-len(text) % 4))


def _js_normalize(value: Any) -> Any:
    """JS 숫자 표기에 맞춘다 — `12.0` → `12`.

    ★ 왜 필요한가
      서버(Next.js)가 metadata 를 `JSON.stringify` 로 다시 직렬화해 DB 에 넣는다.
      JS 에는 int/float 구분이 없어 `12.0` 이 `12` 로 저장된다.
      파이썬이 `12.0` 으로 해시하면, 서버를 거친 뒤 읽어온 값과 계속 어긋난다
      (= 정상 서명이 전건 «변조» 로 보인다). 그래서 서명 해시도 JS 표기를 기준으로 맞춘다.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, float) and value.is_integer() and abs(value) < 1e21:
        return int(value)
    if isinstance(value, dict):
        return {k: _js_normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_js_normalize(v) for v in value]
    return value


def _stable_json(value: Any) -> str:
    """키 정렬 · 공백 없음 직렬화 (JS JSON.stringify 와 같은 모양)."""
    return json.dumps(
        _js_normalize(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def metadata_hash(meta: dict[str, Any] | None) -> str:
    body = {k: v for k, v in (meta or {}).items() if k not in EXCLUDED_META_KEYS}
    return hashlib.sha256(_stable_json(body).encode("utf-8")).hexdigest()


def iso_utc_ms(raw: str) -> str:
    """DB(timestamptz) 가 돌려주는 시각을 SDK 가 보냈던 `toISOString()` 형식으로 복원한다.

    ★ 이 복원이 없으면 «+00:00» 과 «Z» 표기 차이만으로 서명 검증이 전건 실패한 것처럼 보인다.
    """
    text = str(raw).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def canonical(
    *,
    trace_id: str,
    seq: int,
    event_id: str,
    event_type: str,
    occurred_at: str,
    model: str | None,
    tool: str | None,
    action: str | None,
    decision: str | None,
    human_review: str | None,
    policy_result: str | None,
    meta_hash: str,
    prev_sig: str,
) -> bytes:
    parts = [
        trace_id, str(seq), event_id, event_type, occurred_at,
        model or "", tool or "", action or "", decision or "",
        human_review or "", policy_result or "", meta_hash, prev_sig,
    ]
    return "|".join(parts).encode("utf-8")


# ── 서명 / 검증 ───────────────────────────────────────────────────────────────

def sign_event(evt: dict[str, Any]) -> bool:
    """이벤트 dict 의 metadata 에 서명 필드를 **제자리로** 넣는다.

    제자리 수정인 이유 = 전송 실패 시 스풀에 남는 것도 서명된 이벤트여야 하기 때문이다
    (재전송해도 같은 서명이 따라간다).
    반환 = 서명했으면 True.
    """
    if _seed is None or _pub is None:
        return False
    meta = evt.get("metadata")
    if not isinstance(meta, dict):
        meta = {}
        evt["metadata"] = meta
    trace_id = str(evt.get("_trace_id") or "")
    prev = _prev_sig.get(trace_id, "")
    mh = metadata_hash(meta)
    msg = canonical(
        trace_id=trace_id,
        seq=int(evt.get("seq") or 0),
        event_id=str(evt.get("event_id") or ""),
        event_type=str(evt.get("type") or ""),
        occurred_at=str(evt.get("occurred_at") or ""),
        model=evt.get("model"),
        tool=evt.get("tool"),
        action=evt.get("action"),
        decision=evt.get("decision"),
        human_review=evt.get("human_review"),
        policy_result=evt.get("policy_result"),
        meta_hash=mh,
        prev_sig=prev,
    )
    sig = b64(ed25519.sign(msg, _seed, _pub))
    meta["_sig_alg"] = SIG_ALG
    meta["_pk"] = b64(_pub)
    meta["_mh"] = mh
    meta["_psig"] = prev
    meta["_sig"] = sig
    # ★ M-11(1001-543) = session_end 는 그 trace 의 마지막 이벤트다. 서명 뒤에도
    #   `_prev_sig[trace_id]` 를 남겨두면 끝난 trace 가 영원히 메모리에 쌓인다
    #   (오래 사는 프로세스에서 trace 수만큼 자라는 누수) — 여기서 그 자리를 지운다.
    #   재시작하면 어차피 새 구간으로 보므로(모듈 docstring) 끝난 trace 를 남겨둘
    #   이유가 없다.
    if str(evt.get("type")) == "session_end":
        _prev_sig.pop(trace_id, None)
    else:
        _prev_sig[trace_id] = sig
    return True


def verify_row(row: dict[str, Any], prev_sig_expected: str | None) -> tuple[bool, str]:
    """보관된 기록 한 건을 검증한다. (ok, 사유) 반환.

    row 는 이벤트 필드 이름을 그대로 쓴다(trace_id·event_seq·event_type·metadata …).
    prev_sig_expected = 직전 행의 _sig. None 이면 체인 시작으로 본다.
    """
    meta = row.get("metadata") or {}
    if not isinstance(meta, dict) or "_sig" not in meta:
        return False, "서명 없음"
    if meta.get("_sig_alg") != SIG_ALG:
        return False, f"알 수 없는 알고리즘 {meta.get('_sig_alg')!r}"
    pub = unb64(str(meta.get("_pk", "")))
    if len(pub) != 32:
        return False, "공개키 형식 오류"
    mh = metadata_hash(meta)
    if mh != meta.get("_mh"):
        return False, "metadata 변조(_mh 불일치)"
    if prev_sig_expected is not None and meta.get("_psig") != prev_sig_expected:
        return False, "체인 불연속(_psig 불일치)"
    occurred = iso_utc_ms(row.get("occurred_at") or "1970-01-01T00:00:00.000Z")
    msg = canonical(
        trace_id=str(row.get("trace_id") or ""),
        seq=int(row.get("event_seq") or 0),
        event_id=str(meta.get("_event_id") or ""),
        event_type=str(row.get("event_type") or ""),
        occurred_at=occurred,
        model=row.get("model"),
        tool=row.get("tool"),
        action=row.get("action"),
        decision=row.get("decision"),
        human_review=row.get("human_review"),
        policy_result=row.get("policy_result"),
        meta_hash=mh,
        prev_sig=str(meta.get("_psig", "")),
    )
    ok = ed25519.verify(unb64(str(meta["_sig"])), msg, pub)
    return ok, "" if ok else "서명 불일치"
