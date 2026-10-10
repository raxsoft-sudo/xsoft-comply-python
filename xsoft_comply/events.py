"""이벤트 dict 조립 · event_id 생성.

원문 미전송(다중 검증):
  - log_llm_content=True여도 원문을 metadata에 넣지 않는다.
  - 대신 content_len · content_sha256_8(sha256 앞 8자)만 포함한다.
  - 금지 metadata 키는 SDK에서도 보내지 않는다.

설계 노트:
  - seq 관리는 Session이 담당한다(session.py). events.py는 seq를 인자로 받는다.
  - 금지 키 제거 후 PII 마스킹 순서로 처리한다.
  - _trace_id·_agent_id·_product_id는 transport가 그루핑에 사용하는 내부 필드다.
    transport가 API 요청 시 제거한다.
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from . import config as _config
from . import evidence as _evidence
from ._time import kst_date, kst_now
from .masking import apply as _mask_pii

# ── 금지 metadata 키 ────────────────────────────────────────────
#   ★ S-17(2026-09-29 최소 수리) = 정확 일치만 본다는 한계는 그대로다(자유 텍스트 키 이름을
#     전부 탐지한다고 보장하지 않는다). 다만 실제로 자주 쓰이는 별칭 몇 개가 빠져 있어
#     `user_input`·`question` 같은 흔한 이름은 그대로 통과했다 — 그 별칭들을 추가한다.
_FORBIDDEN_RAW: frozenset[str] = frozenset({
    "prompt", "prompt_text", "response", "response_text", "completion",
    "input", "output", "content", "contents", "messages", "message",
    "text", "query", "answer", "tool_input", "tool_output", "params",
    "result", "raw", "body", "system_prompt",
    # ★ S-17 추가 별칭(최소 수리 · 흔히 쓰이는 이름만) —
    "user_input", "user_message", "assistant_message", "question",
    "reply", "chat_history", "conversation", "transcript", "utterance",
})

# 키 이름에서 «사람 눈에 안 보이는 차이» 를 지운다 = 공백·제로폭·제어문자·구분자 제거 +
# NFKC 정규화(전각 ｐ → p) + 소문자.
_KEY_NOISE = re.compile(r"[\s\u200b-\u200d\ufeff\u0000-\u001f._\-]+")


def _norm_key(k: str) -> str:
    """인코딩 우회 차단.

    단순 소문자 비교만으로는 « prompt»(앞 공백)·«ｐrompt»(전각 p) 같은 변형이 걸러지지
    않아 값에 원문이 실려 올 수 있다.
    비교 전에 눈에 안 보이는 차이를 없애고, 금지 목록도 같은 규칙으로 정규화해 맞춘다.
    """
    return _KEY_NOISE.sub("", unicodedata.normalize("NFKC", k)).lower()


_FORBIDDEN_KEYS: frozenset[str] = frozenset(_norm_key(k) for k in _FORBIDDEN_RAW)


# ── 유틸리티 ──────────────────────────────────────────────────────────────────

def new_event_id() -> str:
    """ev_<uuid4 hex> 형식 중복 방지 키 생성."""
    return f"ev_{uuid.uuid4().hex}"


def now_iso() -> str:
    """현재 UTC 시각 ISO8601 밀리초 형식."""
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def retain_until(days: int) -> str:
    """보관 만료일(**한국 날짜 KST**). 보관기간 설정값이 그대로 반영된다.

    ★ 이 값은 «언제까지 보관하기로 계약했는가» 의 기록이다. 실제 삭제는 서버측 정리 작업이 한다
      (`python -m xsoft_comply.retention` 이 대상 산정·감사용 보고서를 만든다).
    ★ KST 기준인 이유 = 이것은 절대시각이 아니라 사람이 읽는 **날짜**다.
      UTC 날짜로 뽑으면 KST 00:00~09:00 에 기록된 이벤트만 만료일이 **하루 일찍** 찍혀
      같은 날 기록끼리 만료일이 갈라진다. 절대시각(`now_iso`·서명 스탬프)은 UTC 그대로다.
    """
    return kst_date(kst_now() + timedelta(days=int(days)))



# ── 직렬화 안전화 ────────────────────────────

_JSON_SAFE_MAX_DEPTH = 12


def json_safe(value: Any, _depth: int = 0, _seen: "set[int] | None" = None) -> Any:
    """metadata 를 **반드시 JSON 으로 변할 수 있는 값**으로 바꾼다.

    ★ 왜 필요한가
      metadata 에 순환참조·bytes·NaN·`__repr__` 예외 객체가 하나라도 섞이면
      전송 직전 `json.dumps` 가 실패해, 같은 묶음의 정상 이벤트까지 전송되지 못할 수 있다.

      그래서 **이벤트를 만드는 그 자리에서** 안전한 값으로 바꾼다.
      직렬화는 전송 경로의 마지막에 하는 «별개의 일» 이 아니라, SDK 가 기록을 받을 때
      지켜야 하는 규칙이다. 못 바꾸는 값은 «버리지 않고» 타입 이름만 남긴다
      = 원문은 어차피 저장하지 않으므로 잃는 정보가 없다.
    """
    if _seen is None:
        _seen = set()
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        # NaN·Infinity 는 표준 JSON 이 아니다 → 서버 파서가 거부한다
        return value if -1e308 < value < 1e308 and value == value else None
    if isinstance(value, (bytes, bytearray)):
        # 원문을 되살리지 않는다 = 길이만 남긴다
        return f"<bytes len={len(value)}>"
    if _depth >= _JSON_SAFE_MAX_DEPTH:
        return "<깊이초과>"
    if isinstance(value, dict):
        if id(value) in _seen:
            return "<순환참조>"
        _seen = _seen | {id(value)}
        out: dict[str, Any] = {}
        for k, v in value.items():
            try:
                key = k if isinstance(k, str) else str(k)
            except BaseException:
                key = "<키변환실패>"
            out[key] = json_safe(v, _depth + 1, _seen)
        return out
    if isinstance(value, (list, tuple, set, frozenset)):
        if id(value) in _seen:
            return "<순환참조>"
        _seen = _seen | {id(value)}
        return [json_safe(v, _depth + 1, _seen) for v in value]
    # 그 밖의 객체 = str() 를 시도하고, 그것도 실패하면 타입 이름만 남긴다
    try:
        return str(value)
    except BaseException:
        try:
            return f"<{type(value).__name__}>"
        except BaseException:
            return "<직렬화불가>"


# ── 금지 키 제거 (재귀) ─────────────────────────────────────────────────────────

def _strip_forbidden(
    meta: dict[str, Any],
    depth: int = 0,
) -> tuple[dict[str, Any], list[str]]:
    """금지 키를 재귀적으로 제거한다. (cleaned, dropped_keys) 반환.

    ★ 리스트 안까지 들어간다.
      `{"arr": [{"output": "<원문>"}]}` 처럼 리스트로 감싼 값도 거른다.
      서버가 한 번 더 거르지만, SDK 가 «애초에 원문을 만들지 않는다» 는
      원칙이므로 클라이언트에서 먼저 지운다.
    """
    dropped: list[str] = []
    cleaned: dict[str, Any] = {}
    if depth > 10:                      # 무한 재귀 방어
        return dict(meta), dropped
    for k, v in meta.items():
        if _norm_key(k) in _FORBIDDEN_KEYS:
            dropped.append(k)
            continue
        cleaned[k], sub_dropped = _strip_value(v, depth + 1)
        dropped.extend(sub_dropped)
    return cleaned, dropped


def _strip_value(v: Any, depth: int) -> tuple[Any, list[str]]:
    """dict·list 를 가리지 않고 내려간다."""
    if depth > 10:
        return v, []
    if isinstance(v, dict):
        return _strip_forbidden(v, depth)
    if isinstance(v, (list, tuple)):
        out: list[Any] = []
        dropped: list[str] = []
        for item in v:
            cleaned_item, sub = _strip_value(item, depth + 1)
            out.append(cleaned_item)
            dropped.extend(sub)
        return out, dropped
    return v, []


def clean_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    """금지 키를 제거하고 `_dropped_keys` 에 **개수만** 기록한다.

    ★ 왜 이름이 아니라 개수인가
      제거한 키 이름을 그대로 남기면 그 이름 자체가 원문의 단서가 될 수 있다.
      서버 규약도 개수 기록이므로 SDK 를 거기에 맞춘다.
      감사에 필요한 것은 «몇 개를 제거했나» 이지 «무엇을 제거했나» 가 아니다.
    """
    cleaned, dropped = _strip_forbidden(meta)
    if dropped:
        existing = cleaned.get("_dropped_keys")
        cleaned["_dropped_keys"] = (existing if isinstance(existing, int) else 0) + len(dropped)
    return cleaned


# ── 원문 → 길이·해시 변환(다중 검증) ──────────────────────────────────

# ── metadata 크기 상한(M-02b) ────────────────────────────────────────────────
#   서버는 4,096바이트를 넘는 metadata 를 거절한다(INVALID_EVENT). SDK 가 미리
#   잘라내면 그 이벤트가 아예 유실되지 않고 일부라도(+ 잘려나간 사실) 전달된다.
_META_BYTE_LIMIT = 4096


def _meta_bytes(meta: dict[str, Any]) -> int:
    return len(json.dumps(meta, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _cap_metadata_bytes(meta: dict[str, Any], limit: int = _META_BYTE_LIMIT) -> dict[str, Any]:
    """4,096바이트를 넘으면 **큰 값부터** 제거해 서버 거절을 막는다(M-02b).

    내부 통제 키(`_` 로 시작 — `_sdk`·`_retain_until`·`_dropped_keys` 등)는 건드리지
    않는다. 작아서 예산에 거의 영향이 없고, 지우면 그 자체가 통제 정보를 잃는다.
    """
    try:
        if _meta_bytes(meta) <= limit:
            return meta
        candidates = sorted(
            (k for k in meta if not k.startswith("_")),
            key=lambda k: _meta_bytes({k: meta[k]}),
            reverse=True,
        )
        removed = 0
        for k in candidates:
            if _meta_bytes(meta) <= limit:
                break
            meta.pop(k, None)
            removed += 1
        if removed:
            meta["_truncated_keys"] = removed
        return meta
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return meta


def _content_fingerprint(content: Any) -> dict[str, Any]:
    """원문 대신 길이·sha256 앞 8자만 반환.

    log_llm_content=True여도 원문은 포함하지 않는다.
    """
    if content is None:
        return {}
    s = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()[:8]
    return {"content_len": len(s), "content_sha256_8": h}


# ── 이벤트 dict 조립 ──────────────────────────────────────────────────────────

def build(
    event_type: str,
    seq: int,
    trace_id: str,
    *,
    model: str | None = None,
    tool: str | None = None,
    action: str | None = None,
    decision: str | None = None,
    human_review: str | None = None,
    policy_result: str | None = None,
    metadata: dict[str, Any] | None = None,
    content: Any = None,     # 원문 → fingerprint만
    evidence: Any = None,    # 조항 증적 블록(배열)
    agent_id: str | None = None,
    product_id: Any | None = None,
) -> dict[str, Any]:
    """API 전송용 이벤트 dict를 조립한다.

    _trace_id·_agent_id·_product_id는 내부 routing 필드다.
    transport가 API 요청 조립 시 제거한다.

    ★ `evidence` = 조항(제31~36조) 표준필드 증적. **선택**이다.
      값이 있으면 서버가 이벤트를 기록한 뒤 조항 증적으로 분류해 보관한다.
      없으면 그 단계를 전부 건너뛴다(부하 방어) = 기존 동작 그대로다.
    ★ 최상위 `decision`·`human_review` 필드는 사용하지 않는다(항상 None 으로 나간다).
      서버는 하위 호환을 위해 아직 읽지만, 이 SDK 는 조항 증적으로만 보낸다.
    """
    cfg = _config.get()
    meta: dict[str, Any] = dict(metadata or {})

    # ① 원문 content → fingerprint (원문은 어떤 경로로도 저장·전송되지 않는다)
    if content is not None and cfg.log_llm_content:
        meta.update(_content_fingerprint(content))

    # ①-b ★ 직렬화 안전화 — 뒤 단계(금지키 제거·마스킹·전송)에서 예외가 나지 않도록 먼저 정리한다
    meta = json_safe(meta) if isinstance(meta, dict) else {}

    # ② 금지 키 제거
    meta = clean_metadata(meta)

    # ③ PII 마스킹
    meta = _mask_pii(meta)

    # ④ SDK 태그 · 보관기간(retention 애드온)
    meta.setdefault("_sdk", "xsoft_comply_python")
    meta.setdefault("_retain_until", retain_until(cfg.retention_days))

    # ⑤ 조항 증적 정규화 — 허용 목록 밖 키는 여기서 이미 제외된다.
    #    ★ 경고는 metadata 에 «개수만» 남긴다. 키 이름을 남기면 그것도 단서가 된다.
    evidence_blocks, evidence_warnings = _evidence.normalize(evidence)
    if evidence_warnings:
        meta["_evidence_warnings"] = len(evidence_warnings)

    # ⑤-b ★ M-03 = 증적 필드도 JSON 안전화한다(날짜 → ISO 문자열 · NaN·Infinity → 뺌).
    #   metadata(①-b)만 json_safe 를 거치고 evidence 는 그대로 나가 날짜·NaN 이 섞이면
    #   전송 직전 json.dumps 가 실패했다 — evidence 도 같은 규칙을 적용한다.
    if evidence_blocks:
        evidence_blocks = json_safe(evidence_blocks)

    # ⑤-c ★ M-02b = metadata 전체(증적 제외 · evidence 는 별도 top-level 키) 크기 상한.
    meta = _cap_metadata_bytes(meta)

    evt: dict[str, Any] = {
        "event_id": new_event_id(),
        "seq": seq,
        "type": event_type,
        "occurred_at": now_iso(),
        "model": model,
        "tool": tool,
        "action": action,
        "decision": decision,
        "human_review": human_review,
        "policy_result": policy_result,
        "metadata": meta,
        # 내부 라우팅 필드 — transport가 제거
        "_trace_id": trace_id,
        "_agent_id": agent_id,
        "_product_id": product_id,
    }
    if evidence_blocks:
        evt["evidence"] = evidence_blocks
    return evt
