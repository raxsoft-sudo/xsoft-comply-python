"""SDK 초기화 파라미터 보관.

기본 시그니처 기준. 값을 변경하려면 반드시 init()을 다시 호출한다.
api_key는 로그에 출력하지 않는다.

부가 기능 = 이벤트 종류별 로그 설정 · 전자서명 · 정책 기반 실행 차단 · 보관기간.

★ 보관기간(`retention_days`)은 `_retain_until` 기록·만료 산정까지 SDK 가 담당하고,
  만료분 실제 삭제는 기록이 추가 전용(append-only)이라 서버가 별도로 처리한다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

_logger = logging.getLogger("xsoft_comply")

# ★ 신규 발행 event_type = **6종**.
#   `decision`·`human_review` 는 여기 없다 = 제34조(고영향 책무) 조항 증적으로 기록되기 때문이다.
#   `session.decision()`·`session.human_review()` 메서드는 **개발자 편의로 그대로 제공한다** —
#   내부적으로 `action` + evidence(제34조) 블록으로 조립해 보낸다.
EVENT_TYPES: Tuple[str, ...] = (
    "session_start",
    "llm_call",
    "tool_call",
    "data_access",
    "action",
    "session_end",
)

# ★ 하위호환 = 이전 판 SDK 가 보내던 옛 타입. 서버가 거부하지 않고 action 으로 수용·변환한다.
LEGACY_EVENT_TYPES: Tuple[str, ...] = ("decision", "human_review")

# 보관기간 기본 = 5년(1825일). 추가 보관 옵션에 맞춰 설정한다.
RETENTION_DEFAULT_DAYS = 1825


@dataclass
class Config:
    api_key: str = ""
    endpoint: str = ""
    # ★ 기본 False = 원문 안 보냄. True 여도 당사 서버에는 원문 미전송(다중 검증).
    log_llm_content: bool = False
    spool_dir: str = "~/.xsoft_comply/spool"
    flush_interval: float = 1.0          # 배경 스레드 배치 주기(초)
    timeout: Tuple[float, float] = field(default_factory=lambda: (2.0, 3.0))
    max_queue: int = 10_000
    max_retries: int = 5
    max_spool_bytes: int = 32 * 1024 * 1024  # 32 MB

    # ── 이벤트 종류별 로그 설정 ───────────────────────────────────
    # {"tool_call": False} 처럼 이벤트 종류별로 끈다. 없으면 전부 켜짐.
    # ★ session_start/session_end 는 끌 수 없다 = trace 자체가 사라지면 감사기록이 성립하지 않는다.
    log_events: Dict[str, bool] = field(default_factory=dict)

    # ── 전자서명 ────────────────────────────────────────
    sign: bool = False
    signing_key: Optional[str] = None    # base64 32바이트 시드. 없으면 로컬 키파일 사용·생성

    # ── 정책 기반 실행 차단 ───────────────────────────────────
    policy_enforce: bool = True          # False = 평가·기록만 하고 실제로 막지는 않는다

    # ── 보관기간 ────────────────────────────
    retention_days: int = RETENTION_DEFAULT_DAYS

    # ── 전송 전 확인 dry_run ───────────────────────
    # True = **전송하지 않는다.** 조립된 전송 데이터·조항 연결·수집 항목·가림 결과를 로컬에 출력만 한다.
    # 도입 검토 단계에서 «무엇이 나가는가» 를 네트워크 없이 눈으로 확인하는 용도다.
    dry_run: bool = False

    def __repr__(self) -> str:
        # api_key 원문 노출 방지 — 끝 4자리만
        masked = f"****{self.api_key[-4:]}" if self.api_key else "<없음>"
        return (
            f"Config(api_key={masked!r}, endpoint={self.endpoint!r}, "
            f"log_llm_content={self.log_llm_content}, sign={self.sign}, "
            f"retention_days={self.retention_days}, dry_run={self.dry_run})"
        )

    def event_enabled(self, event_type: str) -> bool:
        """이벤트 종류별 on/off. 세션 경계 이벤트는 끌 수 없다."""
        if event_type in ("session_start", "session_end"):
            return True
        value = self.log_events.get(event_type)
        return True if value is None else bool(value)


# 모듈 전역 단일 설정 인스턴스
_cfg: Config = Config()


def get() -> Config:
    """현재 설정을 반환한다."""
    return _cfg


def _clean_toggles(raw: Any) -> Dict[str, bool]:
    """알 수 없는 이벤트 타입은 버린다(오타로 «다 켜진 줄 알았다» 사고 방지).

    ★ 옛 이름(`decision`·`human_review`)을 그대로 버리면
      «껐다고 믿었는데 action 으로 계속 나가는» 상태가 된다.
      그래서 옛 이름은 버리지 않고 `action` 토글로 접어 넣는다(더 안전한 쪽 = 끄기가 이긴다).
    """
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, bool] = {k: bool(v) for k, v in raw.items() if k in EVENT_TYPES}
    for legacy in LEGACY_EVENT_TYPES:
        if legacy in raw and not bool(raw[legacy]):
            out["action"] = False
    return out


def _safe_timeout(value: Any) -> Tuple[float, float]:
    """M-14(1001-543) = timeout 형식이 잘못되면(한 칸짜리 튜플 등) 기본값으로 떨어뜨린다."""
    try:
        return (float(value[0]), float(value[1]))
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        try:
            _logger.warning(
                "xsoft_comply: timeout 인자 형식이 올바르지 않아 기본값 (2.0, 3.0) 을 사용합니다.")
        except BaseException:
            pass
        return (2.0, 3.0)


def _safe_max_queue(value: Any) -> int:
    """M-14 = max_queue<=0(또는 형식 오류) 는 기본값 10,000 으로 떨어뜨린다."""
    try:
        n = int(value)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        n = 0
    if n <= 0:
        try:
            _logger.warning(
                "xsoft_comply: max_queue 는 0 보다 커야 합니다 — 기본값 10000 을 사용합니다.")
        except BaseException:
            pass
        return 10_000
    return n


def set_config(
    api_key: str,
    endpoint: str,
    *,
    log_llm_content: bool = False,
    spool_dir: str = "~/.xsoft_comply/spool",
    flush_interval: float = 1.0,
    timeout: tuple = (2.0, 3.0),
    max_queue: int = 10_000,
    max_retries: int = 5,
    log_events: Any = None,
    sign: bool = False,
    signing_key: Optional[str] = None,
    policy_enforce: bool = True,
    retention_days: Optional[int] = None,
    dry_run: bool = False,
) -> None:
    """init()이 호출할 내부 함수. 외부에서 직접 호출하지 않는다.

    ★ M-14 = `timeout`·`max_queue` 형식 오류는 **그 인자만** 기본값으로 떨어뜨리고
      경고한다 — init() 전체를 중단시키지 않는다.
    """
    global _cfg
    _cfg = Config(
        api_key=api_key,
        endpoint=endpoint.rstrip("/"),
        log_llm_content=log_llm_content,
        spool_dir=spool_dir,
        flush_interval=float(flush_interval),
        timeout=_safe_timeout(timeout),
        max_queue=_safe_max_queue(max_queue),
        max_retries=int(max_retries),
        log_events=_clean_toggles(log_events),
        sign=bool(sign),
        signing_key=signing_key,
        policy_enforce=bool(policy_enforce),
        retention_days=int(retention_days) if retention_days else RETENTION_DEFAULT_DAYS,
        dry_run=bool(dry_run),
    )
