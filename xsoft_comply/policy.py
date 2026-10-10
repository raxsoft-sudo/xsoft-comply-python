"""정책 기반 실행 차단 — 규칙을 평가하고 차단 대상 도구를 실행하지 않는다.

★ 규칙을 SDK 가 평가하고, 결과가 block 이면 **도구를 호출하지 않는다.**

★ 영향 최소화 원칙
  - 규칙 평가에서 어떤 예외가 발생해도 «allow» 로 처리한다. 정책 평가 오류가 고객 에이전트를 중단시키지 않는다.
  - 차단은 예외를 발생시키지 않는다. `run_tool()` 이 `blocked=True` 인 결과 객체를 돌려줄 뿐이다
    (에이전트가 «막혔으니 다른 길로 간다» 를 스스로 결정하게 한다).

규칙 형식
    {
      "id": "no-mass-delete",
      "when": {"type": "tool_call", "tool": "crm.delete_all"},   # 완전일치
      "when_re": {"tool": r"^crm\\.delete.*"},                    # 정규식(선택)
      "effect": "block" | "warn" | "allow",
      "reason": "대량 삭제 도구는 사람 승인 후에만",
    }
  - 위에서부터 첫 일치 규칙이 적용된다(먼저 쓴 것이 우선).
  - 일치가 없으면 기본 effect = "allow".
"""
from __future__ import annotations

import re
import threading
import time
from typing import Any

EFFECTS = ("allow", "warn", "block")

_lock = threading.Lock()
_rules: list[dict[str, Any]] = []
_compiled: list[tuple[dict[str, Any], dict[str, Any]]] = []

# ── S-21 = 사용자 정규식(when_re) ReDoS 방지 ──────────────────────────────────
#   고객이 준 정규식·값은 병적 입력(예: (a+)+$ × "aaa…a!")이면 지수 시간이 걸릴 수
#   있다. ★ 실측(2026-09-29) = CPython `re` 매칭은 GIL 을 놓지 않는 단일 C 루프라
#   **진행 중인 매치를 다른 스레드·시그널·타임아웃 어느 것으로도 끊을 수 없다**
#   (스레드로 격리해 봐도 그 스레드가 GIL 을 붙든 채 안 놓아 프로세스 전체가
#   같이 멈춘다 — 직접 재현: 데몬 스레드 + join(timeout) 으로도 `timeout 5`가
#   강제종료할 때까지 프로세스가 응답하지 않았다). 그래서 "시간 상한"은
#   *실행 후 중단*이 아니라 **등록 전 거부**로만 지킬 수 있다.
#   ★ 그래서 `set_rules()` 시점에 정규식의 **구조**를 살펴 중첩된 무제한 반복
#     ((a+)+ · (a*)* · (a+)* 류 — 지수 백트래킹의 고전적 원인)이 있으면
#     **그 규칙을 통째로 등록하지 않는다**(부분 적용 금지 — when_re 만 빼고
#     when 만 남기면 의도보다 넓게 «전부 일치»가 될 수 있어 더 위험하다).
#   ★ 완전한 탐지는 아니다(정지 문제라 이론적으로 불가능) — 가장 흔한 구조만
#     막는다. README 에 when_re 는 짧고 단순하게 쓰라고 명시한다(잔여 위험 고지).
_UNBOUNDED_REPEAT_MIN = 5  # 이 이상 반복 가능하면(상한 없음 포함) "사실상 무제한"으로 본다


def _has_nested_unbounded_repeat(items: Any, inside_unbounded: bool = False) -> bool:
    """중첩된 무제한 반복 구조를 재귀로 찾는다. `items` = `re._parser.parse()` 결과(튜플 목록)."""
    try:
        for op, av in items:
            name = str(op)
            if name in ("MAX_REPEAT", "MIN_REPEAT"):
                lo, hi, body = av
                unbounded = (hi is None) or (hi - lo >= _UNBOUNDED_REPEAT_MIN) or (hi > 1_000_000)
                if unbounded and inside_unbounded:
                    return True
                if _has_nested_unbounded_repeat(body, inside_unbounded or unbounded):
                    return True
            elif name == "SUBPATTERN":
                body = av[-1]
                if _has_nested_unbounded_repeat(body, inside_unbounded):
                    return True
            elif name in ("BRANCH",):
                _, branches = av
                for b in branches:
                    if _has_nested_unbounded_repeat(b, inside_unbounded):
                        return True
            elif name in ("ASSERT", "ASSERT_NOT"):
                body = av[-1]
                if _has_nested_unbounded_repeat(body, inside_unbounded):
                    return True
        return False
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return True  # 구조를 못 미더우면 안전 쪽(=병적으로 간주)으로 판정한다


def _is_unsafe_pattern(pattern_str: str) -> bool:
    """S-21 = 중첩 무제한 반복이 있으면 True(등록 거부 대상)."""
    try:
        try:
            import re._parser as _parser  # Python ≥ 3.11
        except ImportError:  # pragma: no cover
            import sre_parse as _parser  # type: ignore[no-redef]  # Python < 3.11
        parsed = list(_parser.parse(pattern_str))
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return True  # 파싱 실패도 안전 쪽으로 거부한다
    return _has_nested_unbounded_repeat(parsed)


_rejected_pattern_total = 0
_rejected_pattern_lock = threading.Lock()


def _count_rejected_pattern() -> None:
    global _rejected_pattern_total
    with _rejected_pattern_lock:
        _rejected_pattern_total += 1


def rejected_patterns() -> int:
    """S-21 = 병적(중첩 무제한 반복) 구조라 **등록 자체를 거부**한 규칙 누적 건수."""
    with _rejected_pattern_lock:
        return _rejected_pattern_total


def reset_rejected_patterns() -> None:
    global _rejected_pattern_total
    with _rejected_pattern_lock:
        _rejected_pattern_total = 0


# ── F-04 = 정책 평가 예산 ─────────────────────────────────────────────────────
#   평가 1건이 예산을 넘기면 그 시점까지 나온 판정을 믿지 않고 기본값 «허용» 으로
#   처리한다 — 느린 평가(거대 입력·규칙 다수) 때문에 에이전트 실행이 막히면 안 된다.
_POLICY_BUDGET_SEC = 0.010
_budget_exceeded_total = 0
_budget_lock = threading.Lock()


def _count_budget_exceeded() -> None:
    global _budget_exceeded_total
    with _budget_lock:
        _budget_exceeded_total += 1


def budget_exceeded_count() -> int:
    """F-04 = 정책 평가 예산(10ms)을 넘겨 «기본 허용» 으로 처리한 누적 건수."""
    with _budget_lock:
        return _budget_exceeded_total


# ── M-15(1001-543) = 입력 칸 길이 상한 ────────────────────────────────────────
#   when_re 자체는 S-21 로 "구조적 병적 패턴"을 걸러내지만, 평범한 `[a-z]+@x\.com`
#   같은 패턴도 입력이 수만 자면 백트래킹이 O(n²) 로 느려질 수 있다(중첩 반복이
#   아니라 n 이 커서 느린 경우는 S-21 로 못 막는다). 평가 전에 **입력 자체를
#   1,024자로 잘라** 최악의 경우도 짧은 시간 안에 끝나게 한다.
_FIELD_MAX_LEN = 1024
_truncated_total = 0
_truncated_lock = threading.Lock()


def _count_truncated() -> None:
    global _truncated_total
    with _truncated_lock:
        _truncated_total += 1


def truncated_count() -> int:
    """M-15 = 정책 평가 전 1,024자로 잘린 입력 칸의 누적 건수."""
    with _truncated_lock:
        return _truncated_total


def reset_truncated() -> None:
    global _truncated_total
    with _truncated_lock:
        _truncated_total = 0


class Decision:
    """평가 결과. `blocked` 만 봐도 되게 만든다."""

    __slots__ = ("effect", "rule_id", "reason")

    def __init__(self, effect: str = "allow", rule_id: str = "", reason: str = "") -> None:
        self.effect = effect
        self.rule_id = rule_id
        self.reason = reason

    @property
    def blocked(self) -> bool:
        return self.effect == "block"

    @property
    def label(self) -> str:
        """이벤트에 실을 정책 결과 라벨. 109조 = 확정 법률판단 문구 금지."""
        return {"block": "blocked", "warn": "warn", "allow": "allowed"}.get(self.effect, "allowed")

    def __repr__(self) -> str:
        return f"Decision(effect={self.effect!r}, rule_id={self.rule_id!r})"


ALLOW = Decision()


def set_rules(rules: list[dict[str, Any]] | None) -> int:
    """규칙을 교체한다. 형식이 잘못된 규칙·병적 정규식 규칙은 제외하고 나머지를 적용한다.

    ★ S-21 = `when_re` 중 하나라도 병적(중첩 무제한 반복) 구조면 그 **규칙 전체**를
      제외한다(그 정규식만 빼고 나머지 조건으로 등록하면 의도보다 넓게 걸리는
      규칙이 될 수 있어 더 위험하다 — 부분 적용 금지).
    """
    global _rules, _compiled
    # ★ M-14(1001-543) = rules 가 list/tuple 이 아니면(고객 오타 — 예: 단일 dict·정수)
    #   "그 인자만" 규칙 0개로 떨어뜨린다. 예전에는 `for rule in rules or []` 가
    #   비-iterable 에 그대로 돌아 TypeError 를 던져 init() 전체가 중단됐다.
    if rules is not None and not isinstance(rules, (list, tuple)):
        try:
            import logging
            logging.getLogger("xsoft_comply").warning(
                "xsoft_comply: policy 인자가 list/tuple 이 아니어서 무시합니다"
                "(규칙 0개로 진행합니다).")
        except BaseException:
            pass
        rules = []
    ok_rules: list[dict[str, Any]] = []
    compiled: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for rule in rules or []:
        try:
            if not isinstance(rule, dict):
                continue
            effect = str(rule.get("effect", "allow")).lower()
            if effect not in EFFECTS:
                continue
            when_re = rule.get("when_re") or {}
            unsafe = False
            patterns: dict[str, Any] = {}
            for key, pattern_str in when_re.items():
                pattern_str = str(pattern_str)
                if _is_unsafe_pattern(pattern_str):
                    unsafe = True
                    break
                patterns[key] = re.compile(pattern_str)
            if unsafe:
                _count_rejected_pattern()
                continue
            ok_rules.append(rule)
            compiled.append((rule, patterns))
        except Exception:
            continue
    with _lock:
        _rules = ok_rules
        _compiled = compiled
    return len(ok_rules)


def rules_count() -> int:
    return len(_rules)


def reset() -> None:
    set_rules([])


def _matches(rule: dict[str, Any], patterns: dict[str, Any], fields: dict[str, Any]) -> bool:
    for key, expected in (rule.get("when") or {}).items():
        actual = fields.get(key)
        if actual is None or str(actual) != str(expected):
            return False
    for key, pattern in patterns.items():
        actual = fields.get(key)
        if actual is None or not pattern.search(str(actual)):
            return False
    # when·when_re 가 둘 다 비면 «전부 일치» 규칙이 되므로 오적용 방지를 위해 무시한다.
    if not (rule.get("when") or rule.get("when_re")):
        return False
    return True


def evaluate(kind: str, fields: dict[str, Any] | None = None) -> Decision:
    """kind = 이벤트 타입(tool_call·data_access·action …). 예외는 전부 allow 로 처리한다.

    ★ F-04 = 평가 1건이 예산(10ms)을 넘기면(거대 입력·규칙 다수로 느려졌을 때 포함)
      그 시점까지 나온 판정을 버리고 기본값 «허용» 으로 돌아간다. 병적 정규식
      자체는 S-21 에 따라 `set_rules()` 단계에서 이미 걸러져 여기까지 오지 않는다.
    """
    try:
        if not _compiled:
            return ALLOW
        started = time.monotonic()
        merged: dict[str, Any] = {"type": kind}
        for key, value in (fields or {}).items():
            if value is None:
                continue
            # ★ M-15 = 정규식 평가 전에 긴 입력을 자른다(최악의 백트래킹 시간을 상한).
            if isinstance(value, str) and len(value) > _FIELD_MAX_LEN:
                value = value[:_FIELD_MAX_LEN]
                _count_truncated()
            merged[key] = value
        for rule, patterns in list(_compiled):
            matched = _matches(rule, patterns, merged)
            if (time.monotonic() - started) > _POLICY_BUDGET_SEC:
                _count_budget_exceeded()
                return ALLOW
            if matched:
                return Decision(
                    effect=str(rule.get("effect", "allow")).lower(),
                    rule_id=str(rule.get("id", "")),
                    reason=str(rule.get("reason", "")),
                )
        return ALLOW
    except Exception:
        return ALLOW


class ToolOutcome:
    """`Session.run_tool()` 의 결과. 차단이면 `value` 는 없다."""

    __slots__ = ("decision", "value", "called")

    def __init__(self, decision: Decision, value: Any = None, called: bool = False) -> None:
        self.decision = decision
        self.value = value
        self.called = called

    @property
    def blocked(self) -> bool:
        return self.decision.blocked

    def __repr__(self) -> str:
        return f"ToolOutcome(blocked={self.blocked}, called={self.called})"
