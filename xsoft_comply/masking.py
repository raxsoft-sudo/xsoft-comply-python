"""개인정보 형식 기반 가림 — 전송 전 로컬 처리.

정규식 기반 가림은 정해진 형식의 값만 처리하며, 모든 개인정보의 탐지를 보장하지 않는다.

# ★ 다중 검증 = 서버도 같은 규칙으로 한 번 더 거른다.
# Python SDK(로컬)와 서버(TypeScript)가 동일한 패턴으로 PII를 각각 처리한다.
# 코드 공유는 불가(Python ↔ TypeScript)하므로 패턴을 동일하게 유지한다.

PII 마스킹 규칙:
  - 한국 주민등록번호 \\d{6}-?\\d{7}  → ******-*******
  - 휴대전화 01\\d-?\\d{3,4}-?\\d{4}  → 010-****-****
  - 이메일                              → a***@domain
  - 카드번호 16자리                     → 앞4·뒤4만 (****-****-****-1234 형식)
  - 마스킹 적용 시 metadata._pii_masked = true

설계 노트:
  - 모든 마스킹은 dict/list/str을 재귀적으로 처리한다.
  - 마스킹 적용 여부를 bool로 반환해 호출자가 _pii_masked 플래그를 설정한다.
"""
from __future__ import annotations

import re
from typing import Any, Tuple

# ── 정규식 패턴 ─────────────────────────────────────────────────────────────
#
# ★ S-16(2026-09-29 최소 수리) = 예전에는 경계를 `\b`(단어 경계)로 잡았다. `\b` 는
#   숫자↔한글 사이에서 «성립하지 않는다»(둘 다 유니코드 word 문자라 전환이 없다) —
#   그래서 "010-1234-5678번호" 처럼 **숫자 바로 뒤에 한글이 붙으면 매치 자체가 실패**해
#   개인정보가 마스킹되지 않고 그대로 나갔다(한글 인접 번호 누락).
#   반대로 `\b` 는 더 긴 숫자열 안에서도 부분적으로 걸려 **13자리 등 상관없는 숫자를
#   오탐**하는 경우가 있었다. 그래서 word-boundary 대신 **숫자가 아닌 것과만 경계를
#   맞추는 lookaround** `(?<!\d)...(?!\d)` 로 바꾼다 — 앞뒤에 «다른 숫자»만 없으면
#   된다(한글이든 공백이든 문장 처음/끝이든 전부 허용). 두 문제를 한 번에 줄인다.
#   ★ 이 정규식은 정해진 형식만 처리하며, 모든 개인정보 탐지를 보장하지 않는다(그대로).

# 주민등록번호: 6자리-7자리 (하이픈 선택)
_RRN = re.compile(r"(?<!\d)\d{6}-?\d{7}(?!\d)")

# 휴대전화: 01X-XXXX-XXXX (하이픈·점 선택, 가운데 3~4자리) · +82 국가번호 표기 지원
#   국내표기 = 01X.. / 국제표기 = +82 뒤에는 앞자리 0 이 빠진 1X.. 가 온다(+82-10-...).
_PHONE = re.compile(r"(?<!\d)(?:01\d|\+82[-.\s]?1\d)[-.]?\d{3,4}[-.]?\d{4}(?!\d)")

# 이메일
_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

# 카드번호: 4-4-4-4 (구분자 선택)
_CARD = re.compile(r"(?<!\d)(\d{4})[\s\-]?(\d{4})[\s\-]?(\d{4})[\s\-]?(\d{4})(?!\d)")

# ── M-19(1001-543) = 형식만 보던 RRN·카드 규칙이 평범한 13·16자리 숫자(타임스탬프·
#   주문번호 등)를 오탐했다. 날짜·체크섬 같은 **구조적 유효성**까지 봐야 진짜
#   주민번호·카드번호와 구분된다 — 다중 검증(서버 lib/mask/pii.ts 도 같은 규칙 적용).
_DAYS_IN_MONTH = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


def _valid_rrn(digits13: str) -> bool:
    """주민번호 13자리(구분자 제외) = 앞 6자리가 유효한 날짜 + 뒤 첫 자리 1~4."""
    if len(digits13) != 13 or not digits13.isdigit():
        return False
    mm, dd = int(digits13[2:4]), int(digits13[4:6])
    if not (1 <= mm <= 12):
        return False
    if not (1 <= dd <= _DAYS_IN_MONTH[mm - 1]):
        return False
    # ★ S-16/R-5 유지 = 1~4(내국인) 뿐 아니라 5~8(외국인등록·2000년대생)도 유효한 성별자리다.
    return digits13[6] in "12345678"


def _luhn_ok(digits: str) -> bool:
    """카드번호 체크섬(Luhn 알고리즘)."""
    if not digits.isdigit():
        return False
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


# ── 문자열 마스킹 ──────────────────────────────────────────────────────────────

def _mask_str(s: str) -> Tuple[str, bool]:
    """단일 문자열에서 PII 패턴을 마스킹한다."""
    changed = False

    # 주민등록번호 — ★ M-19 = 날짜·성별 자리 유효성 검증을 통과했을 때만 마스킹한다
    #   (13자리 타임스탬프 등을 주민번호로 오탐하지 않는다).
    def _mask_rrn(m: re.Match) -> str:  # type: ignore[type-arg]
        nonlocal changed
        full = m.group(0)
        if not _valid_rrn(full.replace("-", "")):
            return full
        changed = True
        return "******-*******"

    s = _RRN.sub(_mask_rrn, s)

    # 휴대전화 — 마스킹 후 통일 형식 010-****-****
    result, n = _PHONE.subn("010-****-****", s)
    if n:
        s, changed = result, True

    # 이메일
    def _mask_email(m: re.Match) -> str:  # type: ignore[type-arg]
        full = m.group(0)
        local, domain = full.split("@", 1)
        masked = (local[0] + "***") if len(local) > 1 else "***"
        return f"{masked}@{domain}"

    result, n = _EMAIL.subn(_mask_email, s)
    if n:
        s, changed = result, True

    # 카드번호 — ★ M-19 = Luhn 체크섬을 통과했을 때만 마스킹한다(16자리 주문번호 등
    #   임의의 숫자열을 카드로 오탐하지 않는다). 앞4·뒤4만 남긴다.
    def _mask_card(m: re.Match) -> str:  # type: ignore[type-arg]
        nonlocal changed
        digits = m.group(1) + m.group(2) + m.group(3) + m.group(4)
        if not _luhn_ok(digits):
            return m.group(0)
        changed = True
        return f"{m.group(1)}-****-****-{m.group(4)}"

    s = _CARD.sub(_mask_card, s)

    return s, changed


# ── 재귀 마스킹 ────────────────────────────────────────────────────────────────

def _mask_value(v: Any) -> Tuple[Any, bool]:
    """임의 값(str/dict/list/기타)을 재귀적으로 마스킹한다."""
    if isinstance(v, str):
        return _mask_str(v)
    if isinstance(v, dict):
        out: dict[str, Any] = {}
        changed = False
        for k, val in v.items():
            new_val, c = _mask_value(val)
            out[k] = new_val
            changed = changed or c
        return out, changed
    if isinstance(v, list):
        out_list: list[Any] = []
        changed = False
        for item in v:
            new_item, c = _mask_value(item)
            out_list.append(new_item)
            changed = changed or c
        return out_list, changed
    # int/float/bool/None 등은 그대로
    return v, False


# ── 공개 API ──────────────────────────────────────────────────────────────────

def apply(metadata: dict[str, Any]) -> dict[str, Any]:
    """metadata dict에 PII 마스킹을 적용하고 새 dict를 반환한다.

    마스킹이 하나라도 발생하면 _pii_masked=True를 설정한다.
    """
    masked, changed = _mask_value(metadata)
    if not isinstance(masked, dict):
        return metadata
    if changed:
        masked["_pii_masked"] = True
    return masked
