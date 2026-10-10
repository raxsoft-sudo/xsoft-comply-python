"""KST(한국 시각) 부품 — 「날짜」를 만드는 자리에서만 쓴다.

★ 왜 필요한가
  이 SDK 가 남기는 값 중 **절대시각**(`occurred_at` · 서명 스탬프)은
  UTC 가 맞다. 기계끼리 맞추는 시각이라 지역이 끼면 안 된다.
  그러나 **`_retain_until`(보관 만료일)** 은 절대시각이 아니라 «며칠까지 보관하기로
  계약했는가» 를 사람이 읽는 **날짜**다. 이걸 UTC 날짜로 뽑으면 KST 00:00~09:00 사이에
  기록된 이벤트가 **하루 이른 날짜**를 달고 남는다(예: KST 09-06 02:00 → UTC 09-05).
  근거자료 서비스에서 날짜는 곧 증거이므로, 날짜를 만드는 자리는 KST 달력을 쓴다.

★ 의존성 0
  표준 라이브러리만 쓴다. `zoneinfo` 는 3.9+ 표준이지만 **tz 데이터베이스가 없는 환경**
  (대표적으로 Windows · `tzdata` 미설치 · 일부 슬림 컨테이너)에서는 조회가 실패한다.
  SDK 는 고객 서버 안에서 도는 코드라 그 자리에서 중단되면 안 되므로, 조회 실패 시
  **고정 오프셋 +09:00** 으로 물러선다. 한국은 1988 년 이후 서머타임이 없어
  KST = UTC+9 가 상수이므로 두 경로의 결과가 같다.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo

__all__ = ["KST", "kst_now", "kst_today", "kst_date"]


def _resolve_kst() -> tzinfo:
    try:
        from zoneinfo import ZoneInfo  # 3.9+ 표준

        return ZoneInfo("Asia/Seoul")
    except Exception:  # tz 데이터 없음(Windows 등) → 고정 오프셋으로 물러선다
        return timezone(timedelta(hours=9), "KST")


KST: tzinfo = _resolve_kst()


def kst_now() -> datetime:
    """현재 시각을 **KST tz 를 붙인** aware datetime 으로 돌려준다."""
    return datetime.now(KST)


def kst_date(dt: datetime) -> str:
    """어떤 시각이든 **한국 날짜** `YYYY-MM-DD` 문자열로 바꾼다.

    tz 없는(naive) 값이 들어오면 UTC 로 간주한다 = SDK 안에서 만들어지는 시각은
    전부 UTC 기준이므로 UTC 로 처리한다(로컬 시간대를 임의로 적용하지 않는다).
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(KST).strftime("%Y-%m-%d")


def kst_today() -> str:
    """오늘(한국 날짜) `YYYY-MM-DD`."""
    return kst_date(kst_now())
