"""전송 전 확인 — dry_run 출력 · preview.

무엇을 확인시켜 주는가
  ① 실제로 나가는 **payload** (원문이 없다는 것을 눈으로 확인)
  ② 조항 **연결** (어느 조항 증적으로 갔나)
  ③ **수집 항목** 전송 상황 (사실만 기재하며 판단하지 않습니다)
  ④ **마스킹** 결과 (PII·금지 키가 이미 제거된 상태)

사용
    xc.init(..., dry_run=True)      # 전송 없이 위 4가지를 콘솔에 출력
    print(xc.preview())             # 샘플 이벤트 1건을 조립해 문자열로 확인
    python -m xsoft_comply.preview  # 설치 확인용 이벤트 1건 출력

★ 변호사법 제109조 = 준수 또는 위반을 판단하지 않습니다. 수집 항목 N개 중 M개가 전송되었다는 사실만 기재합니다.
★ 이 모듈의 어떤 함수도 예외를 호출자에게 전달하지 않는다.
"""
from __future__ import annotations

import json
from typing import Any

from . import evidence as _evidence

_SEP = "─" * 68


def _fmt_block(blk: dict[str, Any]) -> list[str]:
    """화면에 **당사 내부 보관 구조를 적지 않는다.**
    개발자에게 필요한 사실은 「어느 조항으로 갔고, 허용 항목 몇 개가 채워졌나」뿐이다."""
    report = _evidence.validate_block(blk)
    article = report["obligation_no"]
    lines = [
        f"  · 제{article}조   출처={blk.get('source')}"
        + (f"   중복방지키={blk['evidence_key']}" if blk.get("evidence_key") else ""),
        f"    전송 항목 {len(blk.get('fields') or {})}개 · 수집 항목 {report['field_filled']}/{report['field_total']} 전송",
    ]
    if report["field_empty"]:
        lines.append(f"    전송되지 않은 수집 항목: {', '.join(report['field_empty'])}")
    if report["unknown_fields"]:
        lines.append(f"    ★ 허용된 항목명 아님(제외됨): {', '.join(report['unknown_fields'])}")
    return lines


def render(evt: dict[str, Any]) -> str:
    """조립된 이벤트 1건을 사람이 읽을 형태로 만든다(전송하지 않는다)."""
    try:
        wire = {k: v for k, v in evt.items() if not k.startswith("_")}
        meta = wire.get("metadata") or {}
        blocks = wire.get("evidence") or []

        lines = [_SEP, f"[xsoft_comply dry-run] {wire.get('type')}  seq={wire.get('seq')}  "
                       f"event_id={wire.get('event_id')}"]
        lines.append(f"  trace_id={evt.get('_trace_id')}  occurred_at={wire.get('occurred_at')}")

        # ④ 마스킹·원문 미전송 확인
        masked_note = "PII 마스킹 적용됨" if meta.get("_pii_masked") else "PII 패턴 미검출"
        dropped = meta.get("_dropped_keys")
        lines.append(
            f"  원문 미전송 = metadata 키 {len(meta)}개 · 금지 키 제거 {dropped or 0}개 · {masked_note}"
        )

        # ② 조항 매핑 + ③ 수집 항목 전송 상황
        if blocks:
            lines.append(f"  조항 증적 {len(blocks)}건 (서버가 조항별로 분류해 보관한다)")
            for blk in blocks:
                lines.extend(_fmt_block(blk))
        else:
            lines.append("  조항 증적 없음 = 이벤트 기록만 남고 조항 증적은 만들어지지 않는다.")

        # ① 실제 payload
        lines.append("  payload =")
        body = json.dumps(wire, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        lines.extend("    " + line for line in body.splitlines())
        lines.append(_SEP)
        return "\n".join(lines)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:
        return f"[xsoft_comply dry-run] 출력 실패: {exc}"


def emit(evt: dict[str, Any]) -> None:
    """dry_run 모드에서 session._emit 이 부른다. 전송 대신 표준출력."""
    try:
        print(render(evt))
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass


def sample_event() -> dict[str, Any]:
    """샘플 이벤트 1건을 조립한다(전송 없음). preview()·`-m` 실행이 사용한다."""
    from . import events as _events

    return _events.build(
        "action",
        seq=1,
        trace_id="tr_preview",
        action="human_review",
        metadata={"reviewer_role": "manager", "status": "ok"},
        evidence=[_evidence.block(
            34,
            source="runtime",
            review_result="approved",
            reviewer_role="manager",
            reviewer_id="rv-001",
            decision_basis="신용 심사 기준표 v3 적용",
        )],
        agent_id="preview-agent",
    )


def preview(event: Any = None) -> str:
    """이벤트 1건의 전송 내용을 로컬에서 확인한다.

    인자를 주면 그 이벤트를, 없으면 샘플 이벤트 1건을 출력한다.
    """
    try:
        return render(event if isinstance(event, dict) else sample_event())
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:
        return f"[xsoft_comply preview] 실패: {exc}"


def main() -> int:
    print(preview())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
