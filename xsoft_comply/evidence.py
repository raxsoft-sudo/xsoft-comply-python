"""조항 증적(evidence) 정의 모듈 — 조항별 입력 필드 화이트리스트 · 정리 · 검증.

무엇을 위한 파일인가
  개발자가 아는 것은 조항 번호(31~36)와 그 조항이 받는 입력 필드 이름뿐이면 된다.
  그 값을 이벤트에 실어 보내면 당사 서버가 조항별로 분류해 보관한다.
  이 파일은 «보내기 전» 에 SDK 가 스스로 확인해 주는 쪽이다.

서버와 항목 목록을 함께 두는 이유
  동일한 목록을 서버와 본 파일에 각각 둡니다. 언어가 달라 하나의 정의를 공유할 수 없기 때문입니다.
  서버가 수집 항목의 최종 기준이며, 본 파일은 전송 전에 항목 이름을 확인하는 역할만 수행합니다.
  서버와 SDK 의 정의를 함께 갱신해야 합니다. 목록이 서로 다르면 전송한 항목이 저장되지 않을 수 있습니다.

본 SDK 는 항목의 등급을 두지 않습니다
  본 파일에는 항목의 등급이 없습니다. 어떤 항목을 언제 전송할 것인지는 이용사업자가 정하며,
  전송되지 않은 항목은 비어 있는 상태로 보관됩니다. 이를 미준수로 해석하지 않습니다.

변호사법 제109조 — 법률 판단을 하지 않는 범위
  본 파일은 준수 또는 위반을 판단하지 않습니다. 해당 조항이 수집하는 항목 N개 중 M개가
  전송되었다는 사실만 반환합니다.
"""
from __future__ import annotations

from typing import Any, Iterable

EVIDENCE_SOURCES: tuple[str, ...] = ("runtime", "declared", "document", "assessment")

#: 조항 중립 보관구분 코드
#: ★ 당사 내부 저장 구조 이름을 SDK 가 들고 다니지 않는다 = 공개 API 반환·출력에는
#:   «조항 번호에서 유도한 중립 코드» 만 나간다. 실제 보관 위치는 서버가 정한다.
#:   제33·34조가 같은 코드인 것은 두 조항이 한 묶음으로 다뤄지기 때문이다.
_G31 = "art31"
_G32 = "art32"
_G3334 = "art33_34"
_G35 = "art35"

#: 조문번호 → (조항 보관구분, 수집 항목 이름 목록)
#:
#: ★ 본 목록은 「인공지능 발전과 신뢰 기반 조성 등에 관한 기본법」 제31조부터 제36조까지 및
#:   같은 법 시행령 제23조부터 제29조까지가 요구하는 사실을 수집하기 위한 항목입니다.
#: ★ 본 SDK 는 각 항목에 등급을 두지 않습니다. 항목을 수집할 자리를 제공할 뿐이며,
#:   어떤 항목을 언제 전송할 것인지는 이용사업자가 정합니다.
#: ★ 전송되지 않은 항목은 비어 있는 상태로 보관되며, 미준수로 해석되지 않습니다.
#: ★ 제33조·제35조·제36조는 수집 항목을 두지 않습니다. 해당 조항이 요구하는 사실은
#:   호출 시점마다 새로 발생하지 않으므로 관리 화면 및 문서 등록 경로에서 관리합니다.
ARTICLES: dict[int, tuple[str | None, tuple[str, ...]]] = {
    # ── 제31조 인공지능 투명성 확보 의무 — 15개 ──────────────────────────────
    #    법 제31조제1항부터 제3항까지 및 시행령 제23조제1항부터 제4항까지
    31: (_G31, (
        "prior_notice_provided",        # 법 §31① 사전 고지 이행 여부
        "disclosure_method",            # 영 §23①1~4 고지 방법
        "disclosure_version",           # 영 §23① 적용한 고지 문구의 버전
        "ai_generated_label",           # 법 §31② 생성 결과물 표시 이행 여부
        "label_method",                 # 영 §23②1~2 표시 방법
        "machine_readable_label",       # 영 §23②2 후단 기계판독 표시 시 안내 제공 여부
        "synthetic_media",              # 법 §31③ 실제와 구분이 어려운 결과물 해당 여부
        "label_visibility",             # 법 §31③ 이용자가 명확히 인식할 수 있는 방식 여부
        "accessibility_method",         # 영 §23③1 쉽게 확인할 수 있는 방법
        "audience_consideration",       # 영 §23③2 주된 이용자의 조건 고려
        "artistic_relaxation",          # 법 §31③ 후단 예술적·창의적 표현물 완화 적용 여부
        "exemption_type",               # 영 §23④1~3 적용 예외 유형
        "exemption_basis",              # 영 §23④ 예외로 본 근거
        "output_id",                    # 법 §31②③ 대상 결과물 식별
        "media_type",                   # 법 §31③ 결과물 매체 종류
    )),
    # ── 제32조 인공지능 안전성 확보 의무 — 24개 ─────────────────────────────
    #    법 제32조제1항·제2항 및 시행령 제24조제1항
    32: (_G32, (
        "training_compute",             # 영 §24①1 학습 누적 연산량
        "frontier_technology_status",   # 영 §24①2 최첨단 기술 적용 여부
        "frontier_technology_basis",    # 영 §24①2 그 판단의 근거
        "broad_serious_impact_status",  # 영 §24①3 광범위·중대한 영향 우려 여부
        "broad_serious_impact_basis",   # 영 §24①3 그 판단의 근거
        "model_id",                     # 법 §32① 대상 모델 식별
        "model_version",                # 법 §32① 모델 버전
        "risk_management_policy_ref",   # 법 §32①2 위험관리체계 문서 참조
        "submission_status",            # 법 §32② 이행 결과 제출 여부
        "submission_date",              # 법 §32② 제출일
        "risk_id",                      # 법 §32①1 위험 건 식별
        "risk_signal_source",           # 법 §32①1 위험을 식별한 경로
        "risk_lifecycle_stage",         # 법 §32①1 위험이 발생한 수명주기 단계
        "risk_type",                    # 법 §32①1 위험 구분
        "risk_level",                   # 법 §32①1 위험 평가 결과
        "risk_action",                  # 법 §32①1 위험 완화 조치
        "risk_status",                  # 법 §32①1 위험 처리 상태
        "incident_id",                  # 법 §32①2 안전사고 건 식별
        "incident_detection_source",    # 법 §32①2 사고 감지 경로
        "incident_type",                # 법 §32①2 사고 구분
        "incident_severity",            # 법 §32①2 사고 심각도
        "incident_action",              # 법 §32①2 사고 대응 조치
        "incident_status",              # 법 §32①2 사고 처리 상태
        "incident_close_reason",        # 법 §32①2 사고 종결 사유
    )),
    # ── 제33조 고영향 인공지능의 확인 — 수집 항목 없음 ───────────────────────
    #    제공 전 사전 검토 및 정부 확인 요청은 호출 시점마다 발생하지 않으므로
    #    관리 화면 및 문서 등록 경로에서 관리합니다.
    33: (_G3334, ()),
    # ── 제34조 고영향 인공지능과 관련한 사업자의 책무 — 38개 ─────────────────
    #    법 제34조제1항 각 호·제3항 및 시행령 제27조제1항부터 제5항까지
    34: (_G3334, (
        "risk_mgmt_ref",                # 법 §34①1 위험관리방안 문서 참조
        "control_applied",              # 법 §34①1 통제 실행 여부
        "control_type",                 # 법 §34①1 통제 구분
        "control_outcome",              # 법 §34①1 통제 결과
        "control_version",              # 법 §34①1 적용된 통제 규칙의 버전
        "explanation_policy_ref",       # 법 §34①2 설명 방안 문서 참조
        "decision_basis",               # 법 §34①2 최종결과 도출에 활용된 주요 기준
        "training_data_reference",      # 법 §34①2 학습용데이터 개요 참조
        "explanation_available",        # 법 §34①2 설명 제공 가능 여부
        "explanation_delivery_status",  # 법 §34①2 설명 전달 결과
        "user_protection_policy_ref",   # 법 §34①3 이용자 보호 방안 문서 참조
        "user_protection_action",       # 법 §34①3 이용자 보호 조치
        "protection_reason",            # 법 §34①3 보호 조치 사유
        "protection_outcome",           # 법 §34①3 보호 조치 결과
        "oversight_policy_ref",         # 법 §34①4 사람 관리·감독 체계 문서 참조
        "supervisor_id",                # 법 §34①4 관리·감독자 식별
        "supervisor_name",              # 영 §27①4 관리·감독자 성명
        "supervisor_contact",           # 영 §27①4 관리·감독자 연락처
        "supervisor_role",              # 법 §34①4 관리·감독자 직책
        "intervention_trigger",         # 법 §34①4 개입 조건
        "intervention_authority",       # 법 §34①4 개입 권한 및 범위
        "oversight_required",           # 법 §34①4 사람 검토 필요 여부
        "oversight_request_status",     # 법 §34①4 사람 검토 요청 상태
        "oversight_handover_status",    # 법 §34①4 검토 대상 인계 상태
        "reviewer_id",                  # 법 §34①4 검토자 식별
        "reviewer_role",                # 법 §34①4 검토자 직책
        "review_status",                # 법 §34①4 검토 진행 상태
        "review_result",                # 법 §34①4 검토 결과
        "human_override",               # 법 §34①4 사람에 의한 변경 여부
        "override_reason",              # 법 §34①4 변경 사유
        "evidence_document_id",         # 법 §34①5 조치 근거 문서 참조
        "document_version",             # 법 §34①5 보관 문서의 버전
        "retention_status",             # 영 §27② 문서 5년 보관 상태
        "publication_status",           # 영 §27① 주요 내용 게시 여부
        "publication_url",              # 영 §27① 게시 위치
        "committee_matter",             # 법 §34①6 위원회 심의·의결 사항
        "alt_compliance_basis",         # 법 §34③·영 §27⑤ 다른 법령에 따른 조치로 갈음하는 근거
        "downstream_exemption",         # 영 §27③ 중대한 기능 변경이 없어 이행한 것으로 보는 경우
    )),
    # ── 제35조 고영향 인공지능 영향평가 — 수집 항목 없음 ─────────────────────
    #    영향평가의 내용은 시행령 제28조제1항이 정하는 바에 따라 영향평가서에 포함되며,
    #    문서 등록 경로에서 관리합니다.
    35: (_G35, ()),
    # ── 제36조 국내대리인 지정 — 수집 항목 없음 ──────────────────────────────
    #    국내대리인의 지정 및 신고는 회사 단위의 사실이므로 회사 설정 화면에서 관리합니다.
    36: (None, ()),
}

#: ★ 본 파일은 항목 등급을 두지 않는다. 이 파일이 담는 것은
#:   «그 조항이 어떤 입력 필드를 받는가» 뿐이며, 채움 여부는 사실로만 돌려준다.
#:   제33·35·36조처럼 수집 항목이 없는 조항은 애초에 이 사실 집계 대상이 아니다.
MAX_BLOCKS_PER_EVENT = 6
_MAX_TEXT_LEN = 500

#: 문서참조 하위호환(서버와 같은 규칙)
#:  `evidence_document_id` 는 **정수 식별자**인데, 이전 판 SDK 는 여기에
#:  «문서 무결성 확인값 문자열» 을 실어 보낼 수 있다. 그 값을 제외하지 않고
#:  `evidence_document_hash` 로 옮겨 담는다.
_DOCUMENT_ID_FIELD = "evidence_document_id"
_DOCUMENT_HASH_FIELD = "evidence_document_hash"
_DOCUMENT_REMAP_ARTICLES = (33, 34)


def _is_integer_like(value: Any) -> bool:
    """문서 식별자(정수)로 쓸 수 있는 값인가(정수 또는 정수 문자열)."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        return value.strip().lstrip("-").isdigit()
    return False


def remap_field(obligation_no: int, key: str, value: Any) -> str:
    """구 필드명 → 표준 필드명. 지금은 제33·34조 문서참조 하나뿐이다."""
    if (
        int(obligation_no) in _DOCUMENT_REMAP_ARTICLES
        and key == _DOCUMENT_ID_FIELD
        and not _is_integer_like(value)
    ):
        return _DOCUMENT_HASH_FIELD
    return key


def group_of(obligation_no: int) -> str | None:
    """조문번호 → 조항 중립 보관구분 코드(제36조는 회사 단위 선언이라 None).

    ★ 돌려주는 것은 «조항 번호에서 유도한 코드» 다. 당사 내부 저장 구조 이름이 아니다.
    ★ 계측에 필요하지 않다 = 개발자가 알아야 할 것은 조항 번호와 입력 필드 이름뿐이고,
      실제 보관은 당사 서버의 몫이다.
    """
    spec = ARTICLES.get(int(obligation_no))
    return spec[0] if spec else None


def table_of(obligation_no: int) -> str | None:
    """하위호환 별칭 — `group_of()` 와 같다."""
    return group_of(obligation_no)


def fields_of(obligation_no: int) -> tuple[str, ...]:
    """조문번호 → 그 조항이 받는 입력 필드 이름 목록(등급 없음)."""
    spec = ARTICLES.get(int(obligation_no))
    return spec[1] if spec else ()


def block(
    obligation_no: int,
    *,
    source: str = "runtime",
    evidence_key: str | None = None,
    **fields: Any,
) -> dict[str, Any]:
    """증적 블록 1개를 만든다. 값이 None 인 필드는 넣지 않는다(빈 블록 방지)."""
    out: dict[str, Any] = {
        "obligation_no": int(obligation_no),
        "source": source if source in EVIDENCE_SOURCES else "runtime",
        # ★ 구 이름(evidence_document_id 에 담은 hash)은 여기서 표준 이름으로 옮겨 담는다.
        "fields": {
            remap_field(obligation_no, k, v): v for k, v in fields.items() if v is not None
        },
    }
    if evidence_key:
        out["evidence_key"] = str(evidence_key)
    return out


def normalize(evidence: Any) -> tuple[list[dict[str, Any]], list[str]]:
    """payload 에 실을 형태로 정리한다. (blocks, warnings) 반환.

    ★ 서버와 같은 규칙으로 «먼저» 거른다(다중 검증). 서버가 최종 권위지만,
      개발자는 **보내기 전에** 오타를 알아야 한다.
    ★ 예외를 발생시키지 않는다 = 감사 SDK 가 고객 에이전트를 중단시키지 않는다.
    """
    warnings: list[str] = []
    if evidence is None:
        return [], warnings
    items: Iterable[Any] = evidence if isinstance(evidence, (list, tuple)) else [evidence]
    items = list(items)
    if len(items) > MAX_BLOCKS_PER_EVENT:
        warnings.append(f"evidence 블록이 {MAX_BLOCKS_PER_EVENT}개를 넘어 뒤쪽을 제외했습니다.")
        items = items[:MAX_BLOCKS_PER_EVENT]

    blocks: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            warnings.append("evidence 원소가 dict 가 아니기 때문에 제외했습니다.")
            continue
        try:
            article = int(item.get("obligation_no"))
        except (TypeError, ValueError):
            warnings.append(f"obligation_no {item.get('obligation_no')!r} 가 숫자가 아니기 때문에 제외했습니다.")
            continue
        if article not in ARTICLES:
            warnings.append(f"obligation_no {article} 는 31~36 이 아니기 때문에 제외했습니다.")
            continue

        source = item.get("source") or "runtime"
        if source not in EVIDENCE_SOURCES:
            warnings.append(f"source {source!r} 를 알 수 없어 runtime 으로 처리했습니다.")
            source = "runtime"

        allowed = fields_of(article)
        raw_fields = item.get("fields")
        raw_fields = raw_fields if isinstance(raw_fields, dict) else {}
        fields: dict[str, Any] = {}
        for raw_key, value in raw_fields.items():
            key = remap_field(article, raw_key, value)
            if key not in allowed:
                warnings.append(f"제{article}조의 허용 항목이 아니기 때문에 {raw_key!r} 를 제외했습니다.")
                continue
            if value is None:
                continue
            fields[key] = value[:_MAX_TEXT_LEN] if isinstance(value, str) else value

        out: dict[str, Any] = {"obligation_no": article, "source": source, "fields": fields}
        key = item.get("evidence_key")
        if isinstance(key, str) and key.strip():
            out["evidence_key"] = key.strip()[:200]
        blocks.append(out)
    return blocks, warnings


def validate_block(blk: dict[str, Any]) -> dict[str, Any]:
    """블록 1개의 «입력 필드 충족» 을 센다 (전송 전 확인).

    ★ 109조 = 판정하지 않는다. 돌려주는 것은 사실뿐이다 —
      입력 필드 N개 중 M개가 비어 있고, 화이트리스트 밖 키가 몇 개인가.
    ★ 항목 등급 구분 없이, 그 조항이 받는
      입력 필드 전체를 기준으로 채움 여부만 센다.
    """
    article = int(blk.get("obligation_no") or 0)
    allowed = fields_of(article)
    fields = blk.get("fields") if isinstance(blk.get("fields"), dict) else {}
    # ★ GX-11 = truthiness(`not fields.get(k)`)로 세면 명시적 False·0 도 «미입력»으로 잘못 센다
    #   («미실시»(False)와 «정보 없음»(누락)은 다른 사실이다). 키 자체가 없을 때만 빈 값으로 본다.
    missing = [k for k in allowed if k not in fields]
    unknown = [k for k in fields if k not in allowed]
    return {
        "obligation_no": article,
        "group": group_of(article),
        "field_total": len(allowed),
        "field_filled": len(allowed) - len(missing),
        "field_empty": missing,
        "unknown_fields": unknown,
    }


def validate(event: Any) -> dict[str, Any]:
    """이벤트(또는 증적 블록/배열)의 조항 충족 상황을 돌려준다 (전송 전 확인).

        report = xc.validate(evt)
        report["blocks"][0]["field_empty"]   # 비어 있는 입력 필드

    ★ 예외를 발생시키지 않는다. 형식이 이상하면 `warnings` 에 이유가 담긴다.
    """
    try:
        if isinstance(event, dict) and "evidence" in event:
            raw = event.get("evidence")
            event_type = event.get("type")
        else:
            raw = event
            event_type = None
        blocks, warnings = normalize(raw)
        reports = [validate_block(b) for b in blocks]
        if event_type is not None and event_type in ("decision", "human_review"):
            warnings.append(
                f"event_type {event_type!r} 는 이전 판 형식입니다 — 서버가 action + 제34조 증적으로 변환합니다."
            )
        return {
            "event_type": event_type,
            "blocks": reports,
            "warnings": warnings,
            "field_empty_total": sum(len(r["field_empty"]) for r in reports),
        }
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:  # 영향 최소화 원칙
        return {"event_type": None, "blocks": [], "warnings": [f"검증 실패: {exc}"],
                "field_empty_total": 0}
