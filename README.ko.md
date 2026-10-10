# xsoft_comply Python SDK

[xsoft comply](https://xsoftcomply.com)의 공식 Python SDK입니다. AI 실행·도구 호출·판단·사람 검토 등의 운영기록을 수집합니다. SDK는 법적 의무의 적용 여부나 충족 여부를 판단하지 않습니다.

Python 3.9 이상을 지원하며, 기본 설치에는 Python 표준 라이브러리 이외의 실행 의존성이 없습니다. 배포 패키지 이름은 `xsoft-comply`, import 이름은 `xsoft_comply`입니다.

## 설치

가상환경 사용을 권장합니다. Linux와 macOS에서는 다음과 같이 설치합니다.

```bash
python3 -m venv .venv
.venv/bin/python -m pip install xsoft-comply==1.0.2
.venv/bin/python -c "import xsoft_comply; print(xsoft_comply.__version__)"
```

Windows PowerShell에서는 `.venv\Scripts\python.exe -m pip install xsoft-comply==1.0.2`로 설치할 수 있습니다. 설치 후 출력되는 버전이 `1.0.2`인지 확인하십시오. 배포 파일의 sha256 확인값은 빌드 후 `RELEASES.md`에서 확인하십시오. 해시 고정 설치를 사용한다면 기존 `xsoft-comply==1.0.1` 항목을 `1.0.2`와 해당 배포 파일의 실제 확인값으로 교체하십시오.

## 빠른 시작

API 키를 소스에 직접 적지 말고 환경변수 등으로 주입하십시오. `endpoint`에는 서비스에서 안내받은 Event API 기본 주소를 넣으십시오. 이 예시의 주소는 실제 주소로 교체해야 합니다.

```python
import os
import xsoft_comply as xc

xc.init(
    api_key=os.environ["XSOFT_COMPLY_API_KEY"],
    endpoint="https://example.com/api/v1",
)

with xc.session(agent_id="example-agent-01") as s:
    s.llm_call(model="example-model", provider="example-provider",
               tokens_in=12, tokens_out=8, latency_ms=240)
    s.tool_call(tool="example.lookup", status="ok", latency_ms=14)
    s.action(action="answer.sent", status="ok")

xc.flush(timeout=5.0)
```

세션 시작·종료는 `xc.session()`이 기록합니다. LLM 호출·도구 호출·데이터 조회·업무 처리·판단·사람 검토는 해당 지점에서 메서드를 호출하거나 지원하는 프레임워크 연동을 사용해야 기록됩니다. 전송 대상 AI 시스템은 세션의 `product_id`가 아니라 API 키에 연결된 AI 시스템으로 정해집니다. AI 시스템별로 별도의 API 키를 사용하십시오.

| 전송되는 이벤트 | 기록 방법 |
|---|---|
| `session_start`, `session_end` | `xc.session()`의 시작·종료 |
| `llm_call` | `s.llm_call()` |
| `tool_call` | `s.tool_call()` |
| `data_access` | `s.data_access()` |
| `action` | `s.action()`, `s.decision()`, `s.human_review()` |

`decision()`과 `human_review()`는 별도 이벤트 유형을 늘리지 않고 `action` 이벤트와 관련 근거자료을 기록합니다. LangChain과 OpenAI Agents SDK 연동 함수도 제공하지만 해당 프레임워크는 별도로 설치해야 합니다.

## 조항별 운영 근거자료

`xc.evidence_block(조항, **항목)` 또는 `s.evidence(조항, ...)`로 제31·32·34조의 사전 정의된 항목을 기록할 수 있습니다. 제33·35·36조에는 SDK 입력 항목이 없습니다. 항목이 비어 있거나 기록이 없다는 사실만으로 법적 상태를 판단하지 않습니다.

```python
with xc.session(agent_id="example-agent-01") as s:
    s.action("review.requested", status="ok", evidence=[
        xc.evidence_block(34, supervisor_id="supervisor-01",
                          decision_basis="내부 기준표 v3"),
    ])
    s.evidence(32, source="declared", evidence_key="model-01",
               model_id="model-01", model_version="1.2")
```

`evidence_key`는 같은 선언을 중복 관리하지 않기 위한 키입니다. 입력 항목명은 아래 허용 목록과 정확히 일치해야 합니다. 정의되지 않은 항목명은 저장되지 않습니다. 입력 값을 원문·개인정보·법적 판단으로 채우지 마십시오.

| 타입 | 입력 형식 | 형식 오류 시 |
|---|---|---|
| `text` | 문자열 | 문자열로 변환, 길면 잘릴 수 있음 |
| `numeric` | 숫자 또는 숫자 문자열 | 해당 항목 제외 |
| `bigint` | 정수 또는 정수 문자열 | 해당 항목 제외 |
| `boolean` | `True`·`False` 또는 `"true"`·`"false"` | 해당 항목 제외 |
| `date` | 해석 가능한 날짜 문자열, 권장 `YYYY-MM-DD` | 해당 항목 제외 |

형식이 틀린 항목은 전송 묶음 전체를 중단시키지 않고 해당 항목만 제외됩니다. 접수 응답만으로 모든 항목의 저장을 단정하지 말고 전송 전 로컬에서 확인하십시오.

## 전송 전 로컬 확인

```python
xc.init(api_key=os.environ["XSOFT_COMPLY_API_KEY"],
        endpoint="https://example.com/api/v1", dry_run=True)
block = xc.evidence_block(34, supervisor_id="supervisor-01")
print(xc.validate(block))
print(xc.preview())
```

`dry_run=True`에서는 새 기록과 기존 로컬 임시 보관 기록을 전송하지 않습니다. `validate()`는 입력 항목의 채움 상태와 알 수 없는 항목명을 보여주는 로컬 점검 도구입니다. 법적 의무의 적용·충족·위반을 판단하지 않습니다. `preview()`로 전송 예정 값을 확인하십시오. 실제 전송으로 전환할 때는 `dry_run=False`로 재초기화하거나 정상 설정으로 프로그램을 다시 시작하십시오.

## 전송과 로컬 임시 보관

이벤트 기록 호출 스레드에서는 네트워크 전송을 하지 않습니다. 백그라운드 작업이 전송하며, 일시적인 전송 실패 시 로컬 임시 보관과 재시도를 사용합니다. 서버가 받은 기록을 확인한 뒤에만 해당 로컬 기록을 정리합니다. 일부 기록의 확인 결과가 빠진 응답을 받으면 확인되지 않은 기록은 보관해 다시 시도합니다. 같은 임시 보관 폴더를 여러 프로세스가 사용하는 경우도 처리합니다.

응답이 지연되어 대기열이 절반을 넘으면 해당 몫을 로컬 임시 보관으로 옮깁니다. 대기열이 가득 차면 새 기록을 거절하고 기존 대기 기록은 유지합니다. 임시 보관 용량을 초과할 때도 이미 보관한 기록을 지우지 않고 새 기록을 거절합니다. 디스크 오류, 서버의 최종 거부, 저장 용량 초과 등에서는 기록이 누락될 수 있으므로 아래 계수와 운영 화면을 함께 확인하십시오.

```python
xc.dropped_events()        # 대기열 초과로 거절된 기록
xc.failed_events()         # 서버가 최종 거절한 기록
xc.spool_rejected_events() # 로컬 임시 보관 단계에서 거절된 기록
xc.flush(timeout=5.0)
xc.shutdown()
```

`shutdown()` 후 `init()`으로 다시 시작할 수 있습니다. 서버리스 함수에서는 반환 직전 `flush()` 또는 `@xc.flush_on_return`을 사용하십시오. 전송 주소는 `https://`를 사용하십시오. `http://`는 이 컴퓨터의 `localhost` 시험에만 허용됩니다. 잘못된 형식의 `init()` 설정 인자는 해당 인자만 기본값으로 처리하고 수집을 계속하도록 개선했습니다. 설정이 예상대로 적용됐는지는 로컬 시험으로 확인하십시오.

## 1.0.1에서 1.0.2로 업그레이드

1. 1.0.1을 사용하는 프로세스를 모두 멈추십시오.
2. 같은 Python 환경에서 `python -m pip install -U xsoft-comply==1.0.2`를 실행하십시오. 해시 고정 설치라면 버전과 실제 배포 파일 확인값을 함께 갱신하십시오.
3. 애플리케이션을 다시 시작하고 버전과 수신 상태를 확인하십시오.
4. 시작 시 표시되는 1.0.1 형식 미전송 기록 경고와 `xc.legacy_unowned_events()`의 보존 건수를 확인하십시오.
5. 보존 건수가 0이 아니면 1.0.1 때 사용한 원래 API 키와 AI 시스템을 확인한 뒤 `contact@xsoftcomply.com`으로 복구를 문의하십시오.

1.0.1의 미전송 기록은 어느 API 키의 기록인지 알 수 없어 1.0.2가 `legacy_unowned/`에 보존하며 **자동으로도 수동 재전송 도구로도 보내지 않습니다**. 이 파일을 임의의 다른 키로 보내지 마십시오. 1.0.2가 만든 임시 보관 파일은 1.0.1이 읽지 못하므로 1.0.1로 되돌리지 마십시오.

## 로컬 기록 수동 재전송

먼저 현재 키를 환경변수에 설정하십시오. `--endpoint`는 필수이며, 실제 Event API 주소로 교체해야 합니다.

```bash
python -m xsoft_comply.resend --endpoint https://example.com/api/v1 --dry-run
python -m xsoft_comply.resend --endpoint https://example.com/api/v1
```

`--include-quarantine`는 격리 보관된 기록도 다시 확인합니다. `--all-owners`는 다른 키의 기록을 살펴보기만 하며 그 기록을 현재 키로 보내지 않습니다. 다른 키의 기록은 해당 키로 별도로 처리하십시오. `--dry-run`은 파일을 변경하거나 전송하지 않으며 보고용으로 종료코드 0을 반환합니다.

| 종료코드 | 의미 |
|---|---|
| `0` | 현재 재전송 대상이 남지 않았으며 1.0.1 형식 보존분도 없음. `--dry-run`에서는 조회 완료를 뜻함 |
| `1` | 전송 실패 등으로 재시도할 기록이 남음. API 키·주소·서버 상태를 확인 |
| `3` | 현재 재시도할 기록은 없으나 1.0.1 형식 보존분이 있음. 원래 키 확인 후 지원 창구 문의 |

종료코드보다 출력된 `remaining`과 `legacy_unowned` 건수를 함께 확인하십시오. 재시도할 기록과 1.0.1 형식 보존분이 함께 있으면 종료코드 1이 먼저 반환됩니다. 누락된 옵션이나 비어 있는 API 키 등 명령 입력 오류도 별도로 확인하십시오.

## 원문과 개인정보

LLM 프롬프트·응답 원문은 기본 전송 대상이 아닙니다. `content`에는 길이와 짧은 해시 등 파생 정보만 기록합니다. `metadata`에서는 정해진 민감 키를 제거하고 주민등록번호·전화번호·전자우편주소·유효한 카드번호 형식의 값을 자동으로 가립니다. 예를 들어 `4111-1111-1111-1111`은 `4111-****-****-1111`로 가립니다. 모든 개인정보 형식을 탐지하지는 못하므로 `metadata`에 개인정보나 원문을 넣지 마십시오.

조항 근거자료 `evidence`에는 SDK 자동 가림을 적용하지 않습니다. 감독자·검토자 관련 입력 값은 원문으로 저장되고 서비스 화면에서는 일부를 가려 표시할 수 있으며, 내려받기 자료에는 원문이 포함될 수 있습니다. 해당 필드에 무엇을 넣을지는 이용사업자가 검토하십시오.

## 기록 연결과 보관

서버는 각 운영기록을 앞 기록의 해시와 연결해 변경 여부를 확인할 수 있게 합니다. SDK 연결 상태와 조항별 자료의 채움 상태는 서로 다른 정보입니다. 해시 연결은 법적 판단이나 외부 기관의 검증 결과를 뜻하지 않습니다.

`retention_days`와 기록의 `_retain_until` 값은 서버의 실제 보관·파기 시점을 변경하지 않습니다. 운영 근거자료의 보관과 파기는 이용약관을 따릅니다.

## 조항별 입력 필드

입력값과 형식은 이용사업자가 전송 전에 확인하십시오. 필드가 비어 있어도 법적 상태를 뜻하지 않습니다.

### 제31조

| 입력 필드 | 타입 | 의미 | 입력 예시/형식 |
|---|---|---|---|
| prior_notice_provided | boolean | 사전 고지 이행 여부 | `True` |
| disclosure_method | text | 고지 방법 | `"example-01"` |
| disclosure_version | text | 적용한 고지 문구의 버전 | `"example-01"` |
| ai_generated_label | text | 생성 결과물 표시 이행 여부 | `"example-01"` |
| label_method | text | 표시 방법 | `"example-01"` |
| machine_readable_label | text | 기계판독 표시 시 안내 제공 여부 | `"example-01"` |
| synthetic_media | text | 실제와 구분이 어려운 결과물 해당 여부 | `"example-01"` |
| label_visibility | text | 이용자가 명확히 인식할 수 있는 방식 여부 | `"example-01"` |
| accessibility_method | text | 쉽게 확인할 수 있는 방법 | `"example-01"` |
| audience_consideration | text | 주된 이용자의 조건 고려 | `"example-01"` |
| artistic_relaxation | boolean | 예술적·창의적 표현물 완화 적용 여부 | `True` |
| exemption_type | text | 적용 예외 유형 | `"example-01"` |
| exemption_basis | text | 예외로 본 근거 | `"example-01"` |
| output_id | text | 대상 결과물 식별 | `"example-01"` |
| media_type | text | 결과물 매체 종류 | `"example-01"` |

### 제32조

| 입력 필드 | 타입 | 의미 | 입력 예시/형식 |
|---|---|---|---|
| training_compute | numeric | 학습 누적 연산량 | `0.72` |
| frontier_technology_status | text | 최첨단 기술 적용 여부 | `"example-01"` |
| frontier_technology_basis | text | 그 판단의 근거 | `"example-01"` |
| broad_serious_impact_status | text | 광범위·중대한 영향 우려 여부 | `"example-01"` |
| broad_serious_impact_basis | text | 그 판단의 근거 | `"example-01"` |
| model_id | text | 대상 모델 식별 | `"example-01"` |
| model_version | text | 모델 버전 | `"example-01"` |
| risk_management_policy_ref | text | 위험관리체계 문서 참조 | `"example-01"` |
| submission_status | text | 이행 결과 제출 여부 | `"example-01"` |
| submission_date | date | 제출일 | `"2026-10-02"` |
| risk_id | text | 위험 건 식별 | `"example-01"` |
| risk_signal_source | text | 위험을 식별한 경로 | `"example-01"` |
| risk_lifecycle_stage | text | 위험이 발생한 수명주기 단계 | `"example-01"` |
| risk_type | text | 위험 구분 | `"example-01"` |
| risk_level | text | 위험 평가 결과 | `"example-01"` |
| risk_action | text | 위험 완화 조치 | `"example-01"` |
| risk_status | text | 위험 처리 상태 | `"example-01"` |
| incident_id | text | 안전사고 건 식별 | `"example-01"` |
| incident_detection_source | text | 사고 감지 경로 | `"example-01"` |
| incident_type | text | 사고 구분 | `"example-01"` |
| incident_severity | text | 사고 심각도 | `"example-01"` |
| incident_action | text | 사고 대응 조치 | `"example-01"` |
| incident_status | text | 사고 처리 상태 | `"example-01"` |
| incident_close_reason | text | 사고 종결 사유 | `"example-01"` |

### 제34조

| 입력 필드 | 타입 | 의미 | 입력 예시/형식 |
|---|---|---|---|
| risk_mgmt_ref | text | 위험관리방안 문서 참조 | `"example-01"` |
| control_applied | boolean | 통제 실행 여부 | `True` |
| control_type | text | 통제 구분 | `"example-01"` |
| control_outcome | text | 통제 결과 | `"example-01"` |
| control_version | text | 적용된 통제 규칙의 버전 | `"example-01"` |
| explanation_policy_ref | text | 설명 방안 문서 참조 | `"example-01"` |
| decision_basis | text | 최종결과 도출에 활용된 주요 기준 | `"example-01"` |
| training_data_reference | text | 학습용데이터 개요 참조 | `"example-01"` |
| explanation_available | boolean | 설명 제공 가능 여부 | `True` |
| explanation_delivery_status | text | 설명 전달 결과 | `"example-01"` |
| user_protection_policy_ref | text | 이용자 보호 방안 문서 참조 | `"example-01"` |
| user_protection_action | text | 이용자 보호 조치 | `"example-01"` |
| protection_reason | text | 보호 조치 사유 | `"example-01"` |
| protection_outcome | text | 보호 조치 결과 | `"example-01"` |
| oversight_policy_ref | text | 사람 관리·감독 체계 문서 참조 | `"example-01"` |
| supervisor_id | text | 관리·감독자 식별 | `"example-01"` |
| supervisor_name | text | 관리·감독자 성명 | `"example-01"` |
| supervisor_contact | text | 관리·감독자 연락처 | `"example-01"` |
| supervisor_role | text | 관리·감독자 직책 | `"example-01"` |
| intervention_trigger | text | 개입 조건 | `"example-01"` |
| intervention_authority | text | 개입 권한 및 범위 | `"example-01"` |
| oversight_required | boolean | 사람 검토 필요 여부 | `True` |
| oversight_request_status | text | 사람 검토 요청 상태 | `"example-01"` |
| oversight_handover_status | text | 검토 대상 인계 상태 | `"example-01"` |
| reviewer_id | text | 검토자 식별 | `"example-01"` |
| reviewer_role | text | 검토자 직책 | `"example-01"` |
| review_status | text | 검토 진행 상태 | `"example-01"` |
| review_result | text | 검토 결과 | `"example-01"` |
| human_override | text | 사람에 의한 변경 여부 | `"example-01"` |
| override_reason | text | 변경 사유 | `"example-01"` |
| evidence_document_id | bigint | 조치 근거 문서 참조 | `1` |
| document_version | text | 보관 문서의 버전 | `"example-01"` |
| retention_status | text | 문서 보관 상태 | `"example-01"` |
| publication_status | text | 주요 내용 게시 여부 | `"example-01"` |
| publication_url | text | 게시 위치 | `"example-01"` |
| committee_matter | text | 위원회 심의·의결 사항 | `"example-01"` |
| alt_compliance_basis | text | 다른 법령에 따른 조치로 갈음하는 근거 | `"example-01"` |
| downstream_exemption | boolean | 중대한 기능 변경이 없어 이행한 것으로 보는 경우 | `True` |

### 제33·35·36조

이 세 조항에는 SDK 입력 필드가 없습니다. 관련 정보는 서비스의 관리 화면과 문서 등록 경로에서 관리합니다.

## 라이선스와 문의

MIT 라이선스입니다. 사용·복구 문의: `contact@xsoftcomply.com`.
