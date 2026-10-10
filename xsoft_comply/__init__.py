"""xsoft_comply Python SDK — 공개 표면.

기본 시그니처:
    import xsoft_comply as xc

    xc.init(api_key="xsk_live_...", endpoint="https://.../api/v1",
            log_llm_content=False, spool_dir="~/.xsoft_comply/spool",
            flush_interval=1.0, timeout=(2.0, 3.0), max_queue=10000, max_retries=5)

    with xc.session(agent_id="support-bot", product_id=None) as s:
        s.llm_call(model="gpt-4o-mini", provider="openai", tokens_in=..., tokens_out=..., latency_ms=...)
        s.tool_call(tool="crm.lookup", status="ok", latency_ms=12)
        s.data_access(resource="customer_profile", scope="read", record_count=1)
        s.decision(decision="escalate", options=3, confidence=0.72)
        s.human_review(human_review="approved", reviewer_role="manager")
        s.action(action="ticket.create", status="ok")

    ★ 조항 증적(제31~36조) · 전송 전 확인
      event_type 은 **6종**이다(session_start·llm_call·tool_call·data_access·action·session_end).
      `decision()`·`human_review()` 는 메서드로 그대로 사용하며 내부에서
      `action` + evidence(제34조) 로 조립되어 전송된다 = 기존 호출 형식은 유지된다.

        s.action("loan.approve", evidence=[
            xc.evidence_block(34, supervisor_id="sup-1", decision_basis="기준표 v3"),
            xc.evidence_block(31, ai_generated_label="labeled", label_method="watermark"),
        ])
        s.evidence(32, source="declared", evidence_key="model-2026",
                   model_id="m-100", model_version="1.2")   # 선언 증적

      전송 전 확인 —
        xc.init(..., dry_run=True)        # ① 전송 없이 전송 데이터·조항 연결·수집 항목·가림 결과 출력
        xc.validate(evt)                  # ② 조항별 수집 항목 전송 «사실» 집계
        xc.preview()                      # ③ 이벤트 1건 출력 확인 (= python -m xsoft_comply.preview)

    xc.instrument(agent)          # 에이전트 연동 설정
    xc.langchain_handler()        # LangChain BaseCallbackHandler 반환
    xc.flush(timeout=5.0)
    xc.shutdown()

★★ 애플리케이션 영향 최소화 원칙:
  - 모든 공개 함수는 예외를 호출자에게 전달하지 않는다.
  - KeyboardInterrupt · SystemExit 은 재raise (프로세스 종료를 막지 않기 위해).
  - init() 전 이벤트 기록 = no-op (중단되지 않는다).
  - atexit에 flush 등록 (타임아웃 2s) — 프로세스 종료를 막지 않는다.
"""
from __future__ import annotations

import atexit
import functools
import logging
import os
import threading
from contextlib import contextmanager
from typing import Any, Callable, Generator

from . import buffer as _buffer
from . import config as _config
from . import evidence as _evidence
from . import policy as _policy
from . import preview as _preview
from . import signing as _signing
from . import spool as _spool
from . import transport as _transport
from .policy import Decision, ToolOutcome
from .session import Session, session_ctx

__version__ = "1.0.2"
__all__ = [
    "init", "session", "instrument", "langchain_handler",
    "flush", "shutdown", "__version__",
    # 부가 기능
    "set_policy", "signing_public_key", "agents_run_hooks", "Decision", "ToolOutcome",
    # 서버리스 대응
    "dropped_events", "is_serverless", "flush_on_return",
    # 계수 신뢰도(S-19)
    "counters_unknown",
    # 조항 증적 · 전송 전 확인
    "evidence_block", "validate", "preview",
    # 전송실패 계측
    "failed_events", "spool_rejected_events",
    # 1.0.1 → 1.0.2 레거시 보존 계측(U-10 · 1002-546)
    "legacy_unowned_events",
]

_logger = logging.getLogger("xsoft_comply")

# 서버리스 감지
#   Lambda·Vercel·Cloud Run 은 **응답을 반환한 순간 실행환경을 얼린다.**
#   프로세스가 끝나는 게 아니라 «멈추는» 것이라 atexit 은 영영 안 불린다
#   → 큐에 남은 이벤트는 에러 한 줄 없이 사라진다. 감사 SDK 에서는 이것이 가장 불리한 경우다.
#   그래서 ① 감지되면 init 에서 경고하고 ② `flush_on_return` 훅을 준다.
_SERVERLESS_ENV = (
    "AWS_LAMBDA_FUNCTION_NAME",     # AWS Lambda
    "VERCEL",                       # Vercel Functions
    "K_SERVICE",                    # Google Cloud Run / Knative
    "FUNCTION_TARGET",              # Google Cloud Functions
    "FUNCTIONS_WORKER_RUNTIME",     # Azure Functions
)


def is_serverless() -> bool:
    """서버리스(freeze 방식) 실행환경으로 보이는가."""
    try:
        return any(os.environ.get(name) for name in _SERVERLESS_ENV)
    except BaseException:
        return False


def dropped_events() -> int:
    """**대기열 초과** 로 누락된 이벤트 누적 수.

    0 이 아닐 경우 감사기록에 누락 가능성이 있다는 뜻이다 — `max_queue` 를 키우거나
    `flush_interval` 을 줄여야 한다.

    ★ 서버가 거부해 전송되지 않은 건은 여기 잡히지 않는다 = `failed_events()` 로 따로 본다.
    """
    try:
        return _buffer.dropped()
    except BaseException:
        return 0


def counters_unknown() -> bool:
    """`dropped_events()`·`failed_events()` 의 영속 저장/복원이 실패했으면 True.

    ★ S-19 = True 면 "정확한 손실 0" 을 주장할 수 없다 — 계수가 «불명» 상태라는 뜻이다.
      (디스크 쓰기 실패 등으로 재시작 전 계수를 이어받지 못했을 때 True 가 된다.)
    """
    try:
        return _buffer.counters_unknown()
    except BaseException:
        return True


def failed_events() -> int:
    """**서버 거부** 로 전송되지 않은 이벤트 누적 수.

    `dropped_events()` 와 원인도 처방도 다르다 —

    | 함수 | 언제 늘어나나 | 무엇을 손봐야 하나 |
    |---|---|---|
    | `dropped_events()` | SDK 대기열이 가득 차 제외됐을 때 | `max_queue` ↑ · `flush_interval` ↓ |
    | `failed_events()`  | 서버가 400·401·403·413 등으로 거부했을 때 | API 키 · 요금제 · 본문 크기 |

    스풀에 보관된 건(403 PLAN_REQUIRED · 429 · 5xx)은 재전송 대상이라 여기 안 센다.
    ★ 스풀 자체가 거절(용량 초과)했거나 직렬화에 실패한 건은 여기 안 잡힌다 —
      `spool_rejected_events()` 로 따로 본다.
    """
    try:
        return _buffer.failed()
    except BaseException:
        return 0


def spool_rejected_events() -> int:
    """**스풀 단계**에서 유실된 누적 건수(M-04b).

    = 스풀 용량(max_spool_bytes) 초과로 거절된 건 + 직렬화 실패로 본문을 완전히
      잃은 건의 합계. `dropped_events()`(대기열 초과)·`failed_events()`(서버 거부)와
      원인이 다른 **세 번째 유실 경로**다 — 셋 다 0 이어야 "유실 없음"을 주장할 수 있다.
    """
    try:
        return _spool.rejected() + _spool.serialize_failed()
    except BaseException:
        return 0


def legacy_unowned_events() -> int:
    """1.0.1→1.0.2 전환 때 **보존만 되고 전송되지 않은** 레거시 이벤트 누적 건수(U-10·Y1).

    0 이 아니면 `legacy_unowned/` 아래(이동 실패 시 `events.jsonl` 원자리 포함)에
    그대로 남아 있다는 뜻이다 — ★ SDK 는 이 파일을 다시 읽거나 보내거나 지우지 않는다
    (546-이어 A-546-01 = 1.0.1 파일에는 원래 키 표식이 없어 수동 재전송도 오귀속을 막지
    못한다). 복구하려면 1.0.1 때 쓰던 키(제품·회사)를 확실히 아는 사람이 그 파일을
    오프라인으로 확인한 뒤 수동으로 이전해야 한다.
    """
    try:
        return _spool.legacy_unowned_count(_config.get().spool_dir)
    except BaseException:
        return 0


def flush_on_return(func: "Callable[..., Any]") -> "Callable[..., Any]":
    """서버리스 핸들러를 감싸 **반환 직전에 반드시 flush** 한다.

        import xsoft_comply as xc

        @xc.flush_on_return
        def handler(event, context):
            with xc.session(agent_id="my-agent") as s:
                ...
            return {"statusCode": 200}

    ★ 왜 필요한가 = 서버리스는 응답 반환 후 컨테이너를 얼린다. 백그라운드 전송
      스레드도 같이 멈추고, 다음 호출에서 컨테이너가 재사용되지 않으면 큐에 있던
      이벤트는 전부 사라진다(에러도 안 남는다). 반환 «전» 에 비워야만 한다.
    ★ flush 가 실패해도 핸들러의 반환값·예외는 그대로 통과시킨다(영향 최소화 원칙).
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        finally:
            try:
                flush(timeout=5.0)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                pass
    return wrapper

_initialized = False
_init_lock = threading.Lock()
_atexit_registered = False


# ── init ──────────────────────────────────────────────────────────────────────

def init(
    api_key: str,
    endpoint: str,
    *,
    log_llm_content: bool = False,
    spool_dir: str = "~/.xsoft_comply/spool",
    flush_interval: float = 1.0,
    timeout: tuple = (2.0, 3.0),
    max_queue: int = 10_000,
    max_retries: int = 5,
    log_events: dict | None = None,
    sign: bool = False,
    signing_key: str | None = None,
    policy: list | None = None,
    policy_enforce: bool = True,
    retention_days: int | None = None,
    dry_run: bool = False,
) -> None:
    """SDK를 초기화한다. 한 프로세스에서 여러 번 호출하면 재초기화된다.

    api_key 원문은 설정 객체에 보관되며 절대 로그에 출력되지 않는다.

    부가 기능 파라미터
      log_events     = 이벤트 종류별 on/off (`{"tool_call": False}`) · 세션 경계는 못 끈다
      sign           = 이벤트에 ed25519 전자서명을 붙인다(전송 스레드에서 서명)
      signing_key    = base64 32바이트 시드. 없으면 로컬 키파일을 쓰거나 새로 만든다
      policy         = 정책 규칙 목록(policy.py 규칙 형식) · `Session.run_tool()` 이 실제로 막는다
      policy_enforce = False 면 평가·기록만 하고 차단하지 않는다(도입 초기 관찰용)
      retention_days = 보관기간(기본 1825일 = 5년). 설정한 보관 기간이 이벤트에 기록된다

    전송 전 확인 파라미터
      dry_run        = True 면 **아무것도 전송하지 않는다.** 조립된 payload·조항매핑·
                       수집 항목 전송·마스킹 결과를 콘솔에 출력만 한다. 버퍼·스풀에도 남지 않는다.
    """
    global _initialized, _atexit_registered
    try:
        with _init_lock:
            # 1. 설정 저장
            _config.set_config(
                api_key=api_key,
                endpoint=endpoint,
                log_llm_content=log_llm_content,
                spool_dir=spool_dir,
                flush_interval=flush_interval,
                timeout=timeout,
                max_queue=max_queue,
                max_retries=max_retries,
                log_events=log_events,
                sign=sign,
                signing_key=signing_key,
                policy_enforce=policy_enforce,
                retention_days=retention_days,
                dry_run=dry_run,
            )
            # 1-b. 정책·서명 준비 (실패해도 수집은 계속된다)
            # ★ M-14 = policy 인자 형식이 잘못되면(list/tuple 이 아님) **그 인자만**
            #   기본값(규칙 0개)으로 떨어뜨리고 경고한다 — init() 전체를 중단시키지 않는다.
            if policy is not None and not isinstance(policy, (list, tuple)):
                try:
                    _logger.warning(
                        "xsoft_comply: policy 인자가 list/tuple 이 아니어서 무시합니다"
                        "(규칙 0개로 진행합니다).")
                except BaseException:
                    pass
                policy = None
            _policy.set_rules(policy)
            if sign:
                try:
                    _signing.init(signing_key, spool_dir)
                except BaseException:
                    _signing.reset()  # 키 준비 실패 = 서명만 포기, 수집은 계속
            # 2. 버퍼 초기화
            # ★ M-14 = max_queue 는 **검증을 거친 cfg 값**을 쓴다(원시 인자를 그대로 쓰면
            #   max_queue<=0 처럼 set_config() 가 10,000 으로 교정한 값이 여기 반영되지 않는다).
            _buffer.init(maxsize=_config.get().max_queue)
            # 2-b. S-19 = 이전 실행이 남긴 누락 계수를 이어서 센다(재시작해도 0 으로 안 돌아간다).
            _buffer.load_persisted_counters(spool_dir)
            # 2-c. ★ U-10(1002-546 · M-29 재정정 · Y1 546-이어) = 1.0.1 형식 단일 스풀
            #   파일(events.jsonl) 이 있으면 **어느 키로도 자동 귀속시키지 않고**
            #   `legacy_unowned/` 로 보존만 한다(옮기는 함수 안에서 보존 건수를 경고
            #   로그로 알린다) — ★ 보존분을 SDK 가 다시 보내는 길은 없다(A-546-01 =
            #   1.0.1 파일에는 원래 키 표식이 없어 수동 재전송도 오귀속을 막지 못한다).
            #   보존 건수는 `legacy_unowned_events()` 로 조회해 **확인된 원래 키를
            #   아는 사람이 오프라인으로** 이전한다.
            try:
                _spool.migrate_legacy_events_file(
                    spool_dir, _spool.owner_fingerprint(api_key, endpoint))
            except BaseException:
                pass
            # 3. 전송 데몬 스레드 시작
            _transport.start()
            _initialized = True
            # 4. atexit 등록 (최초 1회)
            if not _atexit_registered:
                atexit.register(_atexit_handler)
                _atexit_registered = True
            # 4-b. dry_run 은 알림 없이 넘어가면 안 된다 = 「보냈다고 믿는데 안 갔다」가 가장 불리한 경우다.
            if dry_run:
                try:
                    _logger.warning(
                        "xsoft_comply: dry_run=True 입니다. 이벤트를 **전송하지 않고** "
                        "조립 결과만 출력합니다. 실제 수집을 켜려면 dry_run 을 빼세요.")
                except BaseException:
                    pass
            # 5. 서버리스면 경고
            #    여기서 알림 없이 넘어가면 고객은 유실을 배포 몇 주 뒤에나 안다.
            if is_serverless():
                try:
                    _logger.warning(
                        "xsoft_comply: 서버리스 환경으로 보입니다. 응답 반환 후 실행환경이 "
                        "얼면 atexit flush 가 돌지 않아 이벤트가 유실됩니다. "
                        "핸들러에 @xsoft_comply.flush_on_return 을 붙이거나 반환 직전에 "
                        "xsoft_comply.flush() 를 직접 부르세요.")
                except BaseException:
                    pass
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass  # init 실패해도 프로세스를 중단시키지 않는다


# ── session ───────────────────────────────────────────────────────────────────

_product_id_warned = False
_product_id_warn_lock = threading.Lock()


def _warn_product_id_not_sent_once() -> None:
    """★ M-17 = `product_id` 는 **전송되지 않는다**(제품은 API 키로 정해진다). 1회만 경고."""
    global _product_id_warned
    with _product_id_warn_lock:
        if _product_id_warned:
            return
        _product_id_warned = True
    try:
        _logger.warning(
            "xsoft_comply: session(product_id=...) 는 전송되지 않습니다 — 제품은 "
            "발급받은 API 키로 정해집니다. product_id 인자는 로컬 참고용입니다.")
    except BaseException:
        pass


@contextmanager
def session(
    agent_id: str | None = None,
    product_id: Any | None = None,
) -> Generator[Session, None, None]:
    """session_start/end를 자동 발행하는 컨텍스트 매니저.

    init() 전에 사용해도 중단되지 않는다(버퍼가 없으면 put이 no-op).

    ★ M-17 = `product_id` 는 전송되지 않는다(제품은 API 키로 정해진다) — 넘기면
      1회 경고한다.
    """
    if product_id is not None:
        _warn_product_id_not_sent_once()
    try:
        with session_ctx(agent_id=agent_id, product_id=product_id) as s:
            yield s
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        # contextmanager가 GeneratorExit 등을 던질 수 있다 — 그대로 전달한다
        # 그러나 yield 전 예외는 여기 오지 않음
        raise


# ── instrument ────────────────────────────────────────────────────────────────

def instrument(agent: Any) -> Any:
    """에이전트 객체에 xsoft_comply 이벤트 훅을 주입한다.

    유형에 의존하지 않는 방식으로 연동 처리를 적용한다(가능한 경우).
    OpenAI Agents SDK 스타일 hooks 속성이 있으면 주입한다.
    일반 callable은 run/invoke/__call__을 래핑한다.
    """
    try:
        # OpenAI Agents SDK 스타일
        from .integrations.openai_agents import wrap_agent
        agent = wrap_agent(agent)

        # 일반 callable 래핑 — run / invoke
        _wrap_methods(agent)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass
    return agent


def _wrap_methods(agent: Any) -> None:
    """run · invoke · __call__ 을 래핑해 tool_call 이벤트를 발행한다."""
    import time as _time
    from .session import get_current

    _WRAP = ("run", "invoke")

    for attr in _WRAP:
        original = getattr(agent, attr, None)
        if original is None or not callable(original):
            continue

        def _make_wrapper(fn: Any, name: str) -> Any:
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                t0 = _time.monotonic()
                try:
                    result = fn(*args, **kwargs)
                    latency = (_time.monotonic() - t0) * 1000
                    try:
                        sess = get_current()
                        if sess is not None:
                            sess.tool_call(tool=f"agent.{name}", status="ok", latency_ms=latency)
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except BaseException:
                        pass
                    return result
                except (KeyboardInterrupt, SystemExit):
                    raise
                except BaseException as exc:
                    latency = (_time.monotonic() - t0) * 1000
                    try:
                        sess = get_current()
                        if sess is not None:
                            sess.tool_call(tool=f"agent.{name}", status="error", latency_ms=latency)
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except BaseException:
                        pass
                    raise exc
            return wrapper

        try:
            setattr(agent, attr, _make_wrapper(original, attr))
        except (AttributeError, TypeError):
            pass


# ── langchain_handler ─────────────────────────────────────────────────────────

def langchain_handler() -> Any:
    """LangChain BaseCallbackHandler 인스턴스를 반환한다.

    langchain 미설치여도 이 함수 호출은 실패하지 않는다.
    """
    try:
        from .integrations.langchain import make_handler
        HandlerClass = make_handler()
        return HandlerClass()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        # langchain 없거나 기타 오류 → 빈 객체 반환
        return object()


# ── 정책 · 서명 ──────────────────────────────────────────

def set_policy(rules: list | None) -> int:
    """정책 규칙을 교체한다. 적용된 규칙 수를 반환한다(예외 없음)."""
    try:
        return _policy.set_rules(rules)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return 0


def agents_run_hooks() -> Any:
    """OpenAI Agents SDK 의 `Runner.run(..., hooks=)` 에 넘길 훅 객체.

    `instrument(agent)` 는 에이전트 단위 훅(`agent.hooks`)을 붙이고,
    이 함수는 실행 단위 훅을 준다 — 둘 다 지원해야 핸드오프까지 잡힌다.
    """
    try:
        from .integrations.openai_agents import run_hooks
        return run_hooks()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return None


def signing_public_key() -> str | None:
    """현재 서명 공개키(base64). 서명을 안 쓰면 None.

    감사인에게 넘겨줄 값이다 — **개인키는 어떤 경로로도 반환하지 않는다.**
    """
    try:
        return _signing.public_key_b64()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return None


# ── 조항 증적 · 전송 전 확인 ──────────────────────────────

def evidence_block(
    obligation_no: int,
    *,
    source: str = "runtime",
    evidence_key: str | None = None,
    **fields: Any,
) -> dict:
    """조항 증적 블록 1개를 만든다(전송하지 않는다).

        s.action("loan.approve", evidence=[
            xc.evidence_block(34, supervisor_id="sup-1", decision_basis="기준표 v3"),
            xc.evidence_block(31, ai_generated_label="labeled", label_method="watermark"),
        ])
    """
    try:
        return _evidence.block(
            obligation_no, source=source, evidence_key=evidence_key, **fields
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return {"obligation_no": obligation_no, "source": source, "fields": {}}


def validate(event: Any) -> dict:
    """전송 전 확인 ② — 조항별 **수집 항목 전송 상황**을 집계합니다.

        report = xc.validate(evt)
        report["field_empty_total"]           # 전송되지 않은 수집 항목의 총 개수
        report["blocks"][0]["field_empty"]

    ★ 변호사법 제109조 = 준수 또는 위반을 판단하지 않습니다. 수집 항목 N개 중 M개가
      전송되었다는 사실만 반환합니다.
    ★ 예외를 던지지 않습니다.
    """
    return _evidence.validate(event)


def preview(event: Any = None) -> str:
    """전송 전 확인 ③ — 이벤트 1건의 전송 내용을 로컬에서 문자열로 확인한다.

    인자를 주면 그 이벤트를, 없으면 샘플 1콜을 렌더링한다.
    `python -m xsoft_comply.preview` 로도 같은 것을 볼 수 있다.
    """
    return _preview.preview(event)


# ── flush / shutdown ──────────────────────────────────────────────────────────

def flush(timeout: float = 5.0) -> None:
    """버퍼가 빌 때까지 기다린다. 최대 timeout초 후 반환."""
    try:
        _transport.flush_buffer(timeout=float(timeout))
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass


def shutdown() -> None:
    """전송 스레드를 정지한다. 종료 전 마지막에 호출한다.

    stop() 후 transport 스레드 드레인 루프가 잔여 큐를 스풀로 내린다.
    join timeout 경과 시 이 함수가 추가로 스풀 안전망을 실행한다.
    """
    try:
        _transport.stop(timeout=5.0)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass
    # transport 스레드 join timeout 경과 시 잔여 큐 → 스풀 안전망
    _drain_queue_to_spool()


# ── atexit ────────────────────────────────────────────────────────────────────

def _drain_queue_to_spool() -> None:
    """큐에 남은 미전송 이벤트를 스풀에 기록한다. 빠른 로컬 디스크 I/O만.

    flush timeout 경과 후 안전망으로 호출된다. 네트워크 재시도 없음.
    """
    try:
        remaining = _buffer.drain()
        if remaining:
            cfg = _config.get()
            # ★ 1001-543 D-1 실측 발견 = owner 를 안 넘기면 "unknown" 으로 적재돼
            #   이 키의 정상 재시도(claim_all)·resend 가 다시는 못 찾는다(소유자 불일치로
            #   영구 미아가 된다 — 다른 전 호출부(transport.py)는 전부 owner 를 넘긴다).
            _spool.append(
                remaining, cfg.spool_dir, cfg.max_spool_bytes,
                owner=_spool.owner_fingerprint(cfg.api_key, cfg.endpoint),
            )
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass
    # ★ 큐뿐 아니라 «전송 중이던 배치» 도 내린다 — 종료와 겹치면 그것이 전부 사라진 사례가 있다.
    try:
        _transport.spool_inflight()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass


def _atexit_handler() -> None:
    """프로세스 종료 시 자동 호출.

    1. flush(timeout=5.0): 큐 비고 in-flight==0 될 때까지 실제 대기.
       확인된 POST 시간 0.9~1.9s → 5s 로 여유를 확보한다.
    2. flush timeout 경과 후 큐에 남은 항목 → 스풀로 내린다.
       xsoft_comply.resend 로 회수 가능.
    daemon 스레드이므로 프로세스 종료를 막지 않는다.
    """
    try:
        flush(timeout=5.0)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass
    # flush timeout 경과 후에도 큐에 남아있는 이벤트 → 스풀 안전망
    _drain_queue_to_spool()
