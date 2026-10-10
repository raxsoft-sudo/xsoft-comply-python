"""세션 컨텍스트 매니저 — trace_id 생성 · seq 채번 · session_start/end 자동 발행.

기본 패턴:
    with xc.session(agent_id="support-bot", product_id=None) as s:
        s.llm_call(...)
        ...  # session_start/end 자동

설계 노트:
  - 각 Session은 trace 1개에 대응한다.
  - seq는 Session 인스턴스당 1부터 시작하는 단조 증가 카운터다(threading.Lock 보호).
  - «현재 세션» 은 contextvars.ContextVar 로 관리한다(스레드뿐 아니라 asyncio 태스크 경계도
    올바르게 격리·복원해야 한다 — threading.local 은 asyncio 이벤트루프 안에서 여러 코루틴이
    한 스레드를 공유할 때 서로 다른 trace 를 섞는다. ContextVar.set()/reset(token) 은
    중첩된 session() 블록도 올바르게 복원한다).
  - _emit()의 모든 예외는 전달하지 않고 처리한다(영향 최소화 원칙).
  - 공개 메서드(llm_call 등)는 인자 형변환까지 포함해 **메서드 전체**를 보호한다 —
    _emit() 안쪽만 감싸면 그 앞에서 하는 int()/float() 변환 실패가 고객 코드로 그대로 샌다.
"""
from __future__ import annotations

import contextvars
import functools
import threading
import uuid
from contextlib import contextmanager
from typing import Any, Generator

from . import buffer as _buffer
from . import config as _config
from . import events as _events
from . import evidence as _evidence
from . import policy as _policy
# ★ 이름 충돌 방지 = `from . import preview` 는 **모듈이 아니라 `__init__.py` 의 `preview()` 함수**를 준다
#   (패키지 속성이 같은 이름의 공개 함수로 덮여 있다 · `session` 도 같은 상황이다).
#   그 경우 dry_run 출력이 AttributeError 로 알림 없이 사라질 수 있다.
#   서브모듈을 확실히 집으려면 `from .preview import emit` 처럼 **모듈 경로로** 가져온다.
from .preview import emit as _dry_run_emit

# ── 현재 세션 (ContextVar · S-20) ────────────────────────────────────────────
_current: "contextvars.ContextVar[Session | None]" = contextvars.ContextVar(
    "xsoft_comply_current_session", default=None
)


def get_current() -> "Session | None":
    """현재 컨텍스트(스레드·asyncio 태스크)에서 활성화된 Session을 반환한다. 없으면 None."""
    try:
        return _current.get()
    except BaseException:
        return None


def _set_current(s: "Session | None") -> "contextvars.Token[Any]":
    return _current.set(s)


def new_trace_id() -> str:
    """tr_<uuid4 hex> 형식."""
    return f"tr_{uuid.uuid4().hex}"


def _fail_open(fn: Any) -> Any:
    """공개 메서드 전체를 감싼다 — SDK 내부의 어떤 예외도 고객 코드로 전달하지 않는다(F-01·F-02).

    ★ 대상 값 변환(int()·float() 등)은 메서드 본문 안에서 일어난다.
      _emit() 만 감싸서는 그 앞의 변환 실패(S-09·S-10)를 막지 못한다.
    """
    @functools.wraps(fn)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return fn(self, *args, **kwargs)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return None
    return wrapper


def _safe_int(value: Any) -> "int | None":
    try:
        return int(value)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return None


def _safe_float(value: Any) -> "float | None":
    try:
        return float(value)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return None


# ── Session 클래스 ─────────────────────────────────────────────────────────────

class Session:
    """단일 trace에 속한 이벤트 발행기.

    기본 메서드 시그니처:
      llm_call · tool_call · data_access · decision · human_review · action
    """

    def __init__(
        self,
        trace_id: str,
        agent_id: str | None,
        product_id: Any | None,
    ) -> None:
        self.trace_id = trace_id
        self.agent_id = agent_id
        self.product_id = product_id
        self._seq = 0
        self._lock = threading.Lock()

    # ── 내부 ────────────────────────────────────────────────────────────────

    def _next_seq(self) -> int:
        """seq는 1부터 시작하는 단조 증가. 스레드 안전. (하위호환 별칭 — 직접 쓰지 않는다.)"""
        with self._lock:
            self._seq += 1
            return self._seq

    def _emit(self, event_type: str, **kwargs: Any) -> None:
        """이벤트를 조립해 버퍼에 넣는다.

        ★ 영향 최소화 원칙: 예외를 호출자에게 전달하지 않는다.
        KeyboardInterrupt · SystemExit은 재raise(프로세스 종료를 막지 않기 위해).

        ★ 이벤트 종류별 로그 설정 = 비활성화한 이벤트 종류는 순번(seq)도 부여하지 않고 제외한다.
          순번을 소비하고 제외하면 서버가 순번 누락으로 이후 이벤트를 거부한다.

        ★★ GX-03 = seq 는 **이벤트 조립이 성공했을 때만** 확정한다.
          예전에는 seq 를 먼저 채번한 뒤 build() 를 불렀다 — build() 가 실패하면(예: 비직렬화
          가능 객체·순환참조가 json_safe 이전 다른 경로로 새어 들어온 경우) 그 seq 번호는
          영원히 «구멍»(SEQ_GAP)으로 남아 이후 이벤트가 전부 막힌다.
          그래서 채번과 build() 를 **같은 락 구간**에 묶고, build() 가 실패하면 seq 를
          커밋하지 않는다(그 시도는 없었던 것으로 남긴다 — 다음 이벤트가 seq+1 을 그대로 쓴다).

        ★★ M-01(1001-543) = seq 는 **큐 넣기(put)가 성공했을 때도** 확정한다. 예전에는
          build() 성공만으로 곧바로 seq 를 커밋했다 — 대기열이 가득 차 `buffer.put()` 이
          거절해도 그 seq 는 이미 확정돼 있어 영원한 SEQ_GAP 이 되고, 그 뒤 모든 이벤트가
          서버에서 막혔다(buffer.put() 의 S-14 거절 규칙과 짝이 안 맞았다). 지금은 put()
          의 반환값을 보고, 성공했을 때만 커밋한다(dry_run 은 큐를 쓰지 않으므로 조립
          성공만으로 커밋한다 — 로컬 전용이라 거절될 길이 없다).
        """
        try:
            cfg = _config.get()
            if not cfg.event_enabled(event_type):
                return
            evt = None
            is_dry = False
            with self._lock:
                candidate_seq = self._seq + 1
                try:
                    built = _events.build(
                        event_type=event_type,
                        seq=candidate_seq,
                        trace_id=self.trace_id,
                        agent_id=self.agent_id,
                        product_id=self.product_id,
                        **kwargs,
                    )
                except (KeyboardInterrupt, SystemExit):
                    raise
                except BaseException:
                    return  # 조립 실패 — seq 를 소비하지 않는다(GX-03)
                if cfg.dry_run:
                    self._seq = candidate_seq
                    evt = built
                    is_dry = True
                elif _buffer.put(built):
                    self._seq = candidate_seq  # 큐 넣기 성공했을 때만 확정(M-01)
                # else: 큐가 가득 차 거절됨 — 이 시도는 없었던 것으로 남긴다(seq 미확정)
            if is_dry and evt is not None:
                # ★ 전송 전 확인 dry_run = **전송하지 않는다.** 조립 결과만 로컬에 출력한다.
                #   버퍼에도 넣지 않으므로 스풀·재전송 경로에도 남지 않는다.
                _dry_run_emit(evt)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass  # 예외를 전달하지 않고 처리한다

    # ── 정책 기반 실행 차단 ───────────────────────────────────

    def check(self, kind: str = "tool_call", **fields: Any) -> "_policy.Decision":
        """정책만 평가한다(이벤트 발행 없음). 예외를 발생시키지 않는다."""
        try:
            return _policy.evaluate(kind, fields)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            return _policy.ALLOW

    def run_tool(
        self,
        tool: str,
        fn: Any = None,
        *args: Any,
        resource: str | None = None,
        **kwargs: Any,
    ) -> "_policy.ToolOutcome":
        """정책을 평가하고 **통과한 경우에만** 도구를 실행한다.

        차단이면 `fn` 을 호출하지 않고 `blocked=True` 인 결과를 돌려준다(예외 없음).
        ★ `fn` 자체가 발생시키는 예외는 고객 코드의 것이므로 그대로 전달한다 —
          전달하지 않으면 에이전트가 «성공했다» 고 오인한다.
        """
        decision = self.check("tool_call", tool=tool, resource=resource)
        enforce = True
        try:
            enforce = _config.get().policy_enforce
        except BaseException:
            pass

        if decision.blocked and enforce:
            self._emit(
                "tool_call",
                tool=tool,
                policy_result=decision.label,
                metadata={
                    "status": "blocked",
                    "policy_rule": decision.rule_id,
                    "policy_reason": decision.reason,
                    **({"resource": resource} if resource else {}),
                },
            )
            return _policy.ToolOutcome(decision, value=None, called=False)

        if fn is None:
            self._emit(
                "tool_call",
                tool=tool,
                policy_result=decision.label,
                metadata={"status": "ok", "policy_rule": decision.rule_id},
            )
            return _policy.ToolOutcome(decision, value=None, called=False)

        import time as _time

        started = _time.monotonic()
        try:
            value = fn(*args, **kwargs)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            self._emit(
                "tool_call",
                tool=tool,
                policy_result=decision.label,
                metadata={
                    "status": "error",
                    "latency_ms": (_time.monotonic() - started) * 1000,
                    "policy_rule": decision.rule_id,
                },
            )
            raise
        self._emit(
            "tool_call",
            tool=tool,
            policy_result=decision.label,
            metadata={
                "status": "ok",
                "latency_ms": (_time.monotonic() - started) * 1000,
                "policy_rule": decision.rule_id,
                **({"resource": resource} if resource else {}),
            },
        )
        return _policy.ToolOutcome(decision, value=value, called=True)

    # ── 공개 메서드 ────────────────────────────────────

    @_fail_open
    def llm_call(
        self,
        model: str | None = None,
        *,
        provider: str | None = None,
        tokens_in: int | None = None,
        tokens_out: int | None = None,
        latency_ms: float | None = None,
        content: Any = None,
        evidence: Any = None,
        **extra: Any,
    ) -> None:
        """LLM 호출 이벤트. content는 원문 미전송 → fingerprint만.

        ★ `evidence=` = 이 호출이 동시에 조항 증적이기도 할 때 붙인다
          (예: 생성형 결과물 표시 제31조). 안 쓰면 종전 그대로다.
        ★ S-09 = tokens_in·tokens_out·latency_ms 형변환 실패는 그 필드만 빼고 계속한다
          (고객이 문자열·None 아닌 이상한 값을 실수로 넘겨도 예외가 새지 않는다).
        """
        meta: dict[str, Any] = {}
        if provider is not None:
            meta["provider"] = provider
        if tokens_in is not None:
            v = _safe_int(tokens_in)
            if v is not None:
                meta["tokens_in"] = v
        if tokens_out is not None:
            v = _safe_int(tokens_out)
            if v is not None:
                meta["tokens_out"] = v
        if latency_ms is not None:
            v = _safe_float(latency_ms)
            if v is not None:
                meta["latency_ms"] = v
        meta.update(extra)
        self._emit("llm_call", model=model, metadata=meta, content=content, evidence=evidence)

    @_fail_open
    def tool_call(
        self,
        tool: str | None = None,
        *,
        status: str | None = None,
        latency_ms: float | None = None,
        evidence: Any = None,
        **extra: Any,
    ) -> None:
        meta: dict[str, Any] = {}
        if status is not None:
            meta["status"] = status
        if latency_ms is not None:
            v = _safe_float(latency_ms)
            if v is not None:
                meta["latency_ms"] = v
        meta.update(extra)
        self._emit("tool_call", tool=tool, metadata=meta, evidence=evidence)

    @_fail_open
    def data_access(
        self,
        *,
        resource: str | None = None,
        scope: str | None = None,
        record_count: int | None = None,
        evidence: Any = None,
        **extra: Any,
    ) -> None:
        meta: dict[str, Any] = {}
        if resource is not None:
            meta["resource"] = resource
        if scope is not None:
            meta["scope"] = scope
        if record_count is not None:
            v = _safe_int(record_count)
            if v is not None:
                meta["record_count"] = v
        meta.update(extra)
        self._emit("data_access", metadata=meta, evidence=evidence)

    # ── ★ 제34조(고영향 책무) 증적 — decision·human_review 는 여기로 기록된다 ────────
    #
    #   신규 발행 event_type 은 6종이다.
    #   `decision`·`human_review` 는 **이벤트 종류가 아니라 조항 증적**이었다 —
    #   「무엇을 했나」(action)와 「그것이 어느 법 조항의 근거인가」(evidence)는 층이 다르다.
    #   메서드 이름은 개발자 편의로 그대로 제공하고, 내부에서
    #   `action` + evidence(제34조) 블록으로 조립해 보낸다.
    #   ★ 기존 호출 형식은 유지된다. 달라지는 것은 «어디에 보관되는가» 뿐이다.

    @_fail_open
    def decision(
        self,
        decision: str | None = None,
        *,
        options: int | None = None,
        confidence: float | None = None,
        evidence_key: str | None = None,
        **extra: Any,
    ) -> None:
        """의사결정 근거 (제34조①2 «설명 방안의 수립·시행»).

        `decision` → `evidence.fields.decision_basis` 로 간다.
        """
        meta: dict[str, Any] = {}
        if options is not None:
            v = _safe_int(options)
            if v is not None:
                meta["option_count"] = v
        if confidence is not None:
            v = _safe_float(confidence)
            if v is not None:
                meta["confidence"] = v
        meta.update(extra)
        self._emit(
            "action",
            action="decision",
            metadata=meta,
            evidence=[_evidence.block(
                34,
                source="runtime",
                evidence_key=evidence_key,
                decision_basis=decision,
            )],
        )

    @_fail_open
    def human_review(
        self,
        human_review: str | None = None,
        *,
        reviewer_role: str | None = None,
        reviewer_id: str | None = None,
        human_override: str | None = None,
        override_reason: str | None = None,
        evidence_key: str | None = None,
        **extra: Any,
    ) -> None:
        """사람의 관리·감독 기록 (제34조①4).

        `human_review` → `evidence.fields.review_result`,
        `reviewer_role` → `evidence.fields.reviewer_role` 로 간다.
        """
        meta: dict[str, Any] = {}
        if reviewer_role is not None:
            meta["reviewer_role"] = reviewer_role
        meta.update(extra)
        self._emit(
            "action",
            action="human_review",
            metadata=meta,
            evidence=[_evidence.block(
                34,
                source="runtime",
                evidence_key=evidence_key,
                review_result=human_review,
                reviewer_role=reviewer_role,
                reviewer_id=reviewer_id,
                human_override=human_override,
                override_reason=override_reason,
            )],
        )

    @_fail_open
    def action(
        self,
        action: str | None = None,
        *,
        status: str | None = None,
        evidence: Any = None,
        **extra: Any,
    ) -> None:
        meta: dict[str, Any] = {}
        if status is not None:
            meta["status"] = status
        meta.update(extra)
        self._emit("action", action=action, metadata=meta, evidence=evidence)

    # ── ★ 선언 증적 (조항 31~36 공용) ──────────────────────────────────────────

    @_fail_open
    def evidence(
        self,
        obligation_no: int,
        *,
        source: str = "declared",
        evidence_key: str | None = None,
        **fields: Any,
    ) -> None:
        """조항 증적을 한 건 «선언» 한다 (제31~36조 공용).

            s.evidence(31, prior_notice_provided=True, disclosure_method="chat_banner")
            s.evidence(32, source="declared", evidence_key="model-2026",
                       model_id="m-100", model_version="1.2",
                       risk_management_policy_ref="RM-2026-01")

        ★ 선언 정보도 **일반 이벤트 기록 절차를 통해 저장한다.**
          기록의 변경 이력과 순서 무결성을 함께 확인할 수 있도록 하기 위함이다.
        ★ `evidence_key` 는 같은 증적의 중복 등록을 방지하기 위한 식별자다. 같은 선언을 여러 번 보내도 증적은 1행이다.
          생략하면 이벤트마다 새 키(`sdk:<event_id>`)가 되어 **매번 새 증적**이 된다 —
          「해마다 갱신하는 선언」 처럼 자연키가 있는 값이면 꼭 넘겨야 한다.

        ★ S-10 = obligation_no 가 숫자로 바뀌지 않으면(고객 오타) **아무 것도 보내지 않고 조용히
          반환한다** — 예전에는 이 int() 변환이 메서드 몸통에서 직접 일어나 고객 코드로 예외가 샜다.
        """
        no = _safe_int(obligation_no)
        if no is None:
            return
        self._emit(
            "action",
            action=f"evidence.{no}",
            metadata={},
            evidence=[_evidence.block(
                no, source=source, evidence_key=evidence_key, **fields
            )],
        )


# ── contextmanager ────────────────────────────────────────────────────────────

@contextmanager
def session_ctx(
    agent_id: str | None = None,
    product_id: Any | None = None,
) -> Generator[Session, None, None]:
    """with xc.session(...) as s: — session_start/end 자동 발행.

    ★ __exit__에서 예외가 발생해도 session_end 를 발행한다(finally).
    ★ S-20 = 이전 값을 **토큰으로 복원**한다(단순히 None 으로 되돌리면 중첩된
      `with xc.session()` 블록에서 바깥쪽 세션 컨텍스트가 사라진다). ContextVar 는
      asyncio 태스크 경계에서도 올바르게 격리·복원된다(threading.local 은 안 된다).
    """
    trace_id = new_trace_id()
    s = Session(trace_id=trace_id, agent_id=agent_id, product_id=product_id)
    token = _set_current(s)
    s._emit("session_start")
    try:
        yield s
    finally:
        s._emit("session_end")
        try:
            _current.reset(token)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            _set_current(None)
