"""OpenAI Agents SDK 연동 — 유형에 의존하지 않는 구현.

★ 인터페이스
  openai-agents(0.21.x)의 실제 인터페이스는 아래와 같고, 전부 **async** 다.

    AgentHooks(에이전트에 붙음 · `agent.hooks`)
      on_start / on_end / on_handoff / on_tool_start / on_tool_end / on_llm_start / on_llm_end
    RunHooks(실행에 붙음 · `Runner.run(..., hooks=)`)
      on_agent_start / on_agent_end / on_handoff / on_tool_start / on_tool_end / on_llm_start / on_llm_end

  훅 이름이 실제 인터페이스와 어긋나면 아무 것도 호출되지 않는다(= 감사기록 0건).
  그래서 실제 인터페이스 이름·시그니처에 맞춘다.

★ 의존성 0 유지 = agents 패키지를 import 하지 않는다. 메서드 이름·시그니처만 맞춘 순수 클래스다.
★ 원문(input/output) 미전송 = 길이·토큰 수만 남긴다.
"""
from __future__ import annotations

import functools
import time
from typing import Any


def _isolate(fn: Any) -> Any:
    """훅 본체 전체를 SDK 내부 실패로부터 보호한다(S-12).

    ★ `_emit()` 안쪽만 감싸면 그 앞의 시계(time.monotonic)·이름 변환·메타 구성 실패가
      그대로 호출자(에이전트 프레임워크)로 샌다 — 프레임워크는 이 예외를 자기 것으로
      오인해 에이전트 실행을 중단한다. 메서드 전체를 감싸야 한다.
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


def _current_session() -> Any:
    """스레드 로컬 현재 세션. 없으면 None."""
    try:
        from ..session import get_current
        return get_current()
    except Exception:
        return None


def _name_of(obj: Any, default: str = "unknown") -> str:
    try:
        name = getattr(obj, "name", None)
        if name:
            return str(name)
        return type(obj).__name__
    except Exception:
        return default


def _usage_of(response: Any) -> dict[str, Any]:
    """ModelResponse.usage → tokens_in/out. 필드가 없으면 값을 채우지 않는다."""
    meta: dict[str, Any] = {}
    try:
        usage = getattr(response, "usage", None)
        if usage is None:
            return meta
        for src, dst in (
            ("input_tokens", "tokens_in"),
            ("output_tokens", "tokens_out"),
            ("requests", "requests"),
        ):
            value = getattr(usage, src, None)
            if isinstance(value, int):
                meta[dst] = value
    except Exception:
        pass
    return meta


class _HookBase:
    """훅 공통 — 시작시각 보관 · 전 메서드에서 예외를 전달하지 않고 처리한다."""

    def __init__(self) -> None:
        self._started: dict[str, float] = {}

    def _begin(self, key: str) -> None:
        self._started[key] = time.monotonic()

    def _elapsed_ms(self, key: str) -> float:
        return (time.monotonic() - self._started.pop(key, time.monotonic())) * 1000

    def _emit(self, method: str, **kwargs: Any) -> None:
        try:
            session = _current_session()
            if session is None:
                return
            getattr(session, method)(**kwargs)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    # ── 공통 본체 (동기) — async 래퍼가 이것만 호출한다 ────────────────────
    #   ★ S-12 = 아래 7개는 전부 @_isolate 로 감싼다 — 시계·문자열화·메타 변환·
    #     버퍼 실패 어느 것도 에이전트 프레임워크로 전달하지 않는다(33훅 공통 경계).
    @_isolate
    def _agent_start(self, agent: Any) -> None:
        self._begin(f"agent:{id(agent)}")
        self._emit("action", action=f"agent.start:{_name_of(agent)}", status="ok")

    @_isolate
    def _agent_end(self, agent: Any, output: Any) -> None:
        extra: dict[str, Any] = {}
        try:
            if output is not None:
                extra["output_len"] = len(str(output))   # 원문 아님 — 길이만
        except Exception:
            pass
        self._emit(
            "action",
            action=f"agent.end:{_name_of(agent)}",
            status="ok",
            latency_ms=self._elapsed_ms(f"agent:{id(agent)}"),
            **extra,
        )

    @_isolate
    def _tool_start(self, tool: Any) -> None:
        self._begin(f"tool:{id(tool)}")

    @_isolate
    def _tool_end(self, tool: Any, result: Any) -> None:
        extra: dict[str, Any] = {}
        try:
            if result is not None:
                extra["result_len"] = len(str(result))   # 원문 아님 — 길이만
        except Exception:
            pass
        self._emit(
            "tool_call",
            tool=_name_of(tool),
            status="ok",
            latency_ms=self._elapsed_ms(f"tool:{id(tool)}"),
            **extra,
        )

    @_isolate
    def _llm_start(self, agent: Any) -> None:
        self._begin(f"llm:{id(agent)}")

    @_isolate
    def _llm_end(self, agent: Any, response: Any) -> None:
        model = None
        try:
            model = getattr(agent, "model", None)
            model = str(model) if model else None
        except Exception:
            model = None
        self._emit(
            "llm_call",
            model=model,
            provider="openai",
            latency_ms=self._elapsed_ms(f"llm:{id(agent)}"),
            **_usage_of(response),
        )

    @_isolate
    def _handoff(self, from_agent: Any, to_agent: Any) -> None:
        self._emit(
            "action",
            action=f"handoff:{_name_of(from_agent)}->{_name_of(to_agent)}",
            status="ok",
        )


class XsoftComplyAgentHooks(_HookBase):
    """`agent.hooks` 자리에 넣는 훅 (AgentHooks 시그니처 · 전부 async)."""

    async def on_start(self, context: Any, agent: Any) -> None:
        self._agent_start(agent)

    async def on_end(self, context: Any, agent: Any, output: Any) -> None:
        self._agent_end(agent, output)

    async def on_handoff(self, context: Any, agent: Any, source: Any) -> None:
        self._handoff(source, agent)

    async def on_tool_start(self, context: Any, agent: Any, tool: Any) -> None:
        self._tool_start(tool)

    async def on_tool_end(self, context: Any, agent: Any, tool: Any, result: Any) -> None:
        self._tool_end(tool, result)

    async def on_llm_start(self, context: Any, agent: Any, system_prompt: Any = None,
                           input_items: Any = None) -> None:
        self._llm_start(agent)

    async def on_llm_end(self, context: Any, agent: Any, response: Any) -> None:
        self._llm_end(agent, response)


class XsoftComplyRunHooks(_HookBase):
    """`Runner.run(..., hooks=)` 자리에 넣는 훅 (RunHooks 시그니처 · 전부 async).

    ★ 중복 기록 방지
      `instrument(agent)` 로 AgentHooks 를 이미 붙였는데 run hooks 까지 넘기면
      **같은 이벤트가 두 번** 기록될 수 있다(감사기록이 부풀고 사용량도 두 배가 된다).
      그래서 실행훅은 «그 에이전트가 이미 SDK 훅을 등록해 두었으면» 자기 몫을 건너뛴다.
      작업 인계는 받는 쪽(to_agent) 기준으로 판단한다.
    """

    @staticmethod
    def _covered(agent: Any) -> bool:
        try:
            return isinstance(getattr(agent, "hooks", None), XsoftComplyAgentHooks)
        except Exception:
            return False

    async def on_agent_start(self, context: Any, agent: Any) -> None:
        if not self._covered(agent):
            self._agent_start(agent)

    async def on_agent_end(self, context: Any, agent: Any, output: Any) -> None:
        if not self._covered(agent):
            self._agent_end(agent, output)

    async def on_handoff(self, context: Any, from_agent: Any, to_agent: Any) -> None:
        if not self._covered(to_agent):
            self._handoff(from_agent, to_agent)

    async def on_tool_start(self, context: Any, agent: Any, tool: Any) -> None:
        if not self._covered(agent):
            self._tool_start(tool)

    async def on_tool_end(self, context: Any, agent: Any, tool: Any, result: Any) -> None:
        if not self._covered(agent):
            self._tool_end(tool, result)

    async def on_llm_start(self, context: Any, agent: Any, system_prompt: Any = None,
                           input_items: Any = None) -> None:
        if not self._covered(agent):
            self._llm_start(agent)

    async def on_llm_end(self, context: Any, agent: Any, response: Any) -> None:
        if not self._covered(agent):
            self._llm_end(agent, response)


# 옛 이름 호환 (이전 판에서 공개했던 이름)
XsoftComplyAgentHook = XsoftComplyAgentHooks


def _bound(mixin: type, base_attr: str) -> type:
    """agents 패키지가 있으면 그쪽 기반 클래스를 상속한 서브클래스를 만들어 준다.

    ★ 왜 필요한가
      openai-agents 0.21 은 `Runner.run(hooks=...)` 에서 **isinstance(RunHooks) 를 강제**한다
      (`validate_run_hooks` → TypeError). 이름만 맞춘 순수 클래스로는 실행이 거부된다.
    ★ 그래도 의존성 0 은 유지된다 = 패키지가 없으면 순수 클래스를 그대로 쓴다.
    ★ `RunHooks` 는 제네릭 별칭(`RunHooksBase[TContext, Agent]`)이라 `type(...)` 으로는 못 만든다
      («MRO entry resolution» TypeError). `types.new_class` 를 써야 한다.
    """
    try:
        import types as _types

        from agents import lifecycle  # type: ignore[import-not-found]
        base = getattr(lifecycle, base_attr)
        return _types.new_class(f"{mixin.__name__}Bound", (mixin, base), {})
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        return mixin


def run_hooks() -> Any:
    """`Runner.run(agent, input, hooks=xc.agents_run_hooks())` 로 넘길 훅."""
    return _bound(XsoftComplyRunHooks, "RunHooks")()


def agent_hooks() -> Any:
    """`agent.hooks` 에 넣을 훅."""
    return _bound(XsoftComplyAgentHooks, "AgentHooks")()


def wrap_agent(agent: Any) -> Any:
    """에이전트 객체에 AgentHooks 를 주입한다.

    이미 hooks 가 있으면 덮어쓰지 않는다(고객이 붙인 훅을 잃지 않게).
    """
    try:
        if getattr(agent, "hooks", None) is None:
            try:
                agent.hooks = agent_hooks()
            except (AttributeError, TypeError, ValueError):
                pass
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException:
        pass
    return agent
