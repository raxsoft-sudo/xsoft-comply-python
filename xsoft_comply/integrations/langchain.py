"""LangChain 연동 처리기 — 유형에 의존하지 않는 구현.

langchain 미설치여도 import 실패하지 않도록 지연 import 한다.
기반 클래스가 없으면 LangChain 의존성 없이 사용할 수 있는 기본 Python 객체로 동작한다.

사용:
    handler = xc.langchain_handler()
    llm = ChatOpenAI(callbacks=[handler])

설계 노트:
  - 클래스 정의를 make_handler() 팩토리 안에서 지연 생성한다.
    이를 통해 langchain이 없어도 모듈 import가 성공한다.
  - 스레드 로컬 현재 세션(session.get_current())을 사용한다.
    세션이 없으면 이벤트를 기록하지 않는다.
  - 원문(prompts/outputs)은 전송하지 않는다.
"""
from __future__ import annotations

import time
from typing import Any


def _get_lc_base() -> type:
    """지연 import. langchain 없으면 object를 반환한다."""
    try:
        from langchain_core.callbacks.base import BaseCallbackHandler
        return BaseCallbackHandler  # type: ignore[return-value]
    except ImportError:
        pass
    try:
        from langchain.callbacks.base import BaseCallbackHandler  # type: ignore[no-redef]
        return BaseCallbackHandler  # type: ignore[return-value]
    except ImportError:
        pass
    return object


def make_handler() -> type:
    """XsoftComplyCallbackHandler 클래스를 동적으로 생성해 반환한다.

    langchain 기반 클래스가 있으면 상속하고, 없으면 기본 Python 객체로 동작한다.
    """
    _Base = _get_lc_base()

    class XsoftComplyCallbackHandler(_Base):  # type: ignore[valid-type,misc]
        """LangChain 이벤트를 xsoft_comply 이벤트로 변환한다.

        원문(prompt/response)은 전송하지 않는다.
        tokens_in/tokens_out/latency_ms 등 구조화 메타만 기록한다.
        """

        def __init__(self) -> None:
            if _Base is not object:
                super().__init__()
            self._llm_start_time: float = 0.0
            self._tool_start_time: float = 0.0

        def _current_session(self) -> Any:
            """스레드 로컬 현재 세션. 없으면 None."""
            try:
                from ..session import get_current
                return get_current()
            except Exception:
                return None

        # ── LLM 이벤트 ───────────────────────────────────────────────────────

        def on_llm_start(
            self,
            serialized: dict[str, Any],
            prompts: list[str],
            **kwargs: Any,
        ) -> None:
            """LLM 호출 시작 — 시작 시각 기록. 원문(prompts) 미전송.

            ★ S-12 = time.monotonic() 자체가 실패해도(시계 장애) 프레임워크로 전달하지 않는다.
            """
            try:
                self._llm_start_time = time.monotonic()
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                pass

        def on_llm_end(self, response: Any, **kwargs: Any) -> None:
            """LLM 호출 완료 — llm_call 이벤트 발행."""
            try:
                latency_ms = (time.monotonic() - self._llm_start_time) * 1000
                sess = self._current_session()
                if sess is None:
                    return
                # 모델명 추출 시도
                model: str | None = None
                try:
                    model = response.llm_output.get("model_name")  # type: ignore[union-attr]
                except Exception:
                    pass
                # 토큰 수 추출 시도
                tokens_in: int | None = None
                tokens_out: int | None = None
                try:
                    usage = response.llm_output.get("token_usage", {})  # type: ignore[union-attr]
                    tokens_in = usage.get("prompt_tokens")
                    tokens_out = usage.get("completion_tokens")
                except Exception:
                    pass
                # 원문은 절대 미전송 — content 파라미터 미사용
                sess.llm_call(
                    model=model,
                    tokens_in=tokens_in,
                    tokens_out=tokens_out,
                    latency_ms=latency_ms,
                    provider="langchain",
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                pass

        def on_llm_error(self, error: BaseException, **kwargs: Any) -> None:
            """LLM 에러 — llm_call(status=error) 기록."""
            try:
                latency_ms = (time.monotonic() - self._llm_start_time) * 1000
                sess = self._current_session()
                if sess is None:
                    return
                sess.llm_call(latency_ms=latency_ms, provider="langchain", status="error")
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                pass

        # ── 도구 이벤트 ──────────────────────────────────────────────────────

        def on_tool_start(
            self,
            serialized: dict[str, Any],
            input_str: str,
            **kwargs: Any,
        ) -> None:
            """도구 호출 시작 — 시작 시각 기록. input_str 원문 미전송.

            ★ S-12 = time.monotonic() 자체가 실패해도(시계 장애) 프레임워크로 전달하지 않는다.
            """
            try:
                self._tool_start_time = time.monotonic()
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                pass

        def on_tool_end(self, output: str, **kwargs: Any) -> None:
            """도구 호출 완료 — tool_call 이벤트 발행. output 원문 미전송."""
            try:
                latency_ms = (time.monotonic() - self._tool_start_time) * 1000
                sess = self._current_session()
                if sess is None:
                    return
                tool_name: str | None = kwargs.get("name")
                sess.tool_call(tool=tool_name, status="ok", latency_ms=latency_ms)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                pass

        def on_tool_error(self, error: BaseException, **kwargs: Any) -> None:
            """도구 에러 — tool_call(status=error) 기록."""
            try:
                latency_ms = (time.monotonic() - self._tool_start_time) * 1000
                sess = self._current_session()
                if sess is None:
                    return
                tool_name: str | None = kwargs.get("name")
                sess.tool_call(tool=tool_name, status="error", latency_ms=latency_ms)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                pass

        # ── 체인 이벤트 (no-op — 구조 메타 없음) ─────────────────────────────

        def on_chain_start(self, serialized: dict, inputs: dict, **kwargs: Any) -> None:
            pass

        def on_chain_end(self, outputs: dict, **kwargs: Any) -> None:
            pass

        def on_chain_error(self, error: BaseException, **kwargs: Any) -> None:
            pass

    return XsoftComplyCallbackHandler


def _make_xsoft_handler_class() -> type:
    """XsoftCallbackHandler 클래스를 동적으로 생성한다.

    langchain_core 가 있으면 BaseCallbackHandler 를 상속해 `raise_error` 등
    필수 속성을 자동으로 얻는다. 없으면 기본 Python 객체로 동작한다.
    """
    _Base = _get_lc_base()

    class _XsoftCallbackHandler(_Base):  # type: ignore[valid-type,misc]
        """xsoft_comply LangChain 연동 처리기 — 인스턴스 기반 API.

        `XsoftCallbackHandler(agent_id=...)` 로 직접 생성한다.
        체인 호출마다 새 Session(trace)을 시작하고, on_chain_end 에서 닫는다.

        사용:
            handler = XsoftCallbackHandler(agent_id="my-bot")
            chain.invoke({...}, config={"callbacks": [handler]})
            handler.close()
            xc.flush(timeout=10.0)
        """

        def __init__(self, agent_id: str | None = None) -> None:
            if _Base is not object:
                super().__init__()
            self.agent_id = agent_id
            self.trace_ids: set[str] = set()
            self._nesting = 0
            self._llm_start_time: float = 0.0
            self._tool_start_time: float = 0.0
            self._impl: object | None = None  # 현재 활성 Session

    return _XsoftCallbackHandler


# Module-level class: created lazily on first use
_XsoftHandlerClass: type | None = None


def _get_xsoft_handler_class() -> type:
    global _XsoftHandlerClass
    if _XsoftHandlerClass is None:
        _XsoftHandlerClass = _make_xsoft_handler_class()
    return _XsoftHandlerClass


class _XsoftCallbackHandlerProxy:
    """프록시: XsoftCallbackHandler(...) 호출 시 실제 클래스 인스턴스를 반환한다."""

    def __new__(cls, agent_id: str | None = None) -> Any:
        HandlerClass = _get_xsoft_handler_class()
        instance = HandlerClass.__new__(HandlerClass)
        instance.__init__(agent_id=agent_id)
        return instance


# Public name: test imports `from xsoft_comply.integrations.langchain import XsoftCallbackHandler`
class XsoftCallbackHandler:
    """xsoft_comply LangChain 연동 처리기 — 인스턴스 기반 API.

    `XsoftCallbackHandler(agent_id=...)` 로 직접 생성한다.
    체인 호출마다 새 Session(trace)을 시작하고, on_chain_end 에서 닫는다.

    사용:
        handler = XsoftCallbackHandler(agent_id="my-bot")
        chain.invoke({...}, config={"callbacks": [handler]})
        handler.close()
        xc.flush(timeout=10.0)
    """

    def __init__(self, agent_id: str | None = None) -> None:
        self.agent_id = agent_id
        self.trace_ids: set[str] = set()
        self._nesting = 0
        self._llm_start_time: float = 0.0
        self._tool_start_time: float = 0.0
        self._impl: object | None = None  # 현재 활성 Session
        self._prev_token: object | None = None  # M-12 = 시작 전 바깥 세션 복원용 토큰

        # BaseCallbackHandler 호환 속성 (langchain 없는 환경)
        self.raise_error = False
        self.ignore_llm = False
        self.ignore_chain = False
        self.ignore_agent = False
        self.ignore_retriever = False
        self.ignore_chat_model = False

    # ── 내부 헬퍼 ────────────────────────────────────────────────────────────

    def _start_session(self) -> None:
        """새 Session 을 시작하고 thread-local current 로 등록한다.

        ★ M-12(1001-543) = `_set_current()` 가 돌려주는 **토큰**을 저장해 둔다 —
          `_end_session()` 이 끝에서 이 토큰으로 되돌려야 바깥(호출자) 세션이 살아난다.
        """
        try:
            from ..session import Session, new_trace_id, _set_current
            tid = new_trace_id()
            self.trace_ids.add(tid)
            sess = Session(trace_id=tid, agent_id=self.agent_id, product_id=None)
            sess._emit("session_start")
            self._prev_token = _set_current(sess)
            self._impl = sess
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    def _end_session(self) -> None:
        """현재 Session 을 종료하고 **이전 세션으로 복원**한다(M-12).

        ★ 예전에는 끝에서 무조건 `None` 으로 지워, 중첩된(바깥) `xc.session()` 블록
          안에서 이 핸들러를 쓰면 바깥 세션 컨텍스트가 사라졌다. ContextVar 의
          `reset(token)` 으로 복원해야 바깥쪽이 그대로 이어진다.
        """
        try:
            from ..session import _current
            sess = self._impl
            if sess is not None:
                sess._emit("session_end")
            token = self._prev_token
            self._prev_token = None
            if token is not None:
                try:
                    _current.reset(token)
                except (KeyboardInterrupt, SystemExit):
                    raise
                except BaseException:
                    _current.set(None)
            else:
                _current.set(None)
            self._impl = None
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    def _current(self):
        """현재 Session."""
        return self._impl

    # ── Chain 이벤트 ─────────────────────────────────────────────────────────

    def on_chain_start(self, serialized: dict, inputs: dict, **kwargs) -> None:  # type: ignore[override]
        self._nesting += 1
        if self._nesting == 1:
            self._start_session()

    def on_chain_end(self, outputs: dict, **kwargs) -> None:  # type: ignore[override]
        self._nesting -= 1
        if self._nesting <= 0:
            self._nesting = 0
            self._end_session()

    def on_chain_error(self, error: BaseException, **kwargs) -> None:  # type: ignore[override]
        self._nesting -= 1
        if self._nesting <= 0:
            self._nesting = 0
            self._end_session()

    # ── LLM / Chat 이벤트 ────────────────────────────────────────────────────

    def on_chat_model_start(self, serialized: dict, messages: list, **kwargs) -> None:  # type: ignore[override]
        """Chat model 호출 시작 — LangChain 1.x chat model 이벤트. 원문(messages) 미전송.

        ★ S-12 = time.monotonic() 자체가 실패해도(시계 장애) 프레임워크로 전달하지 않는다.
        """
        try:
            import time
            self._llm_start_time = time.monotonic()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    def on_llm_start(self, serialized: dict, prompts: list, **kwargs) -> None:  # type: ignore[override]
        try:
            import time
            self._llm_start_time = time.monotonic()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    def on_llm_end(self, response, **kwargs) -> None:  # type: ignore[override]
        try:
            import time
            latency_ms = (time.monotonic() - self._llm_start_time) * 1000
            sess = self._current()
            if sess is None:
                return
            model: str | None = None
            tokens_in: int | None = None
            tokens_out: int | None = None
            try:
                model = response.llm_output.get("model_name")
            except Exception:
                pass
            try:
                usage = response.llm_output.get("token_usage", {})
                tokens_in = usage.get("prompt_tokens")
                tokens_out = usage.get("completion_tokens")
            except Exception:
                pass
            sess.llm_call(  # type: ignore[attr-defined]
                model=model, tokens_in=tokens_in, tokens_out=tokens_out,
                latency_ms=latency_ms, provider="langchain",
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    def on_llm_error(self, error: BaseException, **kwargs) -> None:  # type: ignore[override]
        try:
            import time
            latency_ms = (time.monotonic() - self._llm_start_time) * 1000
            sess = self._current()
            if sess is None:
                return
            sess.llm_call(latency_ms=latency_ms, provider="langchain", status="error")  # type: ignore[attr-defined]
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    # ── 도구 이벤트 ──────────────────────────────────────────────────────────

    def on_tool_start(self, serialized: dict, input_str: str, **kwargs) -> None:  # type: ignore[override]
        try:
            import time
            self._tool_start_time = time.monotonic()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    def on_tool_end(self, output: str, **kwargs) -> None:  # type: ignore[override]
        try:
            import time
            latency_ms = (time.monotonic() - self._tool_start_time) * 1000
            sess = self._current()
            if sess is None:
                return
            tool_name: str | None = kwargs.get("name")
            sess.tool_call(tool=tool_name, status="ok", latency_ms=latency_ms)  # type: ignore[attr-defined]
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    def on_tool_error(self, error: BaseException, **kwargs) -> None:  # type: ignore[override]
        try:
            import time
            latency_ms = (time.monotonic() - self._tool_start_time) * 1000
            sess = self._current()
            if sess is None:
                return
            tool_name: str | None = kwargs.get("name")
            sess.tool_call(tool=tool_name, status="error", latency_ms=latency_ms)  # type: ignore[attr-defined]
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass

    def close(self) -> None:
        """핸들러 종료 — 열려 있는 세션이 있으면 닫는다."""
        try:
            if self._impl is not None:
                self._end_session()
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException:
            pass
