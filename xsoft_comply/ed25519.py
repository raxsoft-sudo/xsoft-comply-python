"""Ed25519 순수 파이썬 구현 (RFC 8032) — 의존성 0.

★ 왜 직접 구현하는가
  SDK 는 «표준 라이브러리만» 이 원칙이다. 고객 프로세스에 pynacl·cryptography 를
  요구하면 설치 실패가 곧 고객 장애가 된다. 서명은 선택 기능이지 설치 조건이 아니다.

★ 성능 = 확장좌표(extended coordinates) 사용.
  서명은 **호출 스레드가 아니라 백그라운드 전송 작업에서** 수행한다.

★ 다른 Ed25519 구현과 상호 검증이 가능하다(RFC 8032 표준 준수).
"""
from __future__ import annotations

import hashlib

# ── 곡선 상수 (RFC 8032 §5.1) ──────────────────────────────────────────────────
Q = 2**255 - 19
L = 2**252 + 27742317777372353535851937790883648493


def _inv(x: int) -> int:
    return pow(x, Q - 2, Q)


D = (-121665 * _inv(121666)) % Q
I = pow(2, (Q - 1) // 4, Q)


def _xrecover(y: int) -> int:
    xx = (y * y - 1) * _inv(D * y * y + 1)
    x = pow(xx, (Q + 3) // 8, Q)
    if (x * x - xx) % Q != 0:
        x = (x * I) % Q
    if x % 2 != 0:
        x = Q - x
    return x


_BY = (4 * _inv(5)) % Q
_BX = _xrecover(_BY)
# 확장좌표 (X, Y, Z, T) · T = X*Y/Z
B = (_BX % Q, _BY % Q, 1, (_BX * _BY) % Q)
IDENT = (0, 1, 1, 0)


def _add(p: tuple, q: tuple) -> tuple:
    """확장좌표 통합 덧셈 공식 (add-2008-hwcd-3). 두 배(P==Q)에도 그대로 쓴다."""
    x1, y1, z1, t1 = p
    x2, y2, z2, t2 = q
    a = (y1 - x1) * (y2 - x2) % Q
    b = (y1 + x1) * (y2 + x2) % Q
    c = t1 * 2 * D * t2 % Q
    dd = z1 * 2 * z2 % Q
    e = b - a
    f = dd - c
    g = dd + c
    h = b + a
    return (e * f % Q, g * h % Q, f * g % Q, e * h % Q)


def _scalarmult(p: tuple, e: int) -> tuple:
    result = IDENT
    while e > 0:
        if e & 1:
            result = _add(result, p)
        p = _add(p, p)
        e >>= 1
    return result


def _affine(p: tuple) -> tuple[int, int]:
    x, y, z, _t = p
    zi = _inv(z)
    return (x * zi % Q, y * zi % Q)


def _encode_point(p: tuple) -> bytes:
    x, y = _affine(p)
    return (y | ((x & 1) << 255)).to_bytes(32, "little")


def _decode_point(data: bytes) -> tuple | None:
    if len(data) != 32:
        return None
    value = int.from_bytes(data, "little")
    y = value & ((1 << 255) - 1)
    sign = value >> 255
    if y >= Q:
        return None
    x = _xrecover(y)
    if x & 1 != sign:
        x = Q - x
    # 곡선 위에 있는지 확인 : -x² + y² = 1 + d·x²·y²
    if (-x * x + y * y - 1 - D * x * x % Q * (y * y) % Q) % Q != 0:
        return None
    return (x, y, 1, x * y % Q)


def _clamp(h32: bytes) -> int:
    a = bytearray(h32)
    a[0] &= 248
    a[31] &= 127
    a[31] |= 64
    return int.from_bytes(a, "little")


# ── 공개 API ──────────────────────────────────────────────────────────────────

def public_key(seed: bytes) -> bytes:
    """32바이트 시드(개인키) → 32바이트 공개키."""
    if len(seed) != 32:
        raise ValueError("seed 는 32바이트여야 합니다.")
    h = hashlib.sha512(seed).digest()
    return _encode_point(_scalarmult(B, _clamp(h[:32])))


def sign(message: bytes, seed: bytes, pub: bytes | None = None) -> bytes:
    """64바이트 서명(R‖S)."""
    if len(seed) != 32:
        raise ValueError("seed 는 32바이트여야 합니다.")
    h = hashlib.sha512(seed).digest()
    a = _clamp(h[:32])
    pk = pub if pub is not None else _encode_point(_scalarmult(B, a))
    r = int.from_bytes(hashlib.sha512(h[32:] + message).digest(), "little") % L
    rr = _encode_point(_scalarmult(B, r))
    k = int.from_bytes(hashlib.sha512(rr + pk + message).digest(), "little") % L
    s = (r + k * a) % L
    return rr + s.to_bytes(32, "little")


def verify(signature: bytes, message: bytes, pub: bytes) -> bool:
    """서명 검증. 형식 오류·불일치 모두 False 를 돌려준다(예외 없음)."""
    try:
        if len(signature) != 64 or len(pub) != 32:
            return False
        rr = signature[:32]
        s = int.from_bytes(signature[32:], "little")
        if s >= L:
            return False
        point_a = _decode_point(pub)
        point_r = _decode_point(rr)
        if point_a is None or point_r is None:
            return False
        k = int.from_bytes(hashlib.sha512(rr + pub + message).digest(), "little") % L
        left = _scalarmult(B, s)
        right = _add(point_r, _scalarmult(point_a, k))
        return _affine(left) == _affine(right)
    except Exception:
        return False
