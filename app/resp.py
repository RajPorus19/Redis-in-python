"""RESP protocol parsing and encoding utilities."""


def _find_crlf(data: bytearray, start: int) -> int:
    idx = data.find(b"\r\n", start)
    return idx


def _parse_int_line(data: bytearray, start: int):
    end = _find_crlf(data, start)
    if end == -1:
        return None
    try:
        value = int(data[start:end])
    except ValueError:
        return None
    return value, end + 2


def _parse_simple_string(data: bytearray, start: int):
    # +<string>\r\n
    end = _find_crlf(data, start)
    if end == -1:
        return None
    return bytes(data[start:end]), end + 2


def _parse_bulk_string(data: bytearray, start: int):
    # $<len>\r\n<bytes>\r\n
    res = _parse_int_line(data, start)
    if res is None:
        return None
    length, idx = res
    if length == -1:
        return None, idx  # Null bulk string
    end = idx + length
    if len(data) < end + 2:
        return None
    if data[end : end + 2] != b"\r\n":
        return None
    return bytes(data[idx:end]), end + 2


def _parse_integer(data: bytearray, start: int):
    return _parse_int_line(data, start)


def _parse_frame_at(data: bytearray, start: int):
    if start >= len(data):
        return None
    prefix = data[start : start + 1]
    if not prefix:
        return None
    p = prefix[0]
    idx = start + 1
    if p == ord(b"+"):
        res = _parse_simple_string(data, idx)
        if res is None:
            return None
        value, next_idx = res
        return value, next_idx
    if p == ord(b"-"):
        # Parse error string as bytes
        res = _parse_simple_string(data, idx)
        if res is None:
            return None
        value, next_idx = res
        return value, next_idx
    if p == ord(b":"):
        res = _parse_integer(data, idx)
        if res is None:
            return None
        value, next_idx = res
        return value, next_idx
    if p == ord(b"$"):
        res = _parse_bulk_string(data, idx)
        if res is None:
            return None
        value, next_idx = res
        return value, next_idx
    if p == ord(b"*"):
        res = _parse_int_line(data, idx)
        if res is None:
            return None
        count, idx = res
        if count == -1:
            return None, idx  # Null array
        elements = []
        for _ in range(count):
            sub = _parse_frame_at(data, idx)
            if sub is None:
                return None
            element, idx = sub
            elements.append(element)
        return elements, idx
    # Unsupported/unknown
    return None


def _encode_simple_string(s: bytes) -> bytes:
    return b"+" + s + b"\r\n"


def _encode_bulk_string(b: bytes | None) -> bytes:
    if b is None:
        return b"$-1\r\n"
    return b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"


def _encode_array(elements: list[bytes]) -> bytes:
    # RESP arrays: *<len>\r\n then each element as bulk string
    count = len(elements)
    if count == 0:
        return b"*0\r\n"
    parts = [b"*" + str(count).encode() + b"\r\n"]
    for e in elements:
        parts.append(_encode_bulk_string(e))
    return b"".join(parts)


def _encode_integer(n: int) -> bytes:
    return b":" + str(n).encode() + b"\r\n"


def _encode_null_array() -> bytes:
    return b"*-1\r\n"


def _consume_next_frame(buffer: bytearray):
    """Parse and consume a single RESP frame from the buffer if available.

    Returns the parsed frame, or None if the buffer does not yet contain a full frame.
    """
    parsed = _parse_frame_at(buffer, 0)
    if parsed is None:
        return None
    frame, next_idx = parsed
    if next_idx > 0:
        del buffer[:next_idx]
    return frame

