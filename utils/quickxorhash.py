from __future__ import annotations
from typing import Iterable
import base64

BITS_IN_LAST_CELL = 32
SHIFT = 11
WIDTH_IN_BITS = 160
SIZE = (WIDTH_IN_BITS - 1) // 8 + 1
_U64_MASK = (1 << 64) - 1
_LAST_CELL_MASK = (1 << BITS_IN_LAST_CELL) - 1


class QuickXorHash:
    def __init__(self) -> None:
        self.data = [0, 0, 0]
        self.length_so_far = 0
        self.shift_so_far = 0

    def write(self, buf: bytes) -> int:
        if not buf:
            return 0

        current_shift = self.shift_so_far
        vector_array_index = current_shift // 64
        vector_offset = current_shift % 64

        iterations = min(len(buf), WIDTH_IN_BITS)

        for i in range(iterations):
            is_last_cell = (vector_array_index == len(self.data) - 1)
            bits_in_vector_cell = BITS_IN_LAST_CELL if is_last_cell else 64

            if vector_offset <= bits_in_vector_cell - 8:
                j = i
                while j < len(buf):
                    self.data[vector_array_index] ^= (buf[j] & 0xFF) << vector_offset
                    self.data[vector_array_index] &= _U64_MASK
                    j += WIDTH_IN_BITS
            else:
                index1 = vector_array_index
                index2 = 0 if is_last_cell else vector_array_index + 1
                low = (bits_in_vector_cell - vector_offset)
                xored_byte = 0
                j = i
                while j < len(buf):
                    xored_byte ^= (buf[j] & 0xFF)
                    j += WIDTH_IN_BITS
                self.data[index1] ^= (xored_byte & 0xFF) << vector_offset
                self.data[index1] &= _U64_MASK
                self.data[index2] ^= (xored_byte & 0xFF) >> low
                self.data[index2] &= _U64_MASK

            vector_offset += SHIFT
            while True:
                if vector_offset < bits_in_vector_cell:
                    break
                if is_last_cell:
                    vector_array_index = 0
                else:
                    vector_array_index = vector_array_index + 1
                vector_offset -= bits_in_vector_cell
                is_last_cell = (vector_array_index == len(self.data) - 1)
                bits_in_vector_cell = BITS_IN_LAST_CELL if is_last_cell else 64

        self.shift_so_far = (self.shift_so_far + SHIFT * (len(buf) % WIDTH_IN_BITS)) % WIDTH_IN_BITS
        self.length_so_far = (self.length_so_far + len(buf)) & ((1 << 64) - 1)

        return len(buf)

    def digest(self) -> bytes:
        rgb = bytearray(SIZE)

        for i in range(len(self.data) - 1):
            rgb[i * 8: i * 8 + 8] = int(self.data[i] & _U64_MASK).to_bytes(8, byteorder="little")

        last_bytes = int(self.data[-1] & _U64_MASK).to_bytes(8, byteorder="little")
        rgb[(len(self.data) - 1) * 8:] = last_bytes[: (SIZE - (len(self.data) - 1) * 8)]

        length_bytes = int(self.length_so_far & ((1 << 64) - 1)).to_bytes(8, byteorder="little")
        start = WIDTH_IN_BITS // 8 - len(length_bytes)  # 20 - 8 = 12
        for i in range(8):
            rgb[start + i] ^= length_bytes[i]

        return bytes(rgb)

    def hexdigest(self) -> str:
        return self.digest().hex()

    def base64(self) -> str:
        return base64.b64encode(self.digest()).decode("ascii")
    
def quickxorhash_file_base64(path: str, chunk_size: int = 64 * 1024) -> str:
    h = QuickXorHash()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.write(chunk)
    return h.base64()