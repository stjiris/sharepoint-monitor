from __future__ import annotations
from typing import Iterable
import base64

BITS_IN_LAST_CELL = 32
SHIFT = 11
WIDTH_IN_BITS = 160
SIZE = (WIDTH_IN_BITS - 1) // 8 + 1
_U64_MASK = (1 << 64) - 1


def quickxorhash_file_base64(path: str, chunk_size: int = 64 * 1024) -> str:
    data = [0, 0, 0]
    length_so_far = 0
    shift_so_far = 0
    
    def process_buffer(buf: bytes) -> None:
        nonlocal shift_so_far, length_so_far
        if not buf:
            return
        
        current_shift = shift_so_far
        vector_array_index = current_shift // 64
        vector_offset = current_shift % 64
        
        iterations = min(len(buf), WIDTH_IN_BITS)
        
        data_len = len(data)
        last_cell_index = data_len - 1
        
        for i in range(iterations):
            is_last_cell = (vector_array_index == last_cell_index)
            bits_in_vector_cell = BITS_IN_LAST_CELL if is_last_cell else 64
            
            if vector_offset <= bits_in_vector_cell - 8:
                j = i
                while j < len(buf):
                    data[vector_array_index] ^= (buf[j] & 0xFF) << vector_offset
                    data[vector_array_index] &= _U64_MASK
                    j += WIDTH_IN_BITS
            else:
                index1 = vector_array_index
                index2 = 0 if is_last_cell else vector_array_index + 1
                low = bits_in_vector_cell - vector_offset
                xored_byte = 0
                
                j = i
                while j < len(buf):
                    xored_byte ^= (buf[j] & 0xFF)
                    j += WIDTH_IN_BITS
                
                data[index1] ^= (xored_byte & 0xFF) << vector_offset
                data[index1] &= _U64_MASK
                data[index2] ^= (xored_byte & 0xFF) >> low
                data[index2] &= _U64_MASK

            vector_offset += SHIFT
            while vector_offset >= bits_in_vector_cell:
                if is_last_cell:
                    vector_array_index = 0
                else:
                    vector_array_index += 1
                vector_offset -= bits_in_vector_cell
                is_last_cell = (vector_array_index == last_cell_index)
                bits_in_vector_cell = BITS_IN_LAST_CELL if is_last_cell else 64
        
        shift_so_far = (shift_so_far + SHIFT * (len(buf) % WIDTH_IN_BITS)) % WIDTH_IN_BITS
        length_so_far = (length_so_far + len(buf)) & _U64_MASK
    
    def compute_digest() -> bytes:
        rgb = bytearray(SIZE)
        
        for i in range(len(data) - 1):
            val = data[i] & _U64_MASK
            rgb[i*8:(i+1)*8] = val.to_bytes(8, 'little')
        
        last_val = data[-1] & _U64_MASK
        last_bytes = last_val.to_bytes(8, 'little')
        start_idx = (len(data) - 1) * 8
        rgb[start_idx:] = last_bytes[:SIZE - start_idx]
        
        length_bytes = (length_so_far & _U64_MASK).to_bytes(8, 'little')
        start = WIDTH_IN_BITS // 8 - len(length_bytes)  # 20 - 8 = 12
        for i in range(8):
            rgb[start + i] ^= length_bytes[i]
        
        return bytes(rgb)
    
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            process_buffer(chunk)
    
    digest = compute_digest()
    return base64.b64encode(digest).decode("ascii")