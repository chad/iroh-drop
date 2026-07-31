#!/usr/bin/env python3
"""Render the app icon set. No dependencies: PNGs are written by hand.

Each size is rendered from vector maths with supersampling rather than scaled
from one big bitmap, so the 16pt icon in a Finder list is as crisp as the 512pt
one in Quick Look.
"""

import os
import struct
import sys
import zlib

# Rounded-square artwork on a transparent canvas, the way macOS expects.
PADDING = 0.09          # transparent margin on each side
CORNER = 0.235          # corner radius, as a fraction of the square's side
TOP = (0x3D, 0x7B, 0xFF)
BOTTOM = (0x8B, 0x3D, 0xF5)


def lerp(a, b, t):
    return a + (b - a) * t


def inside_round_rect(x, y, x0, y0, x1, y1, r):
    """Point-in-rounded-rectangle."""
    if x < x0 or x > x1 or y < y0 or y > y1:
        return False
    cx = min(max(x, x0 + r), x1 - r)
    cy = min(max(y, y0 + r), y1 - r)
    dx = x - cx
    dy = y - cy
    return dx * dx + dy * dy <= r * r


def inside_triangle(x, y, ax, ay, bx, by, cx, cy):
    d1 = (x - bx) * (ay - by) - (ax - bx) * (y - by)
    d2 = (x - cx) * (by - cy) - (bx - cx) * (y - cy)
    d3 = (x - ax) * (cy - ay) - (cx - ax) * (y - ay)
    neg = d1 < 0 or d2 < 0 or d3 < 0
    pos = d1 > 0 or d2 > 0 or d3 > 0
    return not (neg and pos)


def glyph(u, v):
    """The white mark, in coordinates normalised to the rounded square.

    An arrow coming down to a line: 'files land here'. It has to survive being
    16 pixels wide, which rules out anything with fine detail.
    """
    # Shaft.
    if 0.435 <= u <= 0.565 and 0.215 <= v <= 0.545:
        return True
    # Head.
    if inside_triangle(u, v, 0.285, 0.505, 0.715, 0.505, 0.5, 0.775):
        return True
    # Base line, with rounded ends.
    if inside_round_rect(u, v, 0.245, 0.825, 0.755, 0.90, 0.0375):
        return True
    return False


def render(size, samples):
    n = size * samples
    pad = PADDING * n
    x0, y0, x1, y1 = pad, pad, n - pad, n - pad
    side = x1 - x0
    radius = CORNER * side
    inv = 1.0 / (samples * samples)

    rows = []
    for py in range(size):
        row = bytearray()
        for px in range(size):
            covered = 0
            r_sum = g_sum = b_sum = 0.0
            for sy in range(samples):
                y = (py * samples + sy) + 0.5
                for sx in range(samples):
                    x = (px * samples + sx) + 0.5
                    if not inside_round_rect(x, y, x0, y0, x1, y1, radius):
                        continue
                    covered += 1
                    u = (x - x0) / side
                    v = (y - y0) / side
                    if glyph(u, v):
                        r_sum += 255.0
                        g_sum += 255.0
                        b_sum += 255.0
                    else:
                        # Diagonal gradient reads better than vertical at small
                        # sizes: it keeps the top-left corner light.
                        t = min(max((u + v) * 0.5, 0.0), 1.0)
                        r_sum += lerp(TOP[0], BOTTOM[0], t)
                        g_sum += lerp(TOP[1], BOTTOM[1], t)
                        b_sum += lerp(TOP[2], BOTTOM[2], t)
            if covered == 0:
                row += b"\x00\x00\x00\x00"
            else:
                # Straight (non-premultiplied) alpha: average the colour of the
                # covered subpixels only.
                row += bytes(
                    (
                        int(r_sum / covered + 0.5),
                        int(g_sum / covered + 0.5),
                        int(b_sum / covered + 0.5),
                        int(covered * inv * 255 + 0.5),
                    )
                )
        rows.append(bytes(row))
    return rows


def write_png(path, size, rows):
    raw = b"".join(b"\x00" + row for row in rows)

    def chunk(kind, data):
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        )

    header = struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0)
    with open(path, "wb") as out:
        out.write(b"\x89PNG\r\n\x1a\n")
        out.write(chunk(b"IHDR", header))
        out.write(chunk(b"IDAT", zlib.compress(raw, 9)))
        out.write(chunk(b"IEND", b""))


def main():
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)

    # (pixel size, iconset file name). macOS wants both @1x and @2x of each.
    wanted = [
        (16, "icon_16x16.png"),
        (32, "icon_16x16@2x.png"),
        (32, "icon_32x32.png"),
        (64, "icon_32x32@2x.png"),
        (128, "icon_128x128.png"),
        (256, "icon_128x128@2x.png"),
        (256, "icon_256x256.png"),
        (512, "icon_256x256@2x.png"),
        (512, "icon_512x512.png"),
        (1024, "icon_512x512@2x.png"),
    ]

    cache = {}
    for size, name in wanted:
        if size not in cache:
            # More samples where aliasing is most visible.
            samples = 6 if size <= 64 else (4 if size <= 256 else 2)
            cache[size] = render(size, samples)
            print(f"  rendered {size}x{size}", flush=True)
        write_png(os.path.join(out_dir, name), size, cache[size])


if __name__ == "__main__":
    main()
