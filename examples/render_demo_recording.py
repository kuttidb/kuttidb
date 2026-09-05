#!/usr/bin/env python3
"""Render a real demo recording to a README GIF and the static page transcript.

Maintainer-only dependency: Pillow. Running the demo needs no extra packages.
See docs/guides/SAAS_DEMO.md.
"""
import argparse
import html
import json
from pathlib import Path
import re
from PIL import Image, ImageDraw, ImageFont

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--font', help='path to a monospace TrueType font')
args = parser.parse_args()
fonts = [args.font, '/System/Library/Fonts/Menlo.ttc',
         '/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf']
font_path = next((p for p in fonts if p and Path(p).is_file()), None)
if not font_path:
    parser.error('Supply a monospace TrueType font with --font')
font = ImageFont.truetype(font_path, 18)
small = ImageFont.truetype(font_path, 15)
recording = json.loads((root / 'landing/demo-recording.json').read_text())
events = recording['events']
if not any(e['text'].startswith('PASS ') for e in events):
    raise SystemExit('Only render a successful, verified demo run')
width = max(900, int(max(font.getlength(e['text']) for e in events)) + 64)
height = 118 + len(events) * 29
frames, durations = [], []
for count in range(1, len(events) + 1):
    canvas = Image.new('RGB', (width, height), '#0d0a07')
    draw = ImageDraw.Draw(canvas)
    draw.rectangle((0, 0, width, 51), fill='#221a13')
    for x, color in [(23, '#e5566d'), (45, '#e2b542'), (67, '#57c26b')]:
        draw.ellipse((x, 20, x + 10, 30), fill=color)
    draw.text((102, 15), 'KuttiDB / real recorded run', font=font, fill='#f4ecdc')
    for i, event in enumerate(events[:count]):
        color = '#8fd18a' if event['text'].startswith(('PASS ', 'REPLAY ', 'QUEUE ')) else '#d9cdb4'
        if event['text'].startswith('CRASH '):
            color = '#f5a524'
        draw.text((26, 70 + i * 29), event['text'], font=font, fill=color)
    draw.text((26, height - 29),
              'Recorded ' + recording['recorded_at'][:10] + ' | ' + recording['platform'] + ' | RSS sample, not peak',
              font=small, fill='#b4a58c')
    frames.append(canvas)
    durations.append(max(100, round((events[count]['at'] - events[count - 1]['at']) * 1000))
                     if count < len(events) else 5000)
# Play once, leaving the successful result visible. Replay is available on site.
frames[0].save(root / 'landing/demo.gif', save_all=True, append_images=frames[1:],
               duration=durations, optimize=True)
page = root / 'landing/index.html'
text = page.read_text()
for marker, content in [
    ('transcript', '\n'.join(event['text'] for event in events)),
    ('caption', 'Recorded ' + recording['recorded_at'][:10] + ' · ' + recording['platform'] +
     ' · cache durability: always · one event-loop thread.'),
]:
    pattern = rf'(?<=<!-- demo-{marker}:start -->).*?(?=<!-- demo-{marker}:end -->)'
    text, matches = re.subn(pattern, lambda m: html.escape(content), text, flags=re.S)
    if matches != 1:
        raise SystemExit(f'Expected exactly one {marker} marker in the landing page')
page.write_text(text)
print('Rendered landing/demo.gif and refreshed the static recording transcript')
