"""Check the current public chart before replacing a Tracklane homepage."""

import datetime as dt
import html
import json
import re


def chart(page):
    panel = re.search(r'<p class="hero-foot"[^>]*>[^<]*? shoes &middot; ([^<]*?) &middot;', page)
    series = re.search(r'^const (DATA|DAILY) = ', page, re.MULTILINE)
    if not panel or not series:
        raise ValueError("Cannot identify the homepage panel or chart")
    payload, _ = json.JSONDecoder().raw_decode(page[series.end():])
    rows = payload["daily"] if series[1] == "DATA" else payload
    previous = None
    for row in rows:
        date = dt.date.fromisoformat(row[0])
        if len(row) != 5 or (previous and date <= previous):
            raise ValueError("Invalid chart rows or date order")
        previous = date
    if not previous:
        raise ValueError("Empty homepage chart")
    return html.unescape(panel[1]), rows


def check_update(published, candidate, today):
    old_panel, old_rows = chart(published)
    new_panel, new_rows = chart(candidate)
    if new_panel != old_panel:
        raise ValueError("The headline retailer list changed")
    if new_rows[:len(old_rows)] != old_rows:
        raise ValueError("Previously published chart history changed")
    if new_rows[-1][0] != today.isoformat():
        raise ValueError("The latest chart observation is not today's date")
