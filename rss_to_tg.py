import os
import json
import html
import re

import feedparser
import requests

RSS_URL = os.getenv("RSS_URL", "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts")
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
STATE_FILE = os.getenv("STATE_FILE", "state.json")
MAX_SEND = int(os.getenv("MAX_SEND", "20"))
KEEP_SEEN = int(os.getenv("KEEP_SEEN", "1000"))


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"seen": []}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def pick_id(entry):
    return (
        getattr(entry, "id", None)
        or getattr(entry, "guid", None)
        or getattr(entry, "link", None)
        or f"{getattr(entry, 'title', '')}|{getattr(entry, 'published', '')}"
    )


def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()


def clean_html_text(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"<br\\s*/?>", "\n", raw, flags=re.I)
    text = re.sub(r"</p\\s*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def parse_summary_to_dict(summary: str) -> dict:
    text = clean_html_text(summary)
    result = {}

    for line in text.splitlines():
        line = line.strip(" -•\t")
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip().lower()
            value = value.strip()
            if key and value:
                result[key] = value

    return result


def choose(data: dict, *keys):
    for k in keys:
        if k in data and data[k]:
            return data[k]
    return ""


def format_message(entry) -> str:
    title = html.escape(getattr(entry, "title", "Trading Halt"))
    summary = getattr(entry, "summary", "") or getattr(entry, "description", "")
    fields = parse_summary_to_dict(summary)

    symbol = choose(fields, "symbol", "issue symbol")
    market = choose(fields, "market", "listing market")
    halt_code = choose(fields, "reason code", "halt code", "code")
    halt_time = choose(fields, "halt time", "time")
    resume_date = choose(fields, "resumption date", "resume date")
    resume_time = choose(fields, "resumption time", "resume time")
    threshold = choose(fields, "pause threshold price")

    lines = [f"🚨 <b>{title}</b>"]

    if symbol:
        lines.append(f"<b>Symbol</b>: {html.escape(symbol)}")
    if market:
        lines.append(f"<b>Market</b>: {html.escape(market)}")
    if halt_code:
        lines.append(f"<b>Code</b>: {html.escape(halt_code)}")
    if halt_time:
        lines.append(f"<b>Halt Time</b>: {html.escape(halt_time)}")

    resume = " ".join(x for x in [resume_date, resume_time] if x)
    if resume:
        lines.append(f"<b>Resume</b>: {html.escape(resume)}")

    if threshold:
        lines.append(f"<b>Threshold</b>: {html.escape(threshold)}")

    return "\n".join(lines)


def main():
    state = load_state()
    seen = set(state.get("seen", []))

    feed = feedparser.parse(RSS_URL)
    entries = getattr(feed, "entries", []) or []

    new_items = []
    for entry in entries:
        eid = pick_id(entry)
        if not eid or eid in seen:
            continue
        new_items.append((eid, entry))

    new_items = new_items[:MAX_SEND]

    sent_count = 0
    for eid, entry in reversed(new_items):
        msg = format_message(entry)
        send_telegram(msg)
        seen.add(eid)
        sent_count += 1

    state["seen"] = list(seen)[-KEEP_SEEN:]
    save_state(state)

    print(f"Done. sent_count={sent_count}, total_seen={len(state['seen'])}")


if __name__ == "__main__":
    main()
