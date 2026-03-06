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

MAX_SEND = 20
KEEP_SEEN = 1000


REASON_MAP_KR = {
    "T1": "중요 공시 대기",
    "T2": "공시 배포 개시",
    "T5": "개별종목 변동성 거래정지 발동",
    "T6": "비정상적 시장 활동",
    "T8": "ETF 관련 거래정지",
    "T12": "추가 정보 요청",
    "H4": "상장규정 미준수",
    "H9": "정기 공시 미제출",
    "H10": "SEC 거래정지",
    "H11": "규제상 우려",
    "O1": "운영상 거래정지",
    "IPO1": "IPO 거래 개시 전",
    "M1": "기업행위",
    "M2": "호가 정보 없음",
    "LUDP": "변동성 거래정지",
    "LUDS": "변동성 거래정지 (Straddle)",
    "MWC1": "시장 전체 서킷브레이커 1단계",
    "MWC2": "시장 전체 서킷브레이커 2단계",
    "MWC3": "시장 전체 서킷브레이커 3단계",
    "MWC0": "시장 전체 서킷브레이커 (전일 이월)",
    "T3": "공시 완료",
    "T7": "호가 재개",
    "R4": "거래 재개",
    "R9": "공시요건 충족 후 거래 재개",
    "C3": "추가 공시 없음, 거래 재개",
    "C4": "상장요건 충족 후 거래 재개",
    "C9": "공시요건 충족 후 거래 재개",
    "C11": "규제기관 정지 종료 후 거래 재개",
}


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
        or f"{getattr(entry,'title','')}|{getattr(entry,'published','')}"
    )


def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()


def clean_html_text(raw):
    if not raw:
        return ""

    text = re.sub(r"<br\s*/?>", "\n", raw, flags=re.I)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)

    text = html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def parse_summary_to_dict(summary):

    text = clean_html_text(summary)
    result = {}

    for line in text.splitlines():

        line = line.strip(" -•\t")

        if ":" in line:

            key, value = line.split(":", 1)

            key = key.strip().lower()
            value = value.strip()

            result[key] = value

    return result


def choose(data, *keys):

    for k in keys:

        if k in data and data[k]:
            return data[k]

    return ""


def format_message(entry):

    title = getattr(entry, "title", "") or ""
    summary = getattr(entry, "summary", "") or getattr(entry, "description", "")

    fields = parse_summary_to_dict(summary)

    symbol = choose(fields, "issue symbol", "symbol", "ticker")
    stock_name = choose(fields, "issue name", "company name", "security name", "name")
    market = choose(fields, "mkt", "market", "listing market", "exchange")

    reason_code = choose(fields, "reason code", "halt code", "code", "reason")

    halt_date = choose(fields, "halt date", "date")
    halt_time = choose(fields, "halt time", "time")

    if not symbol and title:
        symbol = clean_html_text(title).strip()

    reason_kr = REASON_MAP_KR.get(reason_code, reason_code)

    if reason_code:
        reason_display = f"{reason_kr} ({reason_code})"
    else:
        reason_display = reason_kr

    symbol = symbol or "-"
    stock_name = stock_name or "-"
    market = market or "-"
    halt_date = halt_date or "-"
    halt_time = halt_time or "-"

    msg = (
        f"종목코드 : {html.escape(symbol)}\n"
        f"종목명 : {html.escape(stock_name)}\n"
        f"거래소 : {html.escape(market)}\n"
        f"정지 사유 : {html.escape(reason_display)}\n"
        f"정지일 : {html.escape(halt_date)}\n"
        f"정지시간 : {html.escape(halt_time)}"
    )

    return msg


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

    for eid, entry in reversed(new_items):

        msg = format_message(entry)

        send_telegram(msg)

        seen.add(eid)

    state["seen"] = list(seen)[-KEEP_SEEN:]

    save_state(state)


if __name__ == "__main__":
    main()
