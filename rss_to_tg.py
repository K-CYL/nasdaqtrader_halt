import os
import json
import html
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import feedparser
import requests


RSS_URL = os.getenv("RSS_URL", "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts")
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
STATE_FILE = os.getenv("STATE_FILE", "state.json")

MAX_SEND = 20
KEEP_SEEN = 1000

ET = ZoneInfo("America/New_York")
KST = ZoneInfo("Asia/Seoul")


REASON_MAP_KR = {
    "T1": "중요 공시 대기",
    "T2": "공시 배포 개시",
    "T3": "공시 완료 및 재개 시간 안내",
    "T5": "개별종목 변동성 정지 발동",
    "T6": "비정상적 시장 활동",
    "T7": "호가만 재개",
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
    "LUDS": "변동성 거래정지(스트래들)",
    "MWC1": "시장 전체 서킷브레이커 1단계",
    "MWC2": "시장 전체 서킷브레이커 2단계",
    "MWC3": "시장 전체 서킷브레이커 3단계",
}


KNOWN_LABELS = [
    "Issue Symbol",
    "Issue Name",
    "Symbol",
    "Ticker",
    "Mkt",
    "Market",
    "Exchange",
    "Reason Code",
    "Halt Code",
    "Halt Date",
    "Halt Time",
    "Resume Date",
    "Resume Time",
    "Resumption Date",
    "Resumption Time",
    "Resumption Quote Time",
    "Resumption Trade Time",
    "Quote Resume Time",
    "Trade Resume Time",
]


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
        "disable_web_page_preview": True,
    }

    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()


def clean_text(raw):

    if raw is None:
        return ""

    text = str(raw)

    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def normalize_key(key):

    key = clean_text(key).lower().strip()
    key = key.replace(":", "")

    return key


def parse_known_fields(raw):

    result = {}

    text = clean_text(raw)

    for label in KNOWN_LABELS:

        pattern = re.compile(label + r"\s*:\s*([^:]+)", re.I)

        m = pattern.search(text)

        if m:
            result[normalize_key(label)] = clean_text(m.group(1))

    return result


def choose(data, *keys):

    for key in keys:

        nk = normalize_key(key)

        if nk in data and data[nk]:
            return data[nk]

    return ""


def normalize_market(value):

    v = (value or "").strip()

    if not v:
        return "-"

    if v.upper() == "NASDAQ":
        return "NASDAQ"

    return v


def normalize_reason(code):

    code = (code or "").strip().upper()

    if not code:
        return "-"

    desc = REASON_MAP_KR.get(code, code)

    return f"{desc} ({code})"


def convert_time(date_str, time_str):

    if not date_str or not time_str:
        return "-"

    try:

        dt = datetime.strptime(
            f"{date_str} {time_str}",
            "%m/%d/%Y %H:%M:%S"
        )

        dt = dt.replace(tzinfo=ET)

        kst = dt.astimezone(KST)

        return f"{dt.strftime('%H:%M:%S')} ET ({kst.strftime('%H:%M:%S')} KST)"

    except:
        return time_str


def format_message(entry):

    title = clean_text(getattr(entry, "title", "") or "")
    summary = getattr(entry, "summary", "") or ""

    parsed = parse_known_fields(summary)

    symbol = choose(parsed, "Issue Symbol", "Symbol")

    if not symbol and title:
        symbol = title.split(" ")[0]

    stock_name = choose(parsed, "Issue Name")

    market = choose(parsed, "Mkt", "Market")

    reason_code = choose(parsed, "Reason Code")

    halt_date = choose(parsed, "Halt Date")

    halt_time = choose(parsed, "Halt Time")

    resume_date = choose(parsed, "Resumption Date", "Resume Date")

    quote_resume_time = choose(parsed, "Resumption Quote Time")

    trade_resume_time = choose(parsed, "Resumption Trade Time")

    symbol = symbol or "-"
    stock_name = stock_name or "-"
    market = normalize_market(market)

    reason_display = normalize_reason(reason_code)

    halt_date = halt_date or "-"

    halt_time_display = convert_time(halt_date, halt_time)

    message = (
        f"종목코드 : {html.escape(symbol)}\n"
        f"종목명 : {html.escape(stock_name)}\n"
        f"거래소 : {html.escape(market)}\n"
        f"정지 사유 : {html.escape(reason_display)}\n"
        f"정지일 : {html.escape(halt_date)}\n"
        f"정지시간 : {html.escape(halt_time_display)}"
    )

    # 재개 정보가 하나라도 있을 때만 출력
    if resume_date or quote_resume_time or trade_resume_time:

        message += (
            f"\n재개일 : {html.escape(resume_date or '-')}"
            f"\n호가재개시간 : {html.escape(convert_time(resume_date, quote_resume_time)) if quote_resume_time else '-'}"
            f"\n거래재개시간 : {html.escape(convert_time(resume_date, trade_resume_time)) if trade_resume_time else '-'}"
        )

    return message


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