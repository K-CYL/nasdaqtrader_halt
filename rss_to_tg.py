import os
import json
import html
import re
from pprint import pformat

import feedparser
import requests

RSS_URL = os.getenv("RSS_URL", "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts")
BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
STATE_FILE = os.getenv("STATE_FILE", "state.json")

MAX_SEND = int(os.getenv("MAX_SEND", "20"))
KEEP_SEEN = int(os.getenv("KEEP_SEEN", "1000"))
DEBUG_LOG = os.getenv("DEBUG_LOG", "true").lower() == "true"


REASON_MAP_KR = {
    "T1": "중요 공시 대기",
    "T2": "공시 배포 개시",
    "T3": "공시 완료 및 재개 시간 안내",
    "T5": "개별종목 변동성 정지 발동",
    "T6": "비정상적 시장 활동",
    "T7": "호가만 재개, 거래는 계속 정지",
    "T8": "ETF 관련 거래정지",
    "T12": "추가 정보 요청",
    "H4": "상장규정 미준수",
    "H9": "정기 공시 미제출 또는 최신 공시 상태 아님",
    "H10": "SEC 거래정지",
    "H11": "규제상 우려",
    "O1": "운영상 거래정지",
    "IPO1": "IPO 종목 거래 개시 전",
    "IPOQ": "IPO 종목 호가 가능",
    "IPOE": "IPO 종목 포지셔닝 윈도우 연장",
    "M1": "기업행위",
    "M2": "호가 정보 없음",
    "M": "변동성 거래정지",
    "LUDP": "변동성 거래정지",
    "LUDS": "변동성 거래정지(스트래들 조건)",
    "MWC0": "전일 이월 시장 전체 서킷브레이커",
    "MWC1": "시장 전체 서킷브레이커 1단계",
    "MWC2": "시장 전체 서킷브레이커 2단계",
    "MWC3": "시장 전체 서킷브레이커 3단계",
    "MWCQ": "시장 전체 서킷브레이커 재개",
    "R1": "신규 종목 거래 가능",
    "R2": "종목 거래 가능",
    "R4": "자격요건 이슈 해소, 호가/거래 재개",
    "R9": "공시요건 충족, 호가/거래 재개",
    "C3": "추가 공시 없음, 호가/거래 재개",
    "C4": "자격요건 정지 종료, 유지요건 충족 후 재개",
    "C9": "자격요건 정지 종료, 공시요건 충족 후 재개",
    "C11": "타 규제기관 거래정지 종료 후 호가/거래 재개",
    "D": "NASDAQ/CQS에서 종목 삭제",
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


def clean_text(raw: str) -> str:
    if raw is None:
        return ""
    text = str(raw)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = re.sub(r"(?i)</tr\s*>", "\n", text)
    text = re.sub(r"(?i)</td\s*>", " ", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def normalize_key(key: str) -> str:
    key = clean_text(key).lower().strip()
    key = key.replace(":", "")
    key = re.sub(r"\s+", " ", key)
    return key


def parse_summary_to_dict(raw: str) -> dict:
    result = {}
    if not raw:
        return result

    text = clean_text(raw)

    # 기본 줄 단위 key:value
    for line in text.splitlines():
        line = line.strip(" -•\t")
        if ":" in line:
            key, value = line.split(":", 1)
            key = normalize_key(key)
            value = clean_text(value)
            if key and value:
                result[key] = value

    # 한 줄에 필드가 붙어있는 경우
    pattern = re.compile(
        r"(Issue Symbol|Issue Name|Symbol|Ticker|Mkt|Market|Exchange|Reason Code|Halt Code|Halt Date|Halt Time|Date|Time|Pause Threshold Price)\s*:\s*(.*?)(?=(?:Issue Symbol|Issue Name|Symbol|Ticker|Mkt|Market|Exchange|Reason Code|Halt Code|Halt Date|Halt Time|Date|Time|Pause Threshold Price)\s*:|$)",
        re.I,
    )
    for key, value in pattern.findall(text):
        nkey = normalize_key(key)
        nval = clean_text(value)
        if nkey and nval:
            result[nkey] = nval

    return result


def extract_entry_field(entry, *candidate_keys) -> str:
    """
    feedparser entry 내부의 다양한 키를 탐색
    """
    for key in candidate_keys:
        if key in entry and entry.get(key):
            return clean_text(entry.get(key))

    # 키 이름 일부 매칭
    lowered_candidates = [k.lower() for k in candidate_keys]
    for ek in entry.keys():
        ek_norm = ek.lower()
        for cand in lowered_candidates:
            if cand in ek_norm and entry.get(ek):
                return clean_text(entry.get(ek))

    return ""


def choose(data: dict, *keys) -> str:
    for key in keys:
        nk = normalize_key(key)
        if nk in data and data[nk]:
            return clean_text(data[nk])
    return ""


def normalize_market(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return "-"
    upper = v.upper()
    if upper == "NASDAQ":
        return "NASDAQ"
    if upper in {"NON-NASDAQ", "NON NASDAQ"}:
        return "Non-NASDAQ"
    return v


def normalize_reason(code: str) -> str:
    code = (code or "").strip().upper()
    if not code:
        return "-"
    desc = REASON_MAP_KR.get(code, code)
    return f"{desc} ({code})"


def extract_symbol_from_title(title: str) -> str:
    title = clean_text(title)
    if not title:
        return ""
    m = re.match(r"^([A-Z][A-Z0-9.\-]{0,14})\b", title)
    return m.group(1) if m else title


def split_date_time_if_needed(date_val: str, time_val: str):
    """
    정지일에 'Halt Time' 같은 헤더가 잘못 들어오는 경우 방지
    """
    date_val = clean_text(date_val)
    time_val = clean_text(time_val)

    if date_val.lower() in {"halt time", "time"}:
        date_val = ""

    # date_val에 날짜/시간이 같이 들어온 경우
    m = re.match(r"^(\d{1,2}/\d{1,2}/\d{4})[ ,]+(\d{1,2}:\d{2}:\d{2})$", date_val)
    if m:
        return m.group(1), m.group(2)

    # time_val에 날짜/시간이 같이 들어온 경우
    m = re.match(r"^(\d{1,2}/\d{1,2}/\d{4})[ ,]+(\d{1,2}:\d{2}:\d{2})$", time_val)
    if m:
        return m.group(1), m.group(2)

    return date_val, time_val


def debug_dump_entry(entry):
    if not DEBUG_LOG:
        return
    try:
        print("========== DEBUG ENTRY START ==========")
        print("TITLE:", repr(getattr(entry, "title", "")))
        print("LINK:", repr(getattr(entry, "link", "")))
        print("SUMMARY:", repr(getattr(entry, "summary", "")))
        print("DESCRIPTION:", repr(getattr(entry, "description", "")))
        print("ENTRY KEYS:", list(entry.keys()))
        print("ENTRY RAW:")
        print(pformat(dict(entry)))
        print("========== DEBUG ENTRY END ==========")
    except Exception as e:
        print("DEBUG DUMP ERROR:", repr(e))


def format_message(entry) -> str:
    title = clean_text(getattr(entry, "title", "") or "")
    summary = getattr(entry, "summary", "") or ""
    description = getattr(entry, "description", "") or ""

    parsed = {}
    parsed.update(parse_summary_to_dict(summary))
    parsed.update(parse_summary_to_dict(description))

    symbol = (
        choose(parsed, "Issue Symbol", "Symbol", "Ticker")
        or extract_entry_field(entry, "issuesymbol", "issue_symbol", "symbol", "ticker")
        or extract_symbol_from_title(title)
    )

    stock_name = (
        choose(parsed, "Issue Name", "Company Name", "Security Name", "Name")
        or extract_entry_field(entry, "issuename", "issue_name", "company", "securityname", "security_name", "name")
    )

    market = (
        choose(parsed, "Mkt", "Market", "Exchange", "Listing Market")
        or extract_entry_field(entry, "mkt", "market", "exchange", "listingmarket", "listing_market")
    )

    reason_code = (
        choose(parsed, "Reason Code", "Halt Code", "Code", "Reason")
        or extract_entry_field(entry, "reasoncode", "reason_code", "haltcode", "halt_code", "reason", "code")
    )

    halt_date = (
        choose(parsed, "Halt Date", "Date")
        or extract_entry_field(entry, "haltdate", "halt_date", "date")
    )

    halt_time = (
        choose(parsed, "Halt Time", "Time")
        or extract_entry_field(entry, "halttime", "halt_time", "time")
    )

    halt_date, halt_time = split_date_time_if_needed(halt_date, halt_time)

    # 여전히 비어 있으면 디버그 로그 남김
    if DEBUG_LOG and (not stock_name or not market or not reason_code or not halt_date or not halt_time):
        debug_dump_entry(entry)

    symbol = symbol or "-"
    stock_name = stock_name or "-"
    market = normalize_market(market)
    reason_display = normalize_reason(reason_code)
    halt_date = halt_date or "-"
    halt_time = halt_time or "-"

    return (
        f"종목코드 : {html.escape(symbol)}\n"
        f"종목명 : {html.escape(stock_name)}\n"
        f"거래소 : {html.escape(market)}\n"
        f"정지 사유 : {html.escape(reason_display)}\n"
        f"정지일 : {html.escape(halt_date)}\n"
        f"정지시간 : {html.escape(halt_time)}"
    )


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