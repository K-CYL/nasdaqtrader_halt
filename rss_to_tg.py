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

MAX_SEND = int(os.getenv("MAX_SEND", "20"))
KEEP_SEEN = int(os.getenv("KEEP_SEEN", "1000"))

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
    "IPOQ": "IPO 호가 가능",
    "IPOE": "IPO 포지셔닝 윈도우 연장",
    "M1": "기업행위",
    "M2": "호가 정보 없음",
    "M": "변동성 거래정지",
    "LUDP": "변동성 거래정지",
    "LUDS": "변동성 거래정지(스트래들)",
    "MWC0": "전일 이월 시장 전체 서킷브레이커",
    "MWC1": "시장 전체 서킷브레이커 1단계",
    "MWC2": "시장 전체 서킷브레이커 2단계",
    "MWC3": "시장 전체 서킷브레이커 3단계",
    "MWCQ": "시장 전체 서킷브레이커 재개",
    "R1": "신규 종목 거래 가능",
    "R2": "종목 거래 가능",
    "R4": "자격요건 이슈 해소 후 재개",
    "R9": "공시요건 충족 후 재개",
    "C3": "추가 공시 없음, 거래 재개",
    "C4": "상장요건 충족 후 거래 재개",
    "C9": "공시요건 충족 후 거래 재개",
    "C11": "규제기관 정지 종료 후 거래 재개",
    "D": "NASDAQ/CQS 삭제",
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
    "Resume Quote Time",
    "Resume Trade Time",
    "Date",
    "Time",
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


def clean_text(raw) -> str:
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


def parse_known_fields(raw) -> dict:
    result = {}
    text = clean_text(raw)

    if not text:
        return result

    labels_pattern = "|".join(sorted((re.escape(x) for x in KNOWN_LABELS), key=len, reverse=True))
    pattern = re.compile(
        rf"(?i)\b({labels_pattern})\s*:\s*(.*?)(?=\s+(?:{labels_pattern})\s*:|$)"
    )

    for label, value in pattern.findall(text):
        nkey = normalize_key(label)
        nval = clean_text(value)
        if nkey and nval:
            result[nkey] = nval

    for line in text.splitlines():
        line = line.strip(" -•\t")
        if ":" in line:
            key, value = line.split(":", 1)
            nkey = normalize_key(key)
            nval = clean_text(value)
            if nkey and nval and nkey not in result:
                result[nkey] = nval

    return result


def extract_entry_field(entry, *candidate_keys) -> str:
    for key in candidate_keys:
        if key in entry and entry.get(key):
            return clean_text(entry.get(key))

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


def convert_et_to_kst(date_str: str, time_str: str):
    date_str = clean_text(date_str)
    time_str = clean_text(time_str)

    if not date_str or not time_str:
        return "-", "-"

    time_formats = ["%H:%M:%S", "%H:%M"]
    date_formats = ["%m/%d/%Y", "%m/%d/%y"]

    for df in date_formats:
        for tf in time_formats:
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", f"{df} {tf}")
                dt = dt.replace(tzinfo=ET)
                kst_dt = dt.astimezone(KST)
                return dt.strftime("%H:%M:%S"), kst_dt.strftime("%H:%M:%S")
            except ValueError:
                continue

    return time_str, "-"


def format_time_with_kst(date_str: str, time_str: str) -> str:
    date_str = clean_text(date_str)
    time_str = clean_text(time_str)

    if not time_str:
        return "-"

    et_time, kst_time = convert_et_to_kst(date_str, time_str)

    if et_time != "-" and kst_time != "-":
        return f"{et_time} ET ({kst_time} KST)"

    return time_str


def format_message(entry) -> str:
    title = clean_text(getattr(entry, "title", "") or "")
    summary = getattr(entry, "summary", "") or ""
    description = getattr(entry, "description", "") or ""

    parsed = {}
    parsed.update(parse_known_fields(summary))
    parsed.update(parse_known_fields(description))

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
        choose(parsed, "Halt Date")
        or extract_entry_field(entry, "haltdate", "halt_date")
    )

    halt_time = (
        choose(parsed, "Halt Time")
        or extract_entry_field(entry, "halttime", "halt_time")
    )

    resume_date = (
        choose(parsed, "Resumption Date", "Resume Date")
        or extract_entry_field(entry, "resumptiondate", "resumption_date", "resumedate", "resume_date")
    )

    quote_resume_time = (
        choose(parsed, "Resumption Quote Time", "Quote Resume Time", "Resume Quote Time")
        or extract_entry_field(
            entry,
            "resumptionquotetime",
            "resumption_quote_time",
            "quoteresumetime",
            "quote_resume_time",
            "resumequotetime",
            "resume_quote_time",
        )
    )

    trade_resume_time = (
        choose(parsed, "Resumption Trade Time", "Trade Resume Time", "Resume Trade Time")
        or extract_entry_field(
            entry,
            "resumptiontradetime",
            "resumption_trade_time",
            "traderesumetime",
            "trade_resume_time",
            "resumetradetime",
            "resume_trade_time",
        )
    )

    generic_resume_time = (
        choose(parsed, "Resumption Time", "Resume Time")
        or extract_entry_field(entry, "resumptiontime", "resumption_time", "resumetime", "resume_time")
    )

    if not quote_resume_time and not trade_resume_time and generic_resume_time:
        trade_resume_time = generic_resume_time

    symbol = symbol or "-"
    stock_name = stock_name or "-"
    market = normalize_market(market)
    reason_display = normalize_reason(reason_code)
    halt_date = halt_date or "-"
    halt_time_display = format_time_with_kst(halt_date, halt_time)

    resume_date = resume_date or "-"
    quote_resume_display = format_time_with_kst(resume_date, quote_resume_time) if quote_resume_time else "-"
    trade_resume_display = format_time_with_kst(resume_date, trade_resume_time) if trade_resume_time else "-"

    return (
        f"종목코드 : {html.escape(symbol)}\n"
        f"종목명 : {html.escape(stock_name)}\n"
        f"거래소 : {html.escape(market)}\n"
        f"정지 사유 : {html.escape(reason_display)}\n"
        f"정지일 : {html.escape(halt_date)}\n"
        f"정지시간 : {html.escape(halt_time_display)}\n"
        f"재개일 : {html.escape(resume_date)}\n"
        f"호가재개시간 : {html.escape(quote_resume_display)}\n"
        f"거래재개시간 : {html.escape(trade_resume_display)}"
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