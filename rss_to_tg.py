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
DISABLE_WEB_PREVIEW = True


REASON_MAP_KR = {
    "T1": "중요 공시 대기",
    "T2": "공시 배포 개시",
    "T5": "개별종목 변동성 정지 발동",
    "T6": "비정상적 시장 활동",
    "T8": "ETF 관련 거래정지",
    "T12": "나스닥 추가 정보 요청",
    "H4": "상장규정 미준수",
    "H9": "정기 공시 미제출 또는 최신 공시 상태 아님",
    "H10": "SEC 거래정지",
    "H11": "규제상 우려",
    "O1": "운영상 거래정지",
    "IPO1": "IPO 종목 거래 개시 전",
    "M1": "기업행위",
    "M2": "호가 정보 없음",
    "LUDP": "변동성 거래정지",
    "LUDS": "변동성 거래정지(스트래들 조건)",
    "MWC1": "시장 전체 서킷브레이커 1단계",
    "MWC2": "시장 전체 서킷브레이커 2단계",
    "MWC3": "시장 전체 서킷브레이커 3단계",
    "MWC0": "전일 이월 시장 전체 서킷브레이커",
    "T3": "공시 완료 및 재개 시간 안내",
    "T7": "호가만 재개, 거래는 계속 정지",
    "R4": "자격요건 이슈 해소, 호가/거래 재개",
    "R9": "공시요건 충족, 호가/거래 재개",
    "C3": "추가 공시 없음, 호가/거래 재개",
    "C4": "자격요건 정지 종료, 유지요건 충족 후 재개",
    "C9": "자격요건 정지 종료, 공시요건 충족 후 재개",
    "C11": "타 규제기관 거래정지 종료 후 호가/거래 재개",
    "R1": "신규 종목 거래 가능",
    "R2": "종목 거래 가능",
    "IPOQ": "IPO 종목 호가 가능",
    "IPOE": "IPO 종목 포지셔닝 윈도우 연장",
    "MWCQ": "시장 전체 서킷브레이커 재개",
    "M": "변동성 거래정지",
    "D": "NASDAQ/CQS에서 종목 삭제",
    "": "사유 정보 없음",
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
        "disable_web_page_preview": DISABLE_WEB_PREVIEW,
    }
    r = requests.post(url, json=payload, timeout=20)
    r.raise_for_status()


def strip_tags(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", raw)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = re.sub(r"(?i)</tr\s*>", "\n", text)
    text = re.sub(r"(?i)</td\s*>", " ", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def normalize_key(key: str) -> str:
    key = html.unescape(key or "")
    key = key.strip().lower()
    key = re.sub(r"\s+", " ", key)
    key = key.replace("&nbsp;", " ")
    key = key.replace(":", "")
    return key


def extract_td_pairs(raw: str) -> dict:
    """
    HTML table 형태:
    <tr><td>Issue Symbol</td><td>QMMM</td></tr>
    """
    result = {}
    if not raw:
        return result

    pattern = re.compile(
        r"(?is)<tr[^>]*>\s*(?:<t[dh][^>]*>\s*(.*?)\s*</t[dh]>\s*){2,}</tr>"
    )

    row_matches = pattern.findall(raw)
    for row in row_matches:
        # 위 패턴은 마지막 그룹만 줄 수 있어 직접 td/th 재파싱
        pass

    row_pattern = re.compile(r"(?is)<tr[^>]*>(.*?)</tr>")
    cell_pattern = re.compile(r"(?is)<t[dh][^>]*>(.*?)</t[dh]>")

    for row_html in row_pattern.findall(raw):
        cells = [strip_tags(c).strip() for c in cell_pattern.findall(row_html)]
        cells = [c for c in cells if c]
        if len(cells) >= 2:
            key = normalize_key(cells[0])
            value = cells[1].strip()
            if key and value:
                result[key] = value

    return result


def extract_label_value_pairs(raw: str) -> dict:
    """
    일반 텍스트/HTML 조각:
    Issue Symbol: QMMM
    Mkt: NASDAQ
    Reason Code: LUDP
    """
    result = {}
    if not raw:
        return result

    text = strip_tags(raw)

    # 줄바꿈 기준 우선
    for line in text.splitlines():
        line = line.strip(" -•\t")
        if ":" in line:
            key, value = line.split(":", 1)
            key = normalize_key(key)
            value = value.strip()
            if key and value and len(key) <= 60:
                result[key] = value

    # 한 줄에 여러 key:value가 붙어 있는 경우 보강
    inline_pattern = re.compile(
        r"(Issue Symbol|Issue Name|Symbol|Ticker|Mkt|Market|Exchange|Reason Code|Halt Code|Halt Date|Halt Time|Date|Time|Pause Threshold Price)\s*:\s*([^:\n\r]+?)(?=\s+(?:Issue Symbol|Issue Name|Symbol|Ticker|Mkt|Market|Exchange|Reason Code|Halt Code|Halt Date|Halt Time|Date|Time|Pause Threshold Price)\s*:|$)",
        re.I,
    )
    for key, value in inline_pattern.findall(text):
        nkey = normalize_key(key)
        nval = value.strip()
        if nkey and nval:
            result[nkey] = nval

    return result


def parse_summary(summary: str) -> dict:
    result = {}

    # 1) HTML table 파싱
    result.update(extract_td_pairs(summary))

    # 2) 일반 key:value 파싱
    kv = extract_label_value_pairs(summary)
    for k, v in kv.items():
        if k not in result or not result[k]:
            result[k] = v

    return result


def choose(data: dict, *keys) -> str:
    for k in keys:
        nk = normalize_key(k)
        if nk in data and data[nk]:
            return data[nk].strip()
    return ""


def normalize_market(market: str) -> str:
    m = (market or "").strip()
    upper = m.upper()

    if upper == "NASDAQ":
        return "NASDAQ"
    if upper in {"NON-NASDAQ", "NON NASDAQ"}:
        return "Non-NASDAQ"

    return m or "-"


def normalize_reason(reason_code: str) -> str:
    code = (reason_code or "").strip().upper()
    if not code:
        return "-"
    reason_kr = REASON_MAP_KR.get(code, code)
    return f"{reason_kr} ({code})"


def extract_symbol_from_title(title: str) -> str:
    raw = strip_tags(title or "").strip()
    if not raw:
        return ""
    # QMMM 또는 "QMMM - ..." 형태 대응
    m = re.match(r"^([A-Z][A-Z0-9.\-]{0,14})\b", raw)
    return m.group(1) if m else raw


def format_message(entry) -> str:
    title = getattr(entry, "title", "") or ""
    summary = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""

    fields = parse_summary(summary)

    symbol = choose(fields, "Issue Symbol", "Symbol", "Ticker")
    stock_name = choose(fields, "Issue Name", "Company Name", "Security Name", "Name")
    market = choose(fields, "Mkt", "Market", "Listing Market", "Exchange")
    reason_code = choose(fields, "Reason Code", "Halt Code", "Code", "Reason")
    halt_date = choose(fields, "Halt Date", "Date")
    halt_time = choose(fields, "Halt Time", "Time")

    # 제목 fallback
    if not symbol:
        symbol = extract_symbol_from_title(title)

    # 값 보정
    symbol = symbol or "-"
    stock_name = stock_name or "-"
    market = normalize_market(market)
    reason_display = normalize_reason(reason_code)
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