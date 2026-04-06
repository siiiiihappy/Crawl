"""
抓取近三個月「美光 / Micron」相關：
1) 新聞：Google News RSS
2) 社群：Reddit / PTT / Dcard

輸出：
- data/micron_news_YYYYMMDD_HHMMSS.csv
- data/micron_social_YYYYMMDD_HHMMSS.csv
- data/micron_combined_YYYYMMDD_HHMMSS.json

安裝：
    pip install -r requirements.txt

執行：
    python micron_monitor.py
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import os
import time
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException


NEWS_QUERY = "美光 OR Micron"
SOCIAL_QUERY = "Micron OR 美光"
SOCIAL_KEYWORDS = ("美光", "micron", "mu")

# 近三個月：以 90 天估算
DAYS_BACK = 90
NEWS_LIMIT = 100
SOCIAL_LIMIT = 100
PTT_PAGES = 40
DCARD_PAGES = 20

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def normalize_text(text: str) -> str:
    return " ".join(text.split())


def to_iso(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat()


def parse_rss_pubdate(pub: str) -> dt.datetime | None:
    try:
        value = parsedate_to_datetime(pub)
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc)
    except Exception:
        return None


def fetch_google_news(query: str, since: dt.datetime, limit: int = 100) -> list[dict[str, Any]]:
    encoded = quote(f"{query} when:3m")
    url = f"https://news.google.com/rss/search?q={encoded}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
    headers = {"User-Agent": USER_AGENT}

    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.content, "xml")
    rows: list[dict[str, Any]] = []

    for item in soup.find_all("item"):
        title = normalize_text(item.title.text if item.title else "")
        link = item.link.text.strip() if item.link else ""
        source_tag = item.find("source")
        source = source_tag.text.strip() if source_tag else "Google News"
        pub_text = item.pubDate.text.strip() if item.pubDate else ""
        pub_dt = parse_rss_pubdate(pub_text)
        if pub_dt and pub_dt < since:
            continue

        rows.append(
            {
                "type": "news",
                "keyword": query,
                "title": title,
                "content": "",
                "source": source,
                "author": "",
                "score": "",
                "url": link,
                "published_at_utc": pub_dt.isoformat() if pub_dt else "",
                "fetched_at_utc": utc_now().isoformat(),
            }
        )
        if len(rows) >= limit:
            break

    return rows


def fetch_reddit_posts(query: str, since: dt.datetime, limit: int = 100) -> list[dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT}
    rows: list[dict[str, Any]] = []
    after = None

    while len(rows) < limit:
        params = {
            "q": query,
            "sort": "new",
            "t": "year",
            "limit": "100",
            "type": "link",
            "raw_json": "1",
        }
        if after:
            params["after"] = after

        resp = requests.get(
            "https://www.reddit.com/search.json",
            params=params,
            headers=headers,
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", {})
        children = data.get("children", [])

        if not children:
            break

        for child in children:
            item = child.get("data", {})
            created = item.get("created_utc")
            if not created:
                continue
            pub_dt = dt.datetime.fromtimestamp(created, tz=dt.timezone.utc)
            if pub_dt < since:
                return rows

            title = normalize_text(item.get("title", ""))
            selftext = normalize_text(item.get("selftext", ""))
            permalink = item.get("permalink", "")
            full_url = f"https://www.reddit.com{permalink}" if permalink else item.get("url", "")
            author = item.get("author", "")
            subreddit = item.get("subreddit", "")
            score = item.get("score", "")

            rows.append(
                {
                    "type": "social",
                    "keyword": query,
                    "title": title,
                    "content": selftext[:1000],
                    "source": f"reddit/r/{subreddit}" if subreddit else "reddit",
                    "author": author,
                    "score": score,
                    "url": full_url,
                    "published_at_utc": pub_dt.isoformat(),
                    "fetched_at_utc": utc_now().isoformat(),
                }
            )
            if len(rows) >= limit:
                break

        after = data.get("after")
        if not after:
            break
        time.sleep(0.5)

    return rows


def contains_keyword(text: str) -> bool:
    low = text.lower()
    return any(k in low for k in SOCIAL_KEYWORDS)


def parse_ptt_post_time(soup: BeautifulSoup) -> dt.datetime | None:
    for m in soup.select("div.article-metaline"):
        tag = m.select_one("span.article-meta-tag")
        value = m.select_one("span.article-meta-value")
        if not tag or not value:
            continue
        if tag.text.strip() == "時間":
            try:
                parsed = parsedate_to_datetime(value.text.strip())
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=dt.timezone.utc)
                return parsed.astimezone(dt.timezone.utc)
            except Exception:
                return None
    return None


def fetch_ptt_posts(since: dt.datetime, limit: int = 100, pages: int = 40) -> list[dict[str, Any]]:
    base = "https://www.ptt.cc"
    board = "Stock"
    headers = {"User-Agent": USER_AGENT}
    rows: list[dict[str, Any]] = []

    session = requests.Session()
    session.headers.update(headers)

    index_url = f"{base}/bbs/{board}/index.html"
    resp = session.get(index_url, timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    prev_link = None
    for a in soup.select("a.btn.wide"):
        if "上頁" in a.get_text(strip=True):
            prev_link = a
            break

    current_index = 0
    if prev_link and prev_link.get("href", "").startswith(f"/bbs/{board}/index"):
        try:
            part = prev_link["href"].split("index", 1)[1].replace(".html", "")
            current_index = int(part) + 1
        except Exception:
            current_index = 0

    for _ in range(pages):
        page_url = f"{base}/bbs/{board}/index{current_index}.html" if current_index > 0 else index_url
        r = session.get(page_url, timeout=20)
        r.raise_for_status()
        page = BeautifulSoup(r.text, "html.parser")

        entries = page.select("div.r-ent")
        if not entries:
            break

        for entry in entries:
            title_tag = entry.select_one("div.title a")
            if not title_tag:
                continue
            title = normalize_text(title_tag.get_text(" ", strip=True))
            if not contains_keyword(title):
                continue

            link = title_tag.get("href", "")
            if not link:
                continue
            full_url = f"{base}{link}"

            post_resp = session.get(full_url, timeout=20)
            post_resp.raise_for_status()
            post_soup = BeautifulSoup(post_resp.text, "html.parser")
            pub_dt = parse_ptt_post_time(post_soup)
            if pub_dt and pub_dt < since:
                continue

            main = post_soup.select_one("div#main-content")
            content = ""
            author = ""
            if main:
                for tag in main.select("div.article-metaline, div.article-metaline-right, div.push"):
                    tag.decompose()
                content = normalize_text(main.get_text(" ", strip=True))[:1000]

            author_tag = post_soup.select_one("span.article-meta-value")
            if author_tag:
                author = author_tag.get_text(strip=True)

            rows.append(
                {
                    "type": "social",
                    "keyword": SOCIAL_QUERY,
                    "title": title,
                    "content": content,
                    "source": "ptt/Stock",
                    "author": author,
                    "score": "",
                    "url": full_url,
                    "published_at_utc": pub_dt.isoformat() if pub_dt else "",
                    "fetched_at_utc": utc_now().isoformat(),
                }
            )
            if len(rows) >= limit:
                return rows
            time.sleep(0.2)

        if current_index <= 1:
            break
        current_index -= 1

    return rows


def fetch_dcard_posts(since: dt.datetime, limit: int = 100, pages: int = 20) -> list[dict[str, Any]]:
    headers = {"User-Agent": USER_AGENT}
    rows: list[dict[str, Any]] = []
    before = None

    for _ in range(pages):
        params = {"popular": "false", "limit": "30"}
        if before is not None:
            params["before"] = str(before)
        resp = requests.get(
            "https://www.dcard.tw/service/api/v2/forums/stock/posts",
            params=params,
            headers=headers,
            timeout=20,
        )
        resp.raise_for_status()
        posts = resp.json()
        if not posts:
            break

        for post in posts:
            title = normalize_text(post.get("title", ""))
            excerpt = normalize_text(post.get("excerpt", ""))
            joined = f"{title} {excerpt}"
            if not contains_keyword(joined):
                continue

            created_at = post.get("createdAt", "")
            pub_dt = None
            if created_at:
                try:
                    pub_dt = dt.datetime.fromisoformat(created_at.replace("Z", "+00:00")).astimezone(
                        dt.timezone.utc
                    )
                except Exception:
                    pub_dt = None
            if pub_dt and pub_dt < since:
                continue

            forum_alias = post.get("forumAlias", "stock")
            post_id = post.get("id")
            url = f"https://www.dcard.tw/f/{forum_alias}/p/{post_id}" if post_id else ""
            anonymous_school = post.get("anonymousSchool", False)
            anonymous_dept = post.get("anonymousDepartment", False)
            if anonymous_school or anonymous_dept:
                author = "anonymous"
            else:
                author = str(post.get("school", "") or post.get("department", "") or "")

            rows.append(
                {
                    "type": "social",
                    "keyword": SOCIAL_QUERY,
                    "title": title,
                    "content": excerpt[:1000],
                    "source": f"dcard/{forum_alias}",
                    "author": author,
                    "score": post.get("likeCount", ""),
                    "url": url,
                    "published_at_utc": pub_dt.isoformat() if pub_dt else "",
                    "fetched_at_utc": utc_now().isoformat(),
                }
            )
            if len(rows) >= limit:
                return rows

        last_id = posts[-1].get("id")
        if not last_id:
            break
        before = last_id
        time.sleep(0.2)

    return rows


def write_csv(path: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    keys = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: str, payload: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    since = utc_now() - dt.timedelta(days=DAYS_BACK)
    out_dir = "data"
    ensure_output_dir(out_dir)

    print("開始抓取：近三個月 美光相關資料")
    print(f"時間區間起點(UTC): {since.isoformat()}")

    news: list[dict[str, Any]] = []
    social: list[dict[str, Any]] = []

    try:
        news = fetch_google_news(NEWS_QUERY, since=since, limit=NEWS_LIMIT)
    except RequestException as exc:
        print(f"新聞來源抓取失敗，已跳過: {exc}")
    print(f"新聞筆數: {len(news)}")

    reddit_rows: list[dict[str, Any]] = []
    ptt_rows: list[dict[str, Any]] = []
    dcard_rows: list[dict[str, Any]] = []

    try:
        reddit_rows = fetch_reddit_posts(SOCIAL_QUERY, since=since, limit=SOCIAL_LIMIT)
    except RequestException as exc:
        print(f"Reddit 抓取失敗，已跳過: {exc}")

    try:
        ptt_rows = fetch_ptt_posts(since=since, limit=SOCIAL_LIMIT, pages=PTT_PAGES)
    except RequestException as exc:
        print(f"PTT 抓取失敗，已跳過: {exc}")

    try:
        dcard_rows = fetch_dcard_posts(since=since, limit=SOCIAL_LIMIT, pages=DCARD_PAGES)
    except RequestException as exc:
        print(f"Dcard 抓取失敗，已跳過: {exc}")

    social = reddit_rows + ptt_rows + dcard_rows
    print(f"  - Reddit: {len(reddit_rows)}")
    print(f"  - PTT: {len(ptt_rows)}")
    print(f"  - Dcard: {len(dcard_rows)}")
    print(f"社群筆數: {len(social)}")

    now_str = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    news_csv = os.path.join(out_dir, f"micron_news_{now_str}.csv")
    social_csv = os.path.join(out_dir, f"micron_social_{now_str}.csv")
    combined_json = os.path.join(out_dir, f"micron_combined_{now_str}.json")

    write_csv(news_csv, news)
    write_csv(social_csv, social)
    write_json(
        combined_json,
        {
            "meta": {
                "generated_at_local": dt.datetime.now().isoformat(),
                "window_days": DAYS_BACK,
                "news_query": NEWS_QUERY,
                "social_query": SOCIAL_QUERY,
            },
            "news": news,
            "social": social,
        },
    )

    print("\n完成輸出：")
    print(f"- {news_csv}")
    print(f"- {social_csv}")
    print(f"- {combined_json}")


if __name__ == "__main__":
    main()
