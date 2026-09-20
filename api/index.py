"""
Мониторинг залетевших молодых музыкальных каналов — версия для бесплатного serverless
(Vercel), без Mac. Работает по принципу "разбудили — проверили — уснули": вся логика
выполняется за ОДИН вызов, а сам вызов делает внешний бесплатный планировщик
(cron-job.org), который стучится сюда каждые несколько часов.

Переменные окружения (в панели Vercel):
  YOUTUBE_API_KEY           — тот же ключ, что в Niche Scout
  TELEGRAM_TOKEN            — токен бота (тот же, что уже используешь)
  TELEGRAM_CHAT_ID          — твой числовой chat ID (узнать через @userinfobot)
  UPSTASH_REDIS_REST_URL    — та же база, что у бота, или отдельная
  UPSTASH_REDIS_REST_TOKEN
  MONITOR_SECRET            — придуманный тобой пароль для защиты адреса от посторонних
  MONITOR_KEYWORDS          — необязательно, через запятую (например "long mix,dj set,hours")
  MONITOR_MIN_VIEWS         — необязательно, по умолчанию 5000
  MONITOR_MAX_SUBS          — необязательно, по умолчанию 10000
  MONITOR_MAX_AGE_DAYS      — необязательно, по умолчанию 30
"""

import os
import json
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, timezone
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
MONITOR_SECRET = os.environ.get("MONITOR_SECRET", "")
UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")

MIN_VIEWS = int(os.environ.get("MONITOR_MIN_VIEWS", "5000"))
MAX_SUBS = int(os.environ.get("MONITOR_MAX_SUBS", "10000"))
MAX_AGE_DAYS = int(os.environ.get("MONITOR_MAX_AGE_DAYS", "30"))
KEYWORDS = [k.strip() for k in os.environ.get("MONITOR_KEYWORDS", "").split(",") if k.strip()] or [""]
# Пустая строка — это осознанный широкий поиск по ВСЕЙ категории "Музыка" (плюс длинный
# формат), без сужения до конкретных слов. Раньше это не работало из-за отдельного бага
# с лимитом в 50 ID на пачку — он уже исправлен, так что теперь это безопасно и работает.


def redis_command(*args):
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return None
    resp = requests.post(UPSTASH_URL, headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                          json=list(args), timeout=10)
    resp.raise_for_status()
    return resp.json().get("result")


def load_seen_ids():
    raw = redis_command("GET", "monitor:seen_ids")
    if not raw:
        return set()
    try:
        return set(json.loads(raw))
    except Exception:
        return set()


def save_seen_ids(seen_ids):
    trimmed = list(seen_ids)[-2000:]  # не даём списку расти бесконечно
    redis_command("SET", "monitor:seen_ids", json.dumps(trimmed))


def api_get(endpoint, params):
    url = f"https://www.googleapis.com/youtube/v3/{endpoint}?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode())
            reason = body.get("error", {}).get("message", str(e))
        except Exception:
            reason = str(e)
        raise RuntimeError(f"YouTube API вернул ошибку ({e.code}): {reason}")


def search_recent_long_videos(keyword, days_back=30, max_pages=1):
    published_after = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    all_items = []
    next_page = None
    for _ in range(max_pages):
        params = {
            "part": "snippet", "type": "video", "videoDuration": "long", "order": "viewCount",
            "publishedAfter": published_after, "maxResults": 50, "videoCategoryId": "10",
            "key": YOUTUBE_API_KEY,
        }
        if keyword:
            params["q"] = keyword
        if next_page:
            params["pageToken"] = next_page
        data = api_get("search", params)
        all_items.extend(data.get("items", []))
        next_page = data.get("nextPageToken")
        if not next_page:
            break
    return all_items


def get_video_stats(video_ids):
    if not video_ids:
        return {}
    result = {}
    for i in range(0, len(video_ids), 50):  # YouTube принимает максимум 50 id за раз
        batch = video_ids[i:i + 50]
        data = api_get("videos", {"part": "statistics,snippet,contentDetails",
                                    "id": ",".join(batch), "key": YOUTUBE_API_KEY})
        for item in data.get("items", []):
            result[item["id"]] = item
    return result


def get_channel_stats(channel_ids):
    if not channel_ids:
        return {}
    result = {}
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i + 50]
        data = api_get("channels", {"part": "statistics,snippet,contentDetails",
                                      "id": ",".join(batch), "key": YOUTUBE_API_KEY})
        for item in data.get("items", []):
            result[item["id"]] = item
    return result


def get_first_video_date(uploads_playlist_id):
    earliest = None
    next_page = None
    fetched = 0
    while fetched < 150:  # уменьшено со 500 ради скорости — важно уложиться в тайм-аут
        params = {"part": "snippet", "playlistId": uploads_playlist_id, "maxResults": 50,
                   "key": YOUTUBE_API_KEY}
        if next_page:
            params["pageToken"] = next_page
        try:
            data = api_get("playlistItems", params)
        except Exception:
            break
        items = data.get("items", [])
        for item in items:
            published = item["snippet"]["publishedAt"]
            if earliest is None or published < earliest:
                earliest = published
        fetched += len(items)
        next_page = data.get("nextPageToken")
        if not next_page:
            break
    return earliest


def telegram_send_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram отклонил сообщение: {data.get('description', data)}")


def run_check():
    if not YOUTUBE_API_KEY or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return {"error": "Не заданы обязательные переменные окружения "
                          "(YOUTUBE_API_KEY / TELEGRAM_TOKEN / TELEGRAM_CHAT_ID)"}

    seen_ids = load_seen_ids()
    found = []
    errors = []
    age_checks_done = 0
    MAX_AGE_CHECKS_PER_RUN = 15  # самая медленная операция — ограничиваем, чтобы не упереться в тайм-аут

    for kw in KEYWORDS:
        try:
            items = search_recent_long_videos(kw)
            video_ids = [it["id"]["videoId"] for it in items if "videoId" in it.get("id", {})]
            channel_ids = list({it["snippet"]["channelId"] for it in items if "channelId" in it.get("snippet", {})})
            if not video_ids:
                continue
            video_data = get_video_stats(video_ids)
            channel_data = get_channel_stats(channel_ids)

            for it in items:
                vid = it["id"].get("videoId")
                if not vid or vid in seen_ids:
                    continue
                cid = it["snippet"]["channelId"]
                vdata, cdata = video_data.get(vid), channel_data.get(cid)
                if not vdata or not cdata:
                    continue
                views = int(vdata["statistics"].get("viewCount", 0))
                subs = int(cdata["statistics"].get("subscriberCount", 0))
                if views < MIN_VIEWS or subs > MAX_SUBS:
                    continue  # пока не подходит — проверим ещё раз в следующий вызов
                if age_checks_done >= MAX_AGE_CHECKS_PER_RUN:
                    continue  # отложим до следующего вызова — не проверено, не помечаем как seen
                uploads_playlist = cdata["contentDetails"]["relatedPlaylists"]["uploads"]
                first_video_date = get_first_video_date(uploads_playlist)
                age_checks_done += 1
                if not first_video_date:
                    continue
                channel_age_days = (datetime.now(timezone.utc) -
                                     datetime.strptime(first_video_date[:10], "%Y-%m-%d").replace(
                                         tzinfo=timezone.utc)).days
                if channel_age_days > MAX_AGE_DAYS:
                    seen_ids.add(vid)  # возраст назад не пойдёт — исключаем насовсем
                    continue

                title = it["snippet"]["title"]
                channel_title = it["snippet"].get("channelTitle", "?")
                url = f"https://www.youtube.com/watch?v={vid}"
                message = (f"🚀 Залетевшее видео у молодого канала!\n\n"
                           f"Канал: {channel_title} (ведётся {channel_age_days} дн., {subs} подписчиков)\n"
                           f"Видео: {title}\nПросмотров: {views:,}".replace(",", " ") + f"\n{url}")
                seen_ids.add(vid)
                telegram_send_message(message)
                found.append({"title": title, "channel": channel_title, "views": views, "url": url})
        except Exception as e:
            errors.append(f"{kw}: {e}")

    save_seen_ids(seen_ids)
    return {"found": found, "errors": errors, "checked_keywords": KEYWORDS}


@app.route("/", methods=["GET", "POST"])
@app.route("/api/monitor", methods=["GET", "POST"])
def monitor():
    secret = request.args.get("secret", "")
    if not MONITOR_SECRET or secret != MONITOR_SECRET:
        return jsonify({"error": "Неверный или отсутствующий secret"}), 401
    result = run_check()
    return jsonify(result)
