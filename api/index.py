"""
Мониторинг залетевших видео у МОЛОДЫХ музыкальных каналов — весь YouTube-раздел "Музыка",
без ключевых слов. Версия для бесплатного serverless (Vercel): "разбудили — проверили —
уснули". Вызов делает внешний планировщик (cron-job.org) 1-2 раза в сутки.

Как это работает (коротко):
  1. Берём длинные видео (20+ минут) категории "Музыка" за последние DAYS_BACK дней
     и режем это окно на отдельные дни. В каждом дне берём топ по просмотрам.
     Так свежие видео не теряются за старыми гигантами, как было раньше.
  2. Оставляем видео с просмотрами >= MIN_VIEWS у каналов с подписчиками <= MAX_SUBS.
  3. Канал считается молодым, если он создан не раньше MAX_AGE_DAYS дней назад.
     Если канал создан давно, но у него мало видео (<= 50), смотрим дату первого видео.
  4. Шлём в Telegram, запоминаем отправленное в Upstash Redis, чтобы не повторяться.

Переменные окружения (в панели Vercel):
  YOUTUBE_API_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
  UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN, MONITOR_SECRET  — как и раньше.
  Необязательные (если не заданы — работают значения по умолчанию):
  MONITOR_MIN_VIEWS        — минимум просмотров у видео, по умолчанию 5000
  MONITOR_MAX_SUBS         — максимум подписчиков у канала, по умолчанию 10000
  MONITOR_MAX_AGE_DAYS     — максимальный возраст канала в днях, по умолчанию 30
  MONITOR_DAYS_BACK        — за сколько последних дней искать видео, по умолчанию 10
  MONITOR_PAGES_PER_DAY    — сколько страниц по 50 видео брать на каждый день, по умолчанию 2
  MONITOR_MAX_ALERTS       — максимум уведомлений за один запуск, по умолчанию 15

Квота YouTube API: один запуск тратит DAYS_BACK x PAGES_PER_DAY x 100 единиц на поиск
(по умолчанию 10 x 2 x 100 = 2000) плюс несколько десятков единиц на статистику.
Дневной лимит ключа — 10 000 единиц, и он общий со всеми твоими программами на этом ключе.
"""

import os
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from concurrent.futures import ThreadPoolExecutor
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
DAYS_BACK = int(os.environ.get("MONITOR_DAYS_BACK", "10"))
PAGES_PER_DAY = int(os.environ.get("MONITOR_PAGES_PER_DAY", "2"))
MAX_ALERTS = int(os.environ.get("MONITOR_MAX_ALERTS", "15"))

MAX_AGE_CHECKS = 20      # сколько "старых, но маленьких" каналов проверять по первому видео за запуск
TIME_BUDGET_SEC = 24     # cron-job.org ждёт ответ максимум 30 секунд — страхуемся
SEEN_LIMIT = 3000        # сколько последних отправленных/отброшенных видео помнить


# ---------------------------------------------------------------- Redis (память "что уже видели")

def redis_command(*args):
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return None
    resp = requests.post(UPSTASH_URL, headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                         json=list(args), timeout=10)
    resp.raise_for_status()
    return resp.json().get("result")


def load_seen_ids():
    """Возвращает список в порядке добавления (старые — в начале)."""
    raw = redis_command("GET", "monitor:seen_ids")
    if not raw:
        return []
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def save_seen_ids(seen_list):
    redis_command("SET", "monitor:seen_ids", json.dumps(seen_list[-SEEN_LIMIT:]))


# ---------------------------------------------------------------- YouTube API

def api_get(endpoint, params):
    url = f"https://www.googleapis.com/youtube/v3/{endpoint}?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode())
            reason = body.get("error", {}).get("message", str(e))
        except Exception:
            reason = str(e)
        raise RuntimeError(f"YouTube API вернул ошибку ({e.code}): {reason}")


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_dt(s):
    return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)


def search_window(start, end):
    """Топ по просмотрам среди длинных музыкальных видео, опубликованных между start и end."""
    items, token = [], None
    for _ in range(PAGES_PER_DAY):
        params = {
            "part": "snippet", "type": "video", "videoDuration": "long", "order": "viewCount",
            "videoCategoryId": "10", "maxResults": 50,
            "publishedAfter": iso(start), "publishedBefore": iso(end), "key": YOUTUBE_API_KEY,
        }
        if token:
            params["pageToken"] = token
        data = api_get("search", params)
        items.extend(data.get("items", []))
        token = data.get("nextPageToken")
        if not token:
            break
    return items


def fetch_batches(endpoint, part, ids, errors):
    """Статистика пачками по 50 id (максимум YouTube), пачки идут параллельно."""
    batches = [ids[i:i + 50] for i in range(0, len(ids), 50)]

    def one(batch):
        try:
            data = api_get(endpoint, {"part": part, "id": ",".join(batch), "key": YOUTUBE_API_KEY})
            return data.get("items", [])
        except Exception as e:
            errors.append(str(e))
            return []

    result = {}
    if not batches:
        return result
    with ThreadPoolExecutor(max_workers=5) as ex:
        for items in ex.map(one, batches):
            for item in items:
                result[item["id"]] = item
    return result


def first_video_date(uploads_playlist_id):
    """Для каналов с <= 50 видео одной страницы хватает, чтобы найти самое раннее."""
    try:
        data = api_get("playlistItems", {"part": "snippet", "playlistId": uploads_playlist_id,
                                         "maxResults": 50, "key": YOUTUBE_API_KEY})
    except Exception:
        return None
    dates = [it["snippet"]["publishedAt"] for it in data.get("items", [])
             if it.get("snippet", {}).get("publishedAt")]
    return min(dates) if dates else None


# ---------------------------------------------------------------- Telegram

def telegram_send_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    resp = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                                    "disable_web_page_preview": False}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Telegram отклонил сообщение: {data.get('description', data)}")


# ---------------------------------------------------------------- основная проверка

def run_check():
    if not YOUTUBE_API_KEY or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return {"error": "Не заданы обязательные переменные окружения "
                         "(YOUTUBE_API_KEY / TELEGRAM_TOKEN / TELEGRAM_CHAT_ID)"}

    t0 = time.time()
    now = datetime.now(timezone.utc)
    errors = []
    found = []
    stats = {"days_searched": DAYS_BACK, "pool": 0, "already_seen": 0, "views_ok": 0,
             "subs_ok": 0, "young": 0, "alerts_sent": 0}

    try:
        seen_list = load_seen_ids()
    except Exception as e:
        seen_list = []
        errors.append(f"Redis (чтение): {e}")
    seen = set(seen_list)

    def mark_seen(vid):
        if vid not in seen:
            seen.add(vid)
            seen_list.append(vid)

    # 1. Поиск: каждый день отдельным окном, окна идут параллельно
    windows = [(now - timedelta(days=i + 1), now - timedelta(days=i)) for i in range(DAYS_BACK)]
    items = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = [ex.submit(search_window, s, e) for s, e in windows]
        for f in futures:
            try:
                items.extend(f.result())
            except Exception as e:
                errors.append(str(e))

    videos = {}
    for it in items:
        vid = it.get("id", {}).get("videoId")
        if vid and vid not in videos and it.get("snippet", {}).get("channelId"):
            videos[vid] = it
    stats["pool"] = len(videos)

    fresh = [v for v in videos if v not in seen]
    stats["already_seen"] = len(videos) - len(fresh)

    # 2. Просмотры
    vdata = fetch_batches("videos", "statistics", fresh, errors)
    popular = []
    for vid in fresh:
        v = vdata.get(vid)
        if v and int(v.get("statistics", {}).get("viewCount", 0)) >= MIN_VIEWS:
            popular.append(vid)
    popular.sort(key=lambda v: int(vdata[v]["statistics"].get("viewCount", 0)), reverse=True)
    stats["views_ok"] = len(popular)

    # 3. Каналы — только для тех, у кого видео уже набрало просмотры
    channel_ids = list({videos[v]["snippet"]["channelId"] for v in popular})
    cdata = fetch_batches("channels", "statistics,snippet,contentDetails", channel_ids, errors)

    candidates = []
    age_checks = 0
    for vid in popular:
        cid = videos[vid]["snippet"]["channelId"]
        c = cdata.get(cid)
        if not c:
            continue
        cstats = c.get("statistics", {})
        subs = None if cstats.get("hiddenSubscriberCount") else int(cstats.get("subscriberCount", 0))
        if subs is not None and subs > MAX_SUBS:
            continue
        stats["subs_ok"] += 1

        created_days = (now - parse_dt(c["snippet"]["publishedAt"])).days
        video_count = int(cstats.get("videoCount", 0))
        channel_age = created_days

        if created_days > MAX_AGE_DAYS:
            # канал создан давно — но мог начать выкладывать недавно. Проверяем только маленькие каналы
            if video_count > 50:
                mark_seen(vid)  # большой старый канал — больше не проверяем это видео
                continue
            if age_checks >= MAX_AGE_CHECKS or time.time() - t0 > TIME_BUDGET_SEC:
                continue  # не успели проверить — не помечаем, вернёмся в следующий запуск
            first = first_video_date(c["contentDetails"]["relatedPlaylists"]["uploads"])
            age_checks += 1
            if not first:
                continue
            channel_age = (now - parse_dt(first)).days
            if channel_age > MAX_AGE_DAYS:
                mark_seen(vid)
                continue

        stats["young"] += 1
        candidates.append({
            "vid": vid, "cid": cid, "subs": subs, "channel_age": channel_age,
            "video_count": video_count,
        })

    # 4. Уведомления (самые просматриваемые первыми, не больше MAX_ALERTS за запуск)
    for cand in candidates[:MAX_ALERTS]:
        vid = cand["vid"]
        snippet = videos[vid]["snippet"]
        views = int(vdata[vid]["statistics"].get("viewCount", 0))
        video_days = max(1, (now - parse_dt(snippet["publishedAt"])).days)
        subs_text = "подписчики скрыты" if cand["subs"] is None else f"{cand['subs']} подписчиков"
        title = snippet.get("title", "?")
        channel_title = snippet.get("channelTitle", "?")
        url = f"https://www.youtube.com/watch?v={vid}"
        views_text = f"{views:,}".replace(",", " ")
        message = (
            f"🚀 Залетевшее видео у молодого канала!\n\n"
            f"Канал: {channel_title} (ведётся {cand['channel_age']} дн., {cand['video_count']} видео, "
            f"{subs_text})\n"
            f"https://www.youtube.com/channel/{cand['cid']}\n\n"
            f"Видео: {title}\n"
            f"Просмотров: {views_text} за {video_days} дн.\n{url}"
        )
        try:
            telegram_send_message(message)
        except Exception as e:
            errors.append(f"Telegram: {e}")
            break  # не помечаем как отправленное — повторим в следующий запуск
        mark_seen(vid)
        found.append({"title": title, "channel": channel_title, "views": views, "url": url})
    stats["alerts_sent"] = len(found)
    stats["waiting_for_next_run"] = max(0, len(candidates) - len(found))

    try:
        save_seen_ids(seen_list)
    except Exception as e:
        errors.append(f"Redis (запись): {e}")

    stats["seconds"] = round(time.time() - t0, 1)
    return {"found": found, "errors": sorted(set(errors)), "stats": stats,
            "redis_configured": bool(UPSTASH_URL and UPSTASH_TOKEN)}


@app.route("/", methods=["GET", "POST"])
@app.route("/api/monitor", methods=["GET", "POST"])
def monitor():
    secret = request.args.get("secret", "")
    if not MONITOR_SECRET or secret != MONITOR_SECRET:
        return jsonify({"error": "Неверный или отсутствующий secret"}), 401
    result = run_check()
    return jsonify(result)
