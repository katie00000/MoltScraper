import asyncio
import aiohttp
import random
import logging
import re
import hashlib

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, Browser, Playwright

from models import Post, Comment
from config import Config

logging.basicConfig(level=Config.LOG_LEVEL)
logger = logging.getLogger(__name__)


# =========================
# 🔒 GLOBALE RATE-LIMIT-KONTROLLEN
# =========================
HTTP_CONCURRENCY_LIMIT = getattr(Config, "HTTP_CONCURRENCY", 3)
HTTP_SEMAPHORE = asyncio.Semaphore(HTTP_CONCURRENCY_LIMIT)

BASE_BACKOFF = getattr(Config, "RATE_LIMIT_DELAY", 2.0)
MAX_BACKOFF = 60.0

FETCHED_URL_CACHE: Set[str] = set()


def jitter(base: float) -> float:
    return base + random.uniform(0.1, base * 0.3)


def rotate_headers(base_headers: dict) -> dict:
    headers = dict(base_headers)
    headers["Accept-Language"] = random.choice([
        "en-US,en;q=0.9",
        "en-GB,en;q=0.8",
        "de-DE,de;q=0.9,en;q=0.7",
    ])
    return headers


class MoltbookScraper:
    """Asynchroner Scraper mit Rate-Limit-Resilienz"""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.headers = {
            "User-Agent": Config.USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }

        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None

        self.seen_posts: Set[str] = set()

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit_per_host=HTTP_CONCURRENCY_LIMIT,
            enable_cleanup_closed=True,
            ttl_dns_cache=300,
            ssl=False,
        )

        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT),
            headers=self.headers,
        )

        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=Config.HEADLESS,
            args=["--disable-blink-features=AutomationControlled"],
        )

        context = await self.browser.new_context(
            user_agent=Config.USER_AGENT,
            viewport={"width": 1280, "height": 800},
        )

        self.page = await context.new_page()
        return self

    async def __aexit__(self, *args):
        if self.page:
            await self.page.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        if self.session:
            await self.session.close()

    # =========================
    # 🌐 HTTP FETCH MIT BACKOFF
    # =========================
    async def _fetch_page(self, url: str, max_retries: int = 5) -> Optional[str]:
        if not self.session:
            return None

        if url in FETCHED_URL_CACHE:
            logger.debug(f"♻️ Cache-Hit: {url}")
            return None

        async with HTTP_SEMAPHORE:
            backoff = BASE_BACKOFF

            for attempt in range(1, max_retries + 1):
                try:
                    headers = rotate_headers(self.headers)
                    async with self.session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            html = await resp.text()
                            FETCHED_URL_CACHE.add(url)
                            return html

                        if resp.status == 429:
                            retry_after = resp.headers.get("Retry-After")
                            wait = float(retry_after) if retry_after else backoff
                            wait = min(wait, MAX_BACKOFF)
                            logger.warning(f"⏳ 429 → warte {wait:.1f}s")
                            await asyncio.sleep(jitter(wait))
                            backoff *= 2
                            continue

                        logger.warning(f"⚠️ HTTP {resp.status} für {url}")
                        return None

                except asyncio.TimeoutError:
                    await asyncio.sleep(jitter(backoff))
                    backoff *= 2
                except aiohttp.ClientError as e:
                    logger.debug(f"HTTP-Fehler: {e}")
                    await asyncio.sleep(jitter(backoff))
                    backoff *= 2

        return None

    # =========================
    # 🧭 PLAYWRIGHT
    # =========================
    async def load_moltbook(self):
        if not self.page:
            raise RuntimeError("Browser nicht initialisiert")

        await self.page.goto(
            Config.BASE_URL,
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await asyncio.sleep(jitter(1.5))

    async def get_current_posts(self) -> List[Dict[str, str]]:
        if not self.page:
            return []

        html = await self.page.content()
        soup = BeautifulSoup(html, "html.parser")

        fadein_divs = soup.find_all("div", class_="animate-fadeIn")
        if not fadein_divs:
            return []

        posts_data = []
        for div in fadein_divs:
            # Titel und Text
            title_tag = div.find("h3")
            text_tag = div.find("p")
            title = title_tag.get_text(strip=True, separator=" ") if title_tag else ""
            text = text_tag.get_text(strip=True, separator=" ") if text_tag else ""

            # URL extrahieren
            url = self._extract_post_url(div) or ""

            # Username und relative Zeit extrahieren
            author, time_number, time_letter = self._extract_user_and_relative_time(div)

            posts_data.append({
                "title": title,
                "text": text,
                "url": url,
                "author": author,
                "time_number": time_number,
                "time_letter": time_letter
            })

        return posts_data


    # =========================
    # 🔧 HILFSMETHODEN (UNVERÄNDERT)
    # =========================
    def _extract_user_and_relative_time(self, post_soup: BeautifulSoup) -> Tuple[str, Optional[str], Optional[str]]:
        """
        Extrahiert den Username (u/...), die Zahl und den Buchstaben aus der Zeitangabe (z.B. 19d ago)
        """
        try:
            info_div = post_soup.find("div", class_="flex-1 min-w-0")
            print("INFO SOUP: ", info_div)
            if not info_div:
                return "Unknown", None, None

            spans = info_div.find_all("span")

            username = None
            time_number = None
            time_letter = None

            for span in spans:
                text = span.get_text(strip=True)

                # Username extrahieren
                if "u/" in text:
                    username = text

                # Zeitangabe extrahieren (z.B. "19d ago")
                if "ago" in text.lower():
                    parts = text.split(" ")
                    if len(parts) == 2:
                        time_number = ''.join(filter(str.isdigit, parts[0]))
                        time_letter = ''.join(filter(str.isalpha, parts[0]))

            if username is None:
                username = "Unknown"

            return username, time_number, time_letter

        except Exception:
            return "Unknown", None, None


    def _extract_post_url(self, soup: BeautifulSoup) -> Optional[str]:
        try:
            title_link = soup.select_one('h3 a[href*="/post/"]')
            if title_link and title_link.get('href'):
                href = title_link['href']
                if href.startswith('/'):
                    return f"{Config.BASE_URL}{href}"
                return href

            post_link = soup.select_one('a[href*="/post/"]')
            if post_link and post_link.get('href'):
                href = post_link['href']
                if href.startswith('/'):
                    return f"{Config.BASE_URL}{href}"
                return href

            parent_link = soup.find_parent('a', href=True)
            if parent_link and '/post/' in parent_link.get('href', ''):
                href = parent_link['href']
                if href.startswith('/'):
                    return f"{Config.BASE_URL}{href}"
                return href

            return None

        except Exception as e:
            logger.debug(f"Fehler bei _extract_post_url: {e}")
            return None

    def _extract_timestamp(self, time_number: Optional[str], time_letter: Optional[str]) -> Tuple[Optional[datetime], str, str]:
        """
        Berechnet den Timestamp basierend auf Zahl und Buchstabe (z.B. 19d -> 19 Tage)
        """
        if not time_number or not time_letter:
            return None, "unknown", ""

        try:
            amount = int(time_number)
            unit = time_letter.lower()
            now = datetime.now()

            if unit == "s":
                return now - timedelta(seconds=amount), "seconds", f"{amount}{unit} ago"
            if unit == "m":
                return now - timedelta(minutes=amount), "minutes", f"{amount}{unit} ago"
            if unit == "h":
                return now - timedelta(hours=amount), "hours", f"{amount}{unit} ago"
            if unit == "d":
                return now - timedelta(days=amount), "days", f"{amount}{unit} ago"
            if unit == "w":
                return now - timedelta(weeks=amount), "weeks", f"{amount}{unit} ago"
            if unit == "y":
                return now - timedelta(days=amount*365), "years", f"{amount}{unit} ago"

            return None, "unknown", f"{amount}{unit} ago"

        except Exception:
            return None, "unknown", f"{time_number}{time_letter} ago"


    def _extract_likes(self, soup: BeautifulSoup) -> int:
        try:
            for span in soup.select("span.font-bold"):
                text = span.get_text(strip=True)
                if text.isdigit():
                    return int(text)
            return 0
        except Exception:
            return 0

    def _detect_post_type(self, soup: BeautifulSoup) -> str:
        if soup.select_one("img"):
            return "image"
        if soup.select_one("video"):
            return "video"
        return "text"

    def _extract_media_urls(self, soup: BeautifulSoup) -> List[str]:
        urls = []
        for img in soup.select("img[src]"):
            urls.append(img["src"])
        for src in soup.select("video source[src]"):
            urls.append(src["src"])
        return urls

    def _extract_hashtags(self, text: str) -> List[str]:
        return re.findall(r"#(\w+)", text or "")

    def _extract_mentions(self, text: str) -> List[str]:
        return re.findall(r"@(\w+)", text or "")

    async def _parse_comments_from_detail(
        self, detail_soup: BeautifulSoup
    ) -> Tuple[List[Comment], int]:
        comments: List[Comment] = []
        count = 0

        try:
            header = detail_soup.select_one("h2")
            if header:
                m = re.search(r"\((\d+)\)", header.get_text())
                if m:
                    count = int(m.group(1))

            blocks = detail_soup.select("div.rounded-lg.p-4")
            for idx, block in enumerate(blocks, 1):
                author = "Unknown"
                author_link = block.select_one('a[href^="/u/"]')
                if author_link:
                    author = author_link.get_text(strip=True).replace("u/", "")

                content = ""
                p = block.select_one("div.prose p, div.text-sm p")
                if p:
                    content = p.get_text(strip=True)

                raw_time = ""
                timestamp = None
                precision = "unknown"
                for span in block.select("span"):
                    if "ago" in span.get_text(strip=True).lower():
                        raw_time = span.get_text(strip=True)
                        timestamp, precision, _ = self._parse_relative_time(raw_time)
                        break

                comment_id = hashlib.md5(
                    f"{author}{content}{idx}".encode()
                ).hexdigest()[:16]

                comments.append(
                    Comment(
                        comment_id=comment_id,
                        author=author,
                        content=content,
                        timestamp=timestamp,
                        timestamp_precision=precision,
                        timestamp_raw=raw_time,
                        likes=0,
                    )
                )

            return comments, count
        except Exception:
            return [], 0

    async def click_shuffle(self) -> bool: 
        """Shuffle-Button klicken""" 
        if not self.page: 
            return False 
        
        try: 
            shuffle_button = await self.page.query_selector('button:has-text("Shuffle")') 
            
            if not shuffle_button: 
                logger.warning("⚠️ Shuffle-Button nicht gefunden") 
                return False 
            
            is_disabled = await shuffle_button.get_attribute('disabled') 
            if is_disabled: 
                logger.warning("⚠️ Shuffle-Button ist deaktiviert") 
                return False 
            
            logger.info("🎲 Klicke Shuffle...") 
            await shuffle_button.click() 

            await asyncio.sleep(Config.SHUFFLE_WAIT if hasattr(Config, 'SHUFFLE_WAIT') 
            else 2.0) 
            await self.page.wait_for_selector(Config.POST_SELECTOR, state='visible') 

            return True 
        
        except Exception as e: 
            logger.error(f"❌ Shuffle-Fehler: {e}") 
            return False

    # ⚠️ WICHTIG:
    # Entferne die doppelte _generate_post_id-Definition
    # und verwende NUR diese:

    def _generate_post_id(
        self,
        author: str,
        timestamp: Optional[datetime],
        title: str,
        content: str,
        url: str,
    ) -> str:
        base = f"{author}{timestamp}{title}{content[:80]}{url}"
        return hashlib.md5(base.encode()).hexdigest()[:16]


    async def wait_for_posts(self, timeout=5.0):
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < timeout:
            posts = await self.get_current_posts()
            if posts:
                return posts
            await asyncio.sleep(0.5)
        return []

    async def parse_post_data(self, post_data: Dict[str, str], post_index: int) -> Optional[Post]:
        """
        Baut ein Post-Objekt aus einem Dict von get_current_posts().
        Nutzt Detailseite, wenn URL vorhanden, sonst Dummy-Daten.
        """
        title = post_data.get("title", "")
        content = post_data.get("text", "")
        post_url = post_data.get("url", "")
        author = post_data.get("author", "")
        if not title and not content:
            return None

        time_number = post_data.get("time_number")
        time_letter = post_data.get("time_letter")
        timestamp, precision, raw_time = self._extract_timestamp(time_number, time_letter)
        
        # Wenn URL vorhanden, lade Detailseite
        comments, comments_count = [], 0
        if post_url:
            html = await self._fetch_page(post_url)
            if html:
                detail = BeautifulSoup(html, "lxml")
                comments, comments_count = await self._parse_comments_from_detail(detail)

        # Likes Fallback: suche span.font-bold in Detailseite oder Dict (wenn vorhanden)
        likes = post_data.get("likes")
        if likes is None and post_url and html:
            likes = self._extract_likes(detail)
        if likes is None:
            likes = 0

        post_id = self._generate_post_id(author, timestamp or datetime.now(), title, content, post_url or "")

        return Post(
            post_id=post_id,
            author=author or "Unknown",
            author_id=post_data.get("author_id", ""),
            title=title,
            content=content,
            timestamp=timestamp or datetime.now(),
            timestamp_precision=precision or "seconds",
            timestamp_raw=raw_time or str(datetime.now()),
            likes=likes,
            comments_count=comments_count,
            comments=comments,
            post_type="text",
            media_urls=[],  # Optional: falls du Media aus Dict oder Detail extrahieren willst, hier ergänzen
            hashtags=self._extract_hashtags(content),
            mentions=self._extract_mentions(content),
            url=post_url or "",
            scraped_at=datetime.now(),
        )


    # =========================
    # 🔁 SCRAPE-LOOP
    # =========================
    async def scrape_all_posts(self) -> List[Post]:
        await self.load_moltbook()
        all_posts: List[Post] = []

        for shuffle in range(Config.MAX_SHUFFLES):
            posts_data = await self.wait_for_posts()
            if not posts_data:
                logger.warning(f"⚠️ Keine Posts gefunden beim Shuffle {shuffle + 1}/{Config.MAX_SHUFFLES}")
                await asyncio.sleep(jitter(1.0))
                await self.click_shuffle()
                continue

            random.shuffle(posts_data)

            for i, post_data in enumerate(posts_data):
                post = await self.parse_post_data(post_data, len(all_posts) + 1)
                if post:
                    all_posts.append(post)

                if len(all_posts) >= Config.MAX_POSTS:
                    logger.info(f"✅ Maximale Anzahl von Posts ({Config.MAX_POSTS}) erreicht.")
                    return all_posts

                await asyncio.sleep(jitter(Config.REQUEST_DELAY))

            await asyncio.sleep(jitter(2.0))
            await self.click_shuffle()

        return all_posts
