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

# ============================================================
# GLOBALE KONFIGURATION FÜR RATE LIMITING UND FEHLERTOLERANZ
# ============================================================

# Maximale Anzahl paralleler HTTP-Anfragen
HTTP_CONCURRENCY_LIMIT = getattr(Config, "HTTP_CONCURRENCY", 3)

# Semaphore zur Durchsetzung der maximalen Parallelität
HTTP_SEMAPHORE = asyncio.Semaphore(HTTP_CONCURRENCY_LIMIT)

# Basiswert für exponentielles Backoff bei Netzwerkproblemen
BASE_BACKOFF = getattr(Config, "RATE_LIMIT_DELAY", 2.0)

# Obergrenze für Backoff-Zeiten
MAX_BACKOFF = 60.0

# Cache zur Vermeidung mehrfacher Requests derselben URL
FETCHED_URL_CACHE: Set[str] = set()


def jitter(base: float) -> float:
    """
    Fügt eine zufällige zeitliche Abweichung hinzu, um deterministische
    Request-Muster zu vermeiden (Anti-Bot-Maßnahme).
    """
    return base + random.uniform(0.1, base * 0.3)


def rotate_headers(base_headers: dict) -> dict:
    """
    Variiert HTTP-Header (insbesondere Accept-Language), um Requests
    realistischer wirken zu lassen.
    """
    headers = dict(base_headers)
    headers["Accept-Language"] = random.choice([
        "en-US,en;q=0.9",
        "en-GB,en;q=0.8",
        "de-DE,de;q=0.9,en;q=0.7",
    ])
    return headers


class MoltbookScraper:
    """
    Asynchroner Webscraper für Moltbook mit Playwright-Unterstützung,
    Rate-Limit-Resilienz und strukturierter Datenausgabe.
    """

    def __init__(self):
        # HTTP-Session für klassische Requests
        self.session: Optional[aiohttp.ClientSession] = None

        # Standard-Header für alle Requests
        self.headers = {
            "User-Agent": Config.USER_AGENT,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,*/*;q=0.8"
            ),
            "Connection": "keep-alive",
        }

        # Playwright-Komponenten
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None

        # Cache zur Vermeidung doppelter Post-Verarbeitung
        self.seen_posts: Set[str] = set()

    async def __aenter__(self):
        """
        Initialisiert HTTP-Session und Playwright-Browser.
        """
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
        """
        Gibt alle externen Ressourcen deterministisch frei.
        """
        if self.page:
            await self.page.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        if self.session:
            await self.session.close()

    # ============================================================
    # DETAILSEITENABRUF MIT PLAYWRIGHT
    # ============================================================

    async def _fetch_page_browser(self, url: str) -> Optional[str]:
        """
        Lädt eine Detailseite mittels Playwright, um clientseitig
        gerenderte Inhalte (z. B. Kommentare) zuverlässig zu erfassen.
        """
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=rotate_headers(self.headers).get(
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                )
            )

            page = await context.new_page()
            logger.debug(f"Playwright GET {url}")

            await page.goto(url, wait_until="networkidle")

            # Explizites Warten auf Kommentar-Container
            try:
                await page.wait_for_selector("div.py-2", timeout=10_000)
                logger.debug("Kommentare im DOM detektiert")
            except Exception:
                logger.warning("Keine Kommentare im DOM gefunden")

            # Scrollen zur Auslösung von Lazy Loading
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1500)

            html = await page.content()
            await browser.close()
            return html

    # ============================================================
    # PLAYWRIGHT-HAUPTSEITE
    # ============================================================

    async def load_moltbook(self):
        """
        Öffnet die Startseite von Moltbook und wartet robust
        bis Posts wirklich im DOM vorhanden sind (max. 10s).
        Behandelt DNS-Fehler, Timeouts und dynamische Inhalte.
        """
        if not self.page:
            raise RuntimeError("Browser nicht initialisiert")

        logger.info("Lade Moltbook Startseite...")

        from playwright._impl._errors import Error as PlaywrightError, TimeoutError

        # --- 1) Seite mit Retry auf DNS-Probleme laden ---
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self.page.goto(
                    Config.BASE_URL,
                    wait_until="domcontentloaded",  # robuster als networkidle
                    timeout=30_000  # 30 Sekunden
                )
                break
            except PlaywrightError as e:
                if "net::ERR_NAME_NOT_RESOLVED" in str(e):
                    logger.warning(f"DNS-Fehler beim Laden der Seite (Versuch {attempt+1}/{max_retries})")
                    await asyncio.sleep(2)
                else:
                    raise
        else:
            logger.error("Seite konnte nach mehreren Versuchen nicht geladen werden")
            return

        # --- 2) Auf bekannte Post-Struktur warten ---
        try:
            await self.page.wait_for_selector(
                Config.POST_SELECTOR,
                state="visible",
                timeout=10_000,
            )
            logger.debug("Post-Selector sichtbar")
            return
        except TimeoutError:
            logger.warning("Post-Selector nicht rechtzeitig sichtbar – Fallback-Polling")

        # --- 3) Fallback: Polling über HTML ---
        await self._wait_for_posts()

    async def _wait_for_posts(self, timeout: float = 10.0, interval: float = 0.5):
        """
        Polling auf DOM-Elemente für Posts.
        Prüft alle `interval` Sekunden bis `timeout` Sekunden.
        """
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < timeout:
            html = await self.page.content()
            soup = BeautifulSoup(html, "html.parser")

            # bekannte Post-Klasse prüfen
            if soup.find("div", class_="animate-fadeIn"):
                logger.debug("Posts via Fallback-Polling erkannt")
                return

            await asyncio.sleep(interval)

        logger.error(f"Nach {timeout} Sekunden keine Posts gefunden")

    async def get_current_posts(self) -> List[Dict[str, str]]:
        """
        Extrahiert alle aktuell sichtbaren Posts aus dem DOM
        der Startseite.
        """
        if not self.page:
            return []

        html = await self.page.content()
        soup = BeautifulSoup(html, "html.parser")

        fadein_divs = soup.find_all("div", class_="animate-fadeIn")
        if not fadein_divs:
            return []

        posts_data = []
        for div in fadein_divs:
            title_tag = div.find("h3")
            text_tag = div.find("p")

            title = title_tag.get_text(strip=True, separator=" ") if title_tag else ""
            text = text_tag.get_text(strip=True, separator=" ") if text_tag else ""

            url = self._extract_post_url(div)
            submolt, author, time_number, time_letter = self.extract_post_metadata(div)

            comment_count = None
            for span in div.find_all("span"):
                if span.get_text(strip=True) == "comments":
                    prev = span.find_previous_sibling("span")
                    if prev and prev.get_text(strip=True).isdigit():
                        comment_count = int(prev.get_text(strip=True))
                    break

            posts_data.append({
                "title": title,
                "text": text,
                "url": url or "",
                "author": author,
                "submolt": submolt,
                "time_number": time_number,
                "time_letter": time_letter,
                "comment_count": comment_count
            })

        return posts_data

    # ============================================================
    # HILFSMETHODEN ZUR DATENEXTRAKTION
    # ============================================================

    def extract_post_metadata(
        self, post_soup: BeautifulSoup
    ) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
        """
        Extrahiert:
        - SubMolt / Subforum (z.B. 'm/general')
        - Username (z.B. 'u/easymoneysniper')
        - Relative Zeitangabe (z. B. '19d ago' -> 19, d)
        """
        try:
            info_div = post_soup.find("div", class_="flex-1 min-w-0")
            if not info_div:
                return None, "Unknown", None, None

            submolt = None
            username = "Unknown"
            time_number = None
            time_letter = None

            # Alle Links im info_div
            links = info_div.find_all("a")
            if links:
                # SubMolt: erster Link
                submolt = links[0].get_text(strip=True)
                # Username: zweiter Link (falls vorhanden)
                if len(links) > 1:
                    username = links[1].get_text(strip=True)
                    if username.startswith("u/"):
                        username = username[2:]

            # Zeitangabe: letztes span mit 'ago'
            for span in info_div.find_all("span"):
                text = span.get_text(strip=True)
                if "ago" in text.lower():
                    parts = text.split()
                    if parts:
                        time_number = "".join(filter(str.isdigit, parts[0]))
                        time_letter = "".join(filter(str.isalpha, parts[0]))
                    break

            return submolt, username, time_number, time_letter

        except Exception:
            return None, "Unknown", None, None

    def _extract_post_url(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Versucht die Post-URL über mehrere robuste Selektoren zu ermitteln.
        """
        try:
            for selector in [
                'h3 a[href*="/post/"]',
                'a[href*="/post/"]'
            ]:
                link = soup.select_one(selector)
                if link and link.get("href"):
                    href = link["href"]
                    return f"{Config.BASE_URL}{href}" if href.startswith("/") else href
            return None
        except Exception as e:
            logger.debug(f"URL-Extraktion fehlgeschlagen: {e}")
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
                dt = now - timedelta(seconds=amount)
                dt = dt.replace(microsecond=0)
                return dt, "seconds", f"{amount}{unit} ago"
            if unit == "m":
                dt = now - timedelta(minutes=amount)
                dt = dt.replace(second=0, microsecond=0)
                return dt, "minutes", f"{amount}{unit} ago"
            if unit == "h":
                dt = now - timedelta(hours=amount)
                dt = dt.replace(minute=0,second=0, microsecond=0)
                return dt, "hours", f"{amount}{unit} ago"
            if unit == "d":
                dt = now - timedelta(days=amount)
                dt = dt.replace(hour=0,minute=0,second=0, microsecond=0)
                return dt, "days", f"{amount}{unit} ago"
            if unit == "w":
                dt = dt.replace(day=0,hour=0,minute=0,second=0, microsecond=0)
                dt = now - timedelta(weeks=amount)
                return dt, "weeks", f"{amount}{unit} ago"
            if unit == "y":
                dt = now - timedelta(days=amount*365)
                dt = dt.replace(day=0,hour=0,minute=0,second=0, microsecond=0)
                return dt, "years", f"{amount}{unit} ago"

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


    def _parse_relative_time(self, relative_time: str) -> Tuple[Optional[datetime], str, str]:
        """
        Wandelt relative Zeitangaben (z.B. '18d ago', '5h ago') in datetime um.
        Gibt zurück: (timestamp, precision, raw_time)
        """
        raw = relative_time.strip()
        now = datetime.now()

        try:
            match = re.search(r"(\d+)\s*([smhdwy])", raw.lower())
            if not match:
                return None, "unknown", raw

            amount = int(match.group(1))
            unit = match.group(2)

            if unit == "s":
                dt = now - timedelta(seconds=amount)
                dt = dt.replace(microsecond=0)
                precision = "seconds"
            elif unit == "m":
                dt = now - timedelta(minutes=amount)
                dt = dt.replace(second=0, microsecond=0)
                precision = "minutes"
            elif unit == "h":
                dt = now - timedelta(hours=amount)
                dt = dt.replace(minute=0, second=0, microsecond=0)
                precision = "hours"
            elif unit == "d":
                dt = now - timedelta(days=amount)
                dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                precision = "days"
            elif unit == "w":
                dt = now - timedelta(weeks=amount)
                dt = dt.replace(day=0, hour=0, minute=0, second=0, microsecond=0)
                precision = "weeks"
            elif unit == "y":
                dt = now - timedelta(days=amount*365)
                dt = dt.replace(day=0, hour=0, minute=0, second=0, microsecond=0)
                precision = "years"
            else:
                return None, "unknown", raw

            return dt, precision, raw

        except Exception:
            return None, "unknown", raw


    async def _parse_comments_from_detail(self, detail_soup: BeautifulSoup) -> Tuple[List[Comment], int]:
        comments: List[Comment] = []

        # Kommentarblöcke
        blocks = detail_soup.select("div.py-2")
        for idx, block in enumerate(blocks, 1):

            # META (Author + Zeit)
            meta_div = block.select_one("div[class*='items-center'][class*='gap-2']")

            author = "Unknown"
            raw_time = ""

            if meta_div:
                author_link = meta_div.select_one("a[href^='/u/']")

                if author_link:
                    author = author_link.get_text(strip=True).replace("u/", "")

                spans = meta_div.find_all("span")

                for span in spans:
                    if re.search(r"\d+[smhd]", span.get_text()):
                        raw_time = span.get_text(strip=True)
                        break


            timestamp, precision, parsed_raw_time = self._parse_relative_time(raw_time)

            # CONTENT
            content_div = block.select_one("div.prose")

            content = ""
            if content_div:
                paragraphs = content_div.find_all("p")
                content = " ".join(p.get_text(strip=True) for p in paragraphs)


            if not content and author == "Unknown":
                print("DEBUG: BLOCK WIRD ÜBERSPRUNGEN")
                continue

            comment_id = hashlib.md5(f"{author}{content}{idx}".encode()).hexdigest()[:16]

            #LIKES
            likes = -1

            likes_span = block.select_one("span.flex.items-center.gap-1")
            if likes_span:
                # Alle Zahlen im Text extrahieren
                text = likes_span.get_text(strip=True)
                match = re.search(r"\d+", text)
                if match:
                    likes = int(match.group())


            comments.append(Comment(
                comment_id=comment_id,
                author=author,
                content=content,
                timestamp=timestamp,
                timestamp_precision=precision,
                timestamp_raw=parsed_raw_time,
                likes=likes
            ))

        print(f"\n=== DEBUG: PARSED COMMENTS = {len(comments)} ===")
        return comments, len(comments)



    async def click_shuffle(self) -> bool: 
        """Shuffle-Button klicken""" 
        if not self.page: 
            return False 
        
        try: 
            shuffle_button = await self.page.query_selector('button:has-text("Shuffle")') 
            
            if not shuffle_button: 
                logger.warning("Shuffle-Button nicht gefunden") 
                return False 
            
            is_disabled = await shuffle_button.get_attribute('disabled') 
            if is_disabled: 
                logger.warning("Shuffle-Button ist deaktiviert") 
                return False 
            
            logger.info("Klicke Shuffle...") 
            await shuffle_button.click() 

            await asyncio.sleep(Config.SHUFFLE_WAIT if hasattr(Config, 'SHUFFLE_WAIT') 
            else 2.0) 
            await self.page.wait_for_selector(Config.POST_SELECTOR, state='visible') 

            return True 
        
        except Exception as e: 
            logger.error(f"Shuffle-Fehler: {e}") 
            return False

    def _generate_post_id(
        self,
        submolt: str,
        author: str,
        timestamp: Optional[datetime],
        title: str,
        content: str,
        url: str,
    ) -> str:
        base = f"{author}{submolt}{timestamp}{title}{content[:80]}{url}"
        return hashlib.md5(base.encode()).hexdigest()[:16]


    async def wait_for_posts(self, timeout=60.0):
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
        url = post_data.get("url", "")
        submolt = post_data.get("submolt", "")
        author = post_data.get("author", "Unknown")
        if not title and not content:
            return None

        total_comments_count = post_data.get("comment_count", "0")
        time_number = post_data.get("time_number")
        time_letter = post_data.get("time_letter")
        timestamp, precision, timestamp_raw = self._extract_timestamp(time_number, time_letter)

        
        if url in FETCHED_URL_CACHE:
            print("INFO: Post schon gesehen - ", url)
            return None
        
        print("Neuer Post - ", url)
        FETCHED_URL_CACHE.add(url)

        html = await self._fetch_page_browser(url)
        comments: List[Comment] = []
        likes: int = 0
        if html:
            detail_soup = BeautifulSoup(html, "html.parser")
            likes = self._extract_likes(detail_soup)
            comments, comments_count = await self._parse_comments_from_detail(detail_soup)
        else:
            comments_count = 0

        post_id = self._generate_post_id(author, submolt, timestamp, title, content, url)

        return Post(
            post_id=post_id,
            author=author or "Unknown",
            author_id=post_data.get("author_id", ""),
            submolt = submolt,
            title=title,
            content=content,
            timestamp=timestamp or datetime.now(),
            timestamp_precision=precision or "seconds",
            timestamp_raw=timestamp_raw or str(datetime.now()),
            likes=likes,
            comments_count=comments_count,
            total_comments_count=total_comments_count,
            comments=comments,
            post_type="text",
            media_urls=[],  # TODO
            hashtags=self._extract_hashtags(content),
            mentions=self._extract_mentions(content),
            url=url or "",
            scraped_at=datetime.now(),
        )


    # =========================
    # SCRAPE-LOOP
    # =========================
    async def scrape_all_posts(self) -> List[Post]:
        await self.load_moltbook()
        all_posts: List[Post] = []

        for shuffle in range(Config.MAX_SHUFFLES):
            posts_data = await self.wait_for_posts()
            if not posts_data:
                logger.warning(f"Keine Posts gefunden beim Shuffle {shuffle + 1}/{Config.MAX_SHUFFLES}")
                await asyncio.sleep(jitter(1.0))
                await self.click_shuffle()
                continue

            random.shuffle(posts_data)

            for i, post_data in enumerate(posts_data):
                post = await self.parse_post_data(post_data, len(all_posts) + 1)
                if post:
                    if post.post_id in self.seen_posts:
                        logger.debug(f"Duplikat-Post übersprungen: {post.post_id}")
                        continue
                    self.seen_posts.add(post.post_id)
                    all_posts.append(post)

                if len(all_posts) >= Config.MAX_POSTS:
                    logger.info(f"Maximale Anzahl von Posts ({Config.MAX_POSTS}) erreicht.")
                    return all_posts

                await asyncio.sleep(jitter(Config.REQUEST_DELAY))

            await asyncio.sleep(jitter(2.0))
            await self.click_shuffle()

        return all_posts
