# config.py

from pathlib import Path


class Config:
    """Konfiguration für Moltbook Scraper"""
    
    # ========================================
    # 🌐 WEBSITE KONFIGURATION
    # ========================================
    BASE_URL = "https://www.moltbook.com"
    
    # ========================================
    # 🎯 SELEKTOREN (CSS-Selektoren für HTML-Elemente)
    # ========================================
    
    # Einzelner Post
    POST_SELECTOR = "div[class*='post'], article, div.bg-\\[\\#1a1a1b\\]"
    
    # Shuffle-Button
    SHUFFLE_BUTTON = "button:has-text('Shuffle'), button.shuffle, #shuffle-btn"
    
    # ========================================
    # ⏱️ TIMING
    # ========================================
    REQUEST_TIMEOUT = 30  # Sekunden
    REQUEST_DELAY = 2.0   # Sekunden zwischen Requests
    SHUFFLE_WAIT = 2.0    # Sekunden nach Shuffle warten
    RATE_LIMIT_DELAY = 5.0  # Sekunden bei Rate Limit
    
    # ========================================
    # 🎯 SCRAPING LIMITS
    # ========================================
    MAX_SHUFFLES = 1     # Maximale Anzahl Shuffles
    MAX_POSTS = 20      # Maximale Anzahl Posts (None = unbegrenzt)
    SCRAPE_COMMENTS = True  # Kommentare scrapen?
    
    # ========================================
    # 🌐 HTTP KONFIGURATION
    # ========================================
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    
    # ========================================
    # 🖥️ BROWSER KONFIGURATION
    # ========================================
    HEADLESS = False  # Browser sichtbar (True = unsichtbar)
    
    # ========================================
    # 📊 LOGGING
    # ========================================
    LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
    
    # ========================================
    # 💾 DATENBANK
    # ========================================
    DATA_DIR = Path("data")
    DB_PATH = DATA_DIR / "moltbook.db"
    JSON_PATH = DATA_DIR / "moltbook_posts.json"
    CSV_DIR = DATA_DIR / "csv"


    @classmethod
    def setup_directories(cls):
        """Erstellt alle benötigten Verzeichnisse"""
        # Hauptverzeichnis
        cls.DATA_DIR.mkdir(exist_ok=True)
        
        # CSV-Verzeichnis
        cls.CSV_DIR.mkdir(exist_ok=True)
        
        # Logs-Verzeichnis (optional)
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
        return cls.DATA_DIR
