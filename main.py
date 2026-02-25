#!/usr/bin/env python3
"""
Moltbook Scraper - Hauptprogramm
Scrapes posts and comments from Moltbook social network
"""

import argparse
import asyncio
import sys
from pathlib import Path

from scraper import MoltbookScraper
from storage import DataStorage
from config import Config
from scraper import logger

async def main():
    """Hauptfunktion für Moltbook Scraping"""
    
    # ========================================
    # ARGUMENT PARSER
    # ========================================
    parser = argparse.ArgumentParser(
        description='Moltbook Post & Comment Scraper (Selenium + Shuffle)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  python main.py                          # Standard: 50 Shuffles
  python main.py --max-shuffles 100       # 100 Shuffles
  python main.py --max-posts 500          # Stoppe bei 500 Posts
  python main.py --delay 3.0              # 3 Sekunden Verzögerung
  python main.py --verbose                # Debug-Modus
        """
    )
    
    parser.add_argument(
        '--max-shuffles',
        type=int,
        default=Config.MAX_SHUFFLES,
        help=f'Maximale Anzahl Shuffles (Standard: {Config.MAX_SHUFFLES})'
    )
    
    parser.add_argument(
        '--max-posts',
        type=int,
        default=Config.MAX_POSTS,
        help=f'Maximale Anzahl Posts (Standard: {Config.MAX_POSTS if Config.MAX_POSTS else "unbegrenzt"})'
    )
    
    parser.add_argument(
        '--delay',
        type=float,
        default=Config.REQUEST_DELAY,
        help=f'Verzögerung zwischen Requests in Sekunden (Standard: {Config.REQUEST_DELAY})'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Debug-Modus aktivieren (mehr Logs)'
    )
    
    parser.add_argument(
        '--headless',
        action='store_true',
        default=Config.HEADLESS,
        help='Browser im Hintergrund ausführen'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data',
        help='Ausgabeverzeichnis für Daten (Standard: data/)'
    )
    
    args = parser.parse_args()
    
    # ========================================
    # 🔧 KONFIGURATION ÜBERSCHREIBEN
    # ========================================
    if args.max_shuffles:
        Config.MAX_SHUFFLES = args.max_shuffles
    if args.max_posts:
        Config.MAX_POSTS = args.max_posts
    if args.delay:
        Config.REQUEST_DELAY = args.delay
    if args.headless:
        Config.HEADLESS = args.headless
    
    # ========================================
    # LOGGING KONFIGURATION
    # ========================================
    if args.verbose:
        import logging
        logging.getLogger('scraper').setLevel(logging.DEBUG)
        logger.info("Debug-Modus aktiviert")
    
    # ========================================
    # AUSGABEVERZEICHNIS ERSTELLEN
    # ========================================
    output_path = Path(args.output_dir)
    output_path.mkdir(exist_ok=True)
    logger.info(f"Ausgabeverzeichnis: {output_path.absolute()}")
    
    # ========================================
    # SCRAPER STARTEN
    # ========================================
    logger.info("="*60)
    logger.info("MOLTBOOK SCRAPER GESTARTET")
    logger.info("="*60)
    logger.info(f"Base URL: {Config.BASE_URL}")
    logger.info(f"Max Shuffles: {Config.MAX_SHUFFLES}")
    logger.info(f"Max Posts: {Config.MAX_POSTS if Config.MAX_POSTS else 'unbegrenzt'}")
    logger.info(f"Verzögerung: {Config.REQUEST_DELAY}s")
    logger.info(f"Headless: {Config.HEADLESS}")
    logger.info("="*60)
    
    try:
        async with MoltbookScraper() as scraper:
            posts = await scraper.scrape_all_posts()
            
            if not posts:
                logger.warning("Keine Posts gefunden!")
                return 1
            
            logger.info(f"\n {len(posts)} Posts erfolgreich gescraped!")
            
            # ========================================
            # DATEN SPEICHERN IN CHROMA DB
            # ========================================
            logger.info("\n" + "="*60)
            logger.info("SPEICHERE DATEN IN CHROMA DB")
            logger.info("="*60)
            
            storage = DataStorage(db_path=output_path / "chroma_db")
            storage.save_posts(posts)
            
            stats = storage.get_statistics()
            
            print("\n" + "="*60)
            print("MOLTBOOK SCRAPING STATISTIKEN")
            print("="*60)
            print(f"Gesammelte Posts:       {stats['total_posts']:,}")
            print(f"Gesammelte Kommentare:  {stats['total_comments']:,}")
            print(f"Durchschnittl. Likes:  {stats['avg_likes']:.1f}")
            print(f"Durchschnittl. Kommentare: {stats['avg_comments']:.1f}")
            
            if stats.get('post_types'):
                print(f"\nPost-Typen:")
                for ptype, count in sorted(stats['post_types'].items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / stats['total_posts'] * 100) if stats['total_posts'] > 0 else 0
                    print(f"   {ptype:10s}: {count:4d} ({percentage:5.1f}%)")
            
            print("\nDaten erfolgreich in ChromaDB gespeichert!")
            print(f"ChromaDB Verzeichnis: {storage.db_path}")
            print("="*60)
            
            return 0
            
    except KeyboardInterrupt:
        logger.warning("\nScraping durch Benutzer abgebrochen (Ctrl+C)")
        return 130
        
    except Exception as e:
        logger.error(f"\nKritischer Fehler: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

def run():
    """Entry Point für das Programm"""
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nProgramm abgebrochen")
        sys.exit(130)

if __name__ == '__main__':
    run()
