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
    # 🔧 ARGUMENT PARSER
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
  python main.py --export-csv             # Zusätzlich CSV exportieren
  python main.py --verbose                # Debug-Modus
        """
    )
    
    parser.add_argument(
        '--max-shuffles',
        type=int,
        default=Config.MAX_SHUFFLES,  # ← Korrigiert!
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
        '--export-csv',
        action='store_true',
        help='Daten zusätzlich als CSV exportieren'
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
    # 🔧 LOGGING KONFIGURATION
    # ========================================
    if args.verbose:
        import logging
        logging.getLogger('scraper').setLevel(logging.DEBUG)
        logger.info("🐛 Debug-Modus aktiviert")
    
    # ========================================
    # 📁 AUSGABEVERZEICHNIS ERSTELLEN
    # ========================================
    output_path = Path(args.output_dir)
    output_path.mkdir(exist_ok=True)
    logger.info(f"📁 Ausgabeverzeichnis: {output_path.absolute()}")
    
    # ========================================
    # 🚀 SCRAPER STARTEN
    # ========================================
    logger.info("="*60)
    logger.info("🚀 MOLTBOOK SCRAPER GESTARTET")
    logger.info("="*60)
    logger.info(f"🌐 Base URL: {Config.BASE_URL}")
    logger.info(f"🔄 Max Shuffles: {Config.MAX_SHUFFLES}")
    logger.info(f"📊 Max Posts: {Config.MAX_POSTS if Config.MAX_POSTS else 'unbegrenzt'}")
    logger.info(f"⏱️  Verzögerung: {Config.REQUEST_DELAY}s")
    logger.info(f"👁️  Headless: {Config.HEADLESS}")
    logger.info("="*60)
    
    try:
        # ========================================
        # 🕷️ SCRAPING DURCHFÜHREN
        # ========================================
        async with MoltbookScraper() as scraper:
            # Verwende scrape_all_posts() direkt
            posts = await scraper.scrape_all_posts()
            
            if not posts:
                logger.warning("⚠️ Keine Posts gefunden!")
                logger.info("\n💡 Mögliche Gründe:")
                logger.info("   • Website ist nicht erreichbar")
                logger.info("   • HTML-Struktur hat sich geändert")
                logger.info("   • Selektoren müssen angepasst werden")
                return 1
            
            logger.info(f"\n✅ {len(posts)} Posts erfolgreich gescraped!")
            
            # ========================================
            # 💾 DATEN SPEICHERN
            # ========================================
            logger.info("\n" + "="*60)
            logger.info("💾 SPEICHERE DATEN")
            logger.info("="*60)
            
            storage = DataStorage()
            
            # JSON speichern
            json_path = storage.save_to_json(posts)
            logger.info(f"✅ JSON gespeichert: {json_path}")
            
            # SQLite speichern
            db_path = storage.save_to_sqlite(posts)
            logger.info(f"✅ SQLite gespeichert: {db_path}")
            
            # Optional: CSV exportieren
            if args.export_csv:
                csv_path = storage.export_to_csv()
                logger.info(f"✅ CSV exportiert: {csv_path}")
            
            # ========================================
            # 📊 STATISTIKEN ANZEIGEN
            # ========================================
            stats = storage.get_statistics()
            
            print("\n" + "="*60)
            print("📊 MOLTBOOK SCRAPING STATISTIKEN")
            print("="*60)
            print(f"✅ Gesammelte Posts:       {stats['total_posts']:,}")
            print(f"💬 Gesammelte Kommentare:  {stats['total_comments']:,}")
            print(f"❤️  Durchschnittl. Likes:  {stats['avg_likes']:.1f}")
            print(f"💬 Durchschnittl. Kommentare: {stats['avg_comments']:.1f}")
            
            # Post-Typen
            if stats.get('post_types'):
                print(f"\n📝 Post-Typen:")
                for ptype, count in sorted(stats['post_types'].items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / stats['total_posts'] * 100) if stats['total_posts'] > 0 else 0
                    print(f"   {ptype:10s}: {count:4d} ({percentage:5.1f}%)")
            
            # Top Autoren
            if stats.get('top_authors'):
                print(f"\n👥 Top 10 Autoren:")
                for i, (author, count) in enumerate(stats['top_authors'][:10], 1):
                    display_name = f"u/{author}" if not author.startswith('u/') else author
                    print(f"   {i:2d}. {display_name:20s}: {count:3d} Posts")
            
            # Top Hashtags
            if stats.get('top_hashtags'):
                print(f"\n🏷️  Top 10 Hashtags:")
                for i, (tag, count) in enumerate(stats['top_hashtags'][:10], 1):
                    print(f"   {i:2d}. #{tag:20s}: {count:3d}x")
            
            print("="*60)
            
            # ========================================
            # 📁 DATEIPFADE ANZEIGEN
            # ========================================
            print(f"\n📁 Gespeicherte Dateien:")
            print(f"   JSON:   {json_path}")
            print(f"   SQLite: {db_path}")
            if args.export_csv:
                print(f"   CSV:    {csv_path}")
            print("="*60)
            
            logger.info("\n✅ Scraping erfolgreich abgeschlossen!")
            return 0
            
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Scraping durch Benutzer abgebrochen (Ctrl+C)")
        return 130
        
    except Exception as e:
        logger.error(f"\n❌ Kritischer Fehler: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

def run():
    """Entry Point für das Programm"""
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⚠️ Programm abgebrochen")
        sys.exit(130)

if __name__ == '__main__':
    run()
