"""
Database Manager: SQLite with progress tracking and resume capability
"""

import sqlite3
import json
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Advanced database management for scraping progress and data storage"""
    
    def __init__(self, db_path: str = 'data/linkedin_scraper.db'):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Initialize database with advanced schema"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Profiles table with tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_url TEXT UNIQUE NOT NULL,
                profile_hash TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'pending',
                data TEXT,
                error TEXT,
                retry_count INTEGER DEFAULT 0,
                scraped_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_completeness REAL DEFAULT 0
            )
        ''')
        
        # Search sessions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT NOT NULL,
                total_profiles INTEGER DEFAULT 0,
                scraped_profiles INTEGER DEFAULT 0,
                failed_profiles INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
        ''')
        
        # Scraping statistics
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_date DATE DEFAULT CURRENT_DATE,
                total_profiles INTEGER DEFAULT 0,
                successful_scrapes INTEGER DEFAULT 0,
                failed_scrapes INTEGER DEFAULT 0,
                avg_scrape_time REAL DEFAULT 0,
                total_execution_time INTEGER DEFAULT 0
            )
        ''')
        
        # Data quality logs
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS quality_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_url TEXT,
                completeness REAL,
                validation_score INTEGER,
                errors TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Scraping history table - tracks all user inputs/sessions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraping_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_type TEXT NOT NULL,
                query_or_source TEXT,
                total_found INTEGER DEFAULT 0,
                total_requested INTEGER DEFAULT 0,
                scraped_count INTEGER DEFAULT 0,
                pending_count INTEGER DEFAULT 0,
                failed_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
        ''')
        
        # Profile queue - tracks which profiles belong to which session
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS profile_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                history_id INTEGER,
                profile_url TEXT NOT NULL,
                queue_position INTEGER,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (history_id) REFERENCES scraping_history(id)
            )
        ''')
        
        # Create indexes for faster queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON profiles(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON profiles(profile_url)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_created ON profiles(created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_history_id ON profile_queue(history_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_queue_status ON profile_queue(status)')
        
        conn.commit()
        conn.close()
        
        logger.info("Database initialized")
    
    def _get_connection(self, retries: int = 3) -> sqlite3.Connection:
        """Get database connection with timeout, WAL mode, and retry logic"""
        for attempt in range(retries):
            try:
                conn = sqlite3.connect(str(self.db_path), timeout=60.0)
                # Enable WAL mode for better concurrent access
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA busy_timeout=60000")
                return conn
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < retries - 1:
                    import time
                    logger.warning(f"Database locked, retrying in 2 seconds... (attempt {attempt + 1}/{retries})")
                    time.sleep(2)
                else:
                    raise
        raise sqlite3.OperationalError("Could not connect to database after retries")
    
    def add_profiles(self, profile_urls: List[str], session_id: Optional[int] = None) -> int:
        """Add profiles to scraping queue"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        added = 0
        for url in profile_urls:
            profile_hash = hashlib.md5(url.encode()).hexdigest()
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO profiles 
                    (profile_url, profile_hash, status) 
                    VALUES (?, ?, 'pending')
                ''', (url, profile_hash))
                
                if cursor.rowcount > 0:
                    added += 1
                    
            except sqlite3.IntegrityError:
                continue
        
        # Update session if provided
        if session_id:
            cursor.execute('''
                UPDATE search_sessions 
                SET total_profiles = total_profiles + ?
                WHERE id = ?
            ''', (added, session_id))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Added {added} profiles to queue")
        return added
    
    def save_profile_data(self, profile_url: str, data: Dict, completeness: float = 0):
        """Save scraped profile data - uses INSERT OR REPLACE for robustness"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            profile_hash = hashlib.md5(profile_url.encode()).hexdigest()
            cursor.execute('''
                INSERT OR REPLACE INTO profiles 
                (profile_url, profile_hash, status, data, scraped_at, updated_at, data_completeness)
                VALUES (?, ?, 'completed', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?)
            ''', (profile_url, profile_hash, json.dumps(data, ensure_ascii=False, indent=2), completeness))
            
            conn.commit()
            logger.debug(f"Saved profile data: {data.get('name', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Error saving profile: {e}")
        finally:
            conn.close()
    
    def mark_profile_failed(self, profile_url: str, error: str):
        """Mark profile as failed"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                UPDATE profiles 
                SET status = 'failed', error = ?, retry_count = retry_count + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE profile_url = ?
            ''', (error[:500], profile_url))  # Limit error length
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error marking profile failed: {e}")
        finally:
            conn.close()
    
    def is_profile_scraped(self, profile_url: str) -> bool:
        """Check if profile is already scraped"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT 1 FROM profiles 
                WHERE profile_url = ? AND status = 'completed'
            ''', (profile_url,))
            
            result = cursor.fetchone() is not None
            
        finally:
            conn.close()
        
        return result
    
    def get_pending_profiles(self, limit: int = 100) -> List[str]:
        """Get pending profiles for scraping (with resume capability)"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT profile_url FROM profiles 
                WHERE status = 'pending' AND retry_count < 3
                ORDER BY created_at 
                LIMIT ?
            ''', (limit,))
            
            profiles = [row[0] for row in cursor.fetchall()]
            
        finally:
            conn.close()
        
        return profiles
    
    def get_failed_profiles(self, limit: int = 100) -> List[str]:
        """Get failed profiles for re-scraping"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT profile_url FROM profiles 
                WHERE status = 'failed' AND retry_count < 5
                ORDER BY updated_at DESC
                LIMIT ?
            ''', (limit,))
            
            profiles = [row[0] for row in cursor.fetchall()]
            
        finally:
            conn.close()
        
        return profiles
    
    def reset_failed_to_pending(self, profile_urls: List[str] = None):
        """Reset failed profiles back to pending status for re-scraping"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if profile_urls:
                # Reset specific profiles
                for url in profile_urls:
                    cursor.execute('''
                        UPDATE profiles 
                        SET status = 'pending', error = NULL
                        WHERE profile_url = ? AND status = 'failed'
                    ''', (url,))
            else:
                # Reset all failed profiles with retry_count < 5
                cursor.execute('''
                    UPDATE profiles 
                    SET status = 'pending', error = NULL
                    WHERE status = 'failed' AND retry_count < 5
                ''')
            
            affected = cursor.rowcount
            conn.commit()
            logger.info(f"Reset {affected} failed profiles to pending")
            return affected
            
        finally:
            conn.close()
    
    def get_scraping_stats(self) -> Dict:
        """Get comprehensive scraping statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Get counts
            cursor.execute('SELECT COUNT(*) FROM profiles')
            total = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM profiles WHERE status = "completed"')
            completed = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM profiles WHERE status = "failed"')
            failed = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM profiles WHERE status = "pending"')
            pending = cursor.fetchone()[0]
            
            # Get average completeness
            cursor.execute('SELECT AVG(data_completeness) FROM profiles WHERE status = "completed"')
            avg_completeness = cursor.fetchone()[0] or 0
            
            # Get success rate
            success_rate = (completed / total * 100) if total > 0 else 0
            
            stats = {
                'total': total,
                'completed': completed,
                'failed': failed,
                'pending': pending,
                'success_rate': f"{success_rate:.1f}%",
                'avg_completeness': f"{avg_completeness:.1f}%",
                'progress': f"{completed}/{total}"
            }
            
        finally:
            conn.close()
        
        return stats
    
    def get_all_scraped_data(self, min_completeness: float = 0) -> List[Dict]:
        """Get all successfully scraped profiles"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT data FROM profiles 
                WHERE status = "completed" AND data_completeness >= ?
                ORDER BY data_completeness DESC
            ''', (min_completeness,))
            
            results = cursor.fetchall()
            
            data = []
            for row in results:
                try:
                    data.append(json.loads(row[0]))
                except:
                    continue
            
        finally:
            conn.close()
        
        return data
    
    def create_search_session(self, query: str) -> int:
        """Create a new search session"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO search_sessions (query, status)
                VALUES (?, 'active')
            ''', (query,))
            
            conn.commit()
            return cursor.lastrowid
            
        finally:
            conn.close()
    
    def update_session_stats(self, session_id: int):
        """Update session statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Get recent profile counts for this session
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                FROM profiles 
                WHERE created_at >= (SELECT created_at FROM search_sessions WHERE id = ?)
            ''', (session_id,))
            
            stats = cursor.fetchone()
            
            cursor.execute('''
                UPDATE search_sessions 
                SET total_profiles = ?, 
                    scraped_profiles = ?,
                    failed_profiles = ?
                WHERE id = ?
            ''', (stats[0], stats[1], stats[2], session_id))
            
            conn.commit()
            
        finally:
            conn.close()
    
    def export_to_json(self, filepath: str, min_completeness: float = 0):
        """Export all profiles to JSON"""
        data = self.get_all_scraped_data(min_completeness)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported {len(data)} profiles to {filepath}")
    
    def get_failed_profiles_with_details(self) -> List[Dict]:
        """Get failed profile URLs with error details"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT profile_url, error, retry_count FROM profiles 
                WHERE status = 'failed'
                ORDER BY retry_count DESC
            ''')
            
            profiles = [
                {'url': row[0], 'error': row[1], 'retries': row[2]}
                for row in cursor.fetchall()
            ]
            
        finally:
            conn.close()
        
        return profiles
    
    def cleanup_old_data(self, days: int = 30) -> int:
        """Clean up old data"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                DELETE FROM profiles 
                WHERE status = 'failed' AND created_at < datetime("now", ?)
            ''', (f'-{days} days',))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
        finally:
            conn.close()
        
        return deleted_count
    
    def get_db_size(self) -> str:
        """Get database file size"""
        try:
            size_bytes = self.db_path.stat().st_size
            size_mb = size_bytes / (1024 * 1024)
            return f"{size_mb:.2f} MB"
        except:
            return "Unknown"
    
    # =================== HISTORY TRACKING METHODS ===================
    
    def create_scraping_history(self, session_type: str, query_or_source: str, 
                                total_found: int, total_requested: int) -> int:
        """Create a new scraping history entry"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO scraping_history 
                (session_type, query_or_source, total_found, total_requested, status)
                VALUES (?, ?, ?, ?, 'active')
            ''', (session_type, query_or_source, total_found, total_requested))
            
            conn.commit()
            history_id = cursor.lastrowid
            logger.info(f"Created scraping history #{history_id}: {session_type} - {query_or_source}")
            return history_id
            
        finally:
            conn.close()
    
    def add_profiles_to_queue(self, history_id: int, profile_urls: List[str]) -> int:
        """Add profiles to queue with position tracking"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        added = 0
        for position, url in enumerate(profile_urls, 1):
            try:
                # Check if already in queue for this session
                cursor.execute('''
                    SELECT 1 FROM profile_queue 
                    WHERE history_id = ? AND profile_url = ?
                ''', (history_id, url))
                
                if cursor.fetchone() is None:
                    cursor.execute('''
                        INSERT INTO profile_queue 
                        (history_id, profile_url, queue_position, status)
                        VALUES (?, ?, ?, 'pending')
                    ''', (history_id, url, position))
                    added += 1
                    
            except sqlite3.IntegrityError:
                continue
        
        conn.commit()
        conn.close()
        
        logger.info(f"Added {added} profiles to queue for history #{history_id}")
        return added
    
    def update_queue_status(self, history_id: int, profile_url: str, status: str):
        """Update profile status in queue"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                UPDATE profile_queue 
                SET status = ?
                WHERE history_id = ? AND profile_url = ?
            ''', (status, history_id, profile_url))
            conn.commit()
        finally:
            conn.close()
    
    def get_unscraped_profiles(self, profile_urls: List[str]) -> List[str]:
        """Filter out already scraped profiles from list - return only unscraped ones"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        unscraped = []
        try:
            for url in profile_urls:
                cursor.execute('''
                    SELECT 1 FROM profiles 
                    WHERE profile_url = ? AND status = 'completed' AND data IS NOT NULL
                ''', (url,))
                
                if cursor.fetchone() is None:
                    unscraped.append(url)
            
            logger.info(f"Filtered profiles: {len(profile_urls)} total, {len(unscraped)} unscraped, {len(profile_urls) - len(unscraped)} already scraped")
            
        finally:
            conn.close()
        
        return unscraped
    
    def update_history_stats(self, history_id: int):
        """Update scraping history statistics"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as scraped,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                FROM profile_queue
                WHERE history_id = ?
            ''', (history_id,))
            
            stats = cursor.fetchone()
            
            cursor.execute('''
                UPDATE scraping_history 
                SET scraped_count = ?, pending_count = ?, failed_count = ?
                WHERE id = ?
            ''', (stats[1] or 0, stats[2] or 0, stats[3] or 0, history_id))
            
            # Check if completed
            if stats[2] == 0:  # No pending
                cursor.execute('''
                    UPDATE scraping_history 
                    SET status = 'completed', completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (history_id,))
            
            conn.commit()
            
        finally:
            conn.close()
    
    def get_scraping_history(self, limit: int = 20) -> List[Dict]:
        """Get recent scraping history"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT id, session_type, query_or_source, total_found, total_requested,
                       scraped_count, pending_count, failed_count, status, created_at, completed_at
                FROM scraping_history
                ORDER BY created_at DESC
                LIMIT ?
            ''', (limit,))
            
            history = []
            for row in cursor.fetchall():
                history.append({
                    'id': row[0],
                    'type': row[1],
                    'query': row[2],
                    'total_found': row[3],
                    'requested': row[4],
                    'scraped': row[5],
                    'pending': row[6],
                    'failed': row[7],
                    'status': row[8],
                    'created_at': row[9],
                    'completed_at': row[10]
                })
            
            return history
            
        finally:
            conn.close()
    
    def get_pending_from_history(self, history_id: int = None, limit: int = 100) -> List[str]:
        """Get pending profiles from specific history or most recent active"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if history_id:
                cursor.execute('''
                    SELECT profile_url FROM profile_queue
                    WHERE history_id = ? AND status = 'pending'
                    ORDER BY queue_position
                    LIMIT ?
                ''', (history_id, limit))
            else:
                # Get from most recent active session
                cursor.execute('''
                    SELECT pq.profile_url FROM profile_queue pq
                    JOIN scraping_history sh ON pq.history_id = sh.id
                    WHERE pq.status = 'pending' AND sh.status = 'active'
                    ORDER BY sh.created_at DESC, pq.queue_position
                    LIMIT ?
                ''', (limit,))
            
            return [row[0] for row in cursor.fetchall()]
            
        finally:
            conn.close()
