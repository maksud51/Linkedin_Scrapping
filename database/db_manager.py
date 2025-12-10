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
        
        # Create indexes for faster queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON profiles(status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON profiles(profile_url)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_created ON profiles(created_at)')
        
        conn.commit()
        conn.close()
        
        logger.info("Database initialized")
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        return sqlite3.connect(str(self.db_path))
    
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
        """Save scraped profile data"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                UPDATE profiles 
                SET status = 'completed', data = ?, scraped_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP, data_completeness = ?
                WHERE profile_url = ?
            ''', (json.dumps(data, ensure_ascii=False, indent=2), completeness, profile_url))
            
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
    
    def get_failed_profiles(self) -> List[Dict]:
        """Get failed profile URLs with errors"""
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
