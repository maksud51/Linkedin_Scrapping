#!/usr/bin/env python3
"""
Reset Database Script
Deletes all data from all tables in the SQLite database
"""

import sqlite3
import os
from pathlib import Path

def reset_database():
    """Delete all data from all tables"""
    
    db_path = "data/linkedin_scraper.db"
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print("✓ No tables found in database")
            conn.close()
            return True
        
        print(f"📊 Found {len(tables)} tables:")
        
        # Delete all data from each table
        for (table_name,) in tables:
            print(f"  • Deleting from {table_name}...", end=" ")
            try:
                cursor.execute(f"DELETE FROM {table_name};")
                print(f"✓")
            except sqlite3.Error as e:
                print(f"❌ Error: {e}")
                continue
        
        # Commit changes
        conn.commit()
        
        # Verify all tables are empty
        print("\n✓ Verification:")
        for (table_name,) in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            print(f"  • {table_name}: {count} rows")
        
        conn.close()
        print("\n✅ Database reset complete!")
        return True
        
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


def reset_database_with_schema():
    """Reset database and recreate schema"""
    
    db_path = "data/linkedin_scraper.db"
    
    try:
        # First, delete all data
        print("🔄 Step 1: Deleting all data...")
        reset_database()
        
        # Recreate tables with fresh schema
        print("\n🔄 Step 2: Recreating schema...")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create profiles table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_url TEXT UNIQUE NOT NULL,
                profile_hash TEXT UNIQUE NOT NULL,
                status TEXT DEFAULT 'pending',
                data TEXT,
                scraped_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_completeness FLOAT DEFAULT 0.0
            )
        ''')
        
        # Create search_sessions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                search_query TEXT,
                total_profiles INTEGER DEFAULT 0,
                scraped_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create exports table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS exports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                export_format TEXT,
                file_path TEXT,
                record_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
        print("✅ Schema recreated successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    import sys
    
    print("=" * 60)
    print("DATABASE RESET UTILITY")
    print("=" * 60)
    print("\nOptions:")
    print("  1. Delete all data from tables (keep schema)")
    print("  2. Delete all data AND recreate schema")
    print("  0. Cancel")
    print("\nChoice: ", end="")
    
    choice = input().strip()
    
    if choice == "1":
        print("\n⚠️  WARNING: This will delete all data from the database!")
        print("Continue? (yes/no): ", end="")
        confirm = input().strip().lower()
        
        if confirm == "yes":
            reset_database()
        else:
            print("❌ Cancelled")
            
    elif choice == "2":
        print("\n⚠️  WARNING: This will delete all data AND recreate tables!")
        print("Continue? (yes/no): ", end="")
        confirm = input().strip().lower()
        
        if confirm == "yes":
            reset_database_with_schema()
        else:
            print("❌ Cancelled")
            
    else:
        print("❌ Cancelled")
    
    print("=" * 60)
