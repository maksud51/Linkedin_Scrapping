"""
Database Cleaner Script
Clears all data from all tables in the LinkedIn scraper database
"""

import sqlite3
from pathlib import Path

DB_PATH = Path("data/linkedin_scraper.db")

def clear_all_tables():
    """Clear all data from all tables"""
    if not DB_PATH.exists():
        print("[ERROR] Database not found!")
        return
    
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    print(f"\n{'='*50}")
    print("DATABASE CLEANER")
    print(f"{'='*50}")
    print(f"\nFound {len(tables)} tables: {', '.join(tables)}")
    
    # Show current row counts
    print("\nCurrent data:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  - {table}: {count} rows")
    
    # Confirm deletion
    print("\n" + "="*50)
    confirm = input("Delete ALL data from ALL tables? (yes/no): ").strip().lower()
    
    if confirm == 'yes':
        for table in tables:
            cursor.execute(f"DELETE FROM {table}")
            print(f"  [OK] Cleared {table}")
        
        conn.commit()
        print("\n[SUCCESS] All tables cleared!")
        
        # Vacuum to reclaim space
        cursor.execute("VACUUM")
        print("[OK] Database vacuumed (space reclaimed)")
    else:
        print("\n[CANCELLED] No data was deleted.")
    
    conn.close()

if __name__ == "__main__":
    clear_all_tables()
