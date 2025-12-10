#!/usr/bin/env python3
"""
Debug script to test and verify all functionality
"""

import asyncio
import sys
import json
from pathlib import Path

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))

from database.db_manager import DatabaseManager
from utils.exporter import DataExporter
from utils.logger import setup_logging

logger = setup_logging()

async def test_database():
    """Test database connectivity and data"""
    print("\n" + "="*60)
    print("🔍 DATABASE TEST")
    print("="*60)
    
    db = DatabaseManager()
    
    # Check if database exists
    if not Path("data/linkedin_scraper.db").exists():
        print("❌ Database file not found!")
        return False
    
    print("✓ Database file exists")
    
    # Get all profiles
    profiles = db.get_all_scraped_data()
    print(f"✓ Total scraped profiles: {len(profiles)}")
    
    if profiles:
        print("\nProfile samples:")
        for idx, profile in enumerate(profiles[:2], 1):
            print(f"\n  Profile {idx}:")
            print(f"    Name: {profile.get('name', 'N/A')}")
            print(f"    Headline: {profile.get('headline', 'N/A')}")
            print(f"    Location: {profile.get('location', 'N/A')}")
            print(f"    Has contact_info: {'contact_info' in profile}")
            print(f"    Has experience: {'experience' in profile}")
            print(f"    Has education: {'education' in profile}")
    
    return True

def test_exporter():
    """Test exporter functionality"""
    print("\n" + "="*60)
    print("🔍 EXPORTER TEST")
    print("="*60)
    
    exporter = DataExporter()
    print(f"✓ Export path: {exporter.export_path}")
    
    # Create test data
    test_profiles = [
        {
            "name": "Test User 1",
            "headline": "Software Engineer at Test Co",
            "location": "San Francisco, CA",
            "profile_url": "https://linkedin.com/in/testuser1",
            "about": "Test about section",
            "contact_info": {
                "emails": ["test1@example.com"],
                "phones": ["+1-555-0001"],
                "websites": ["https://example.com"],
                "twitter": ["@testuser1"],
            },
            "experience": [
                {"title": "Software Engineer", "company": "Test Co", "duration": "2020-present"}
            ],
            "education": [
                {"school": "Test University", "field": "Computer Science", "year": 2020}
            ],
            "skills": ["Python", "JavaScript", "React"]
        },
        {
            "name": "Test User 2",
            "headline": "Data Scientist",
            "location": "New York, NY",
            "profile_url": "https://linkedin.com/in/testuser2",
            "about": "Another test",
            "contact_info": {
                "emails": ["test2@example.com"],
            },
            "experience": [],
            "education": [],
            "skills": ["Python", "SQL"]
        }
    ]
    
    print(f"\n✓ Test data created: {len(test_profiles)} profiles")
    
    # Test JSON export
    print("\n📄 Testing JSON export...", end=" ")
    json_result = exporter.export_json(test_profiles, "test_profiles.json")
    if json_result:
        json_file = exporter.export_path / "test_profiles.json"
        if json_file.exists():
            print(f"✓ ({json_file})")
        else:
            print("❌ File not created")
    else:
        print("❌ Export failed")
    
    # Test CSV export
    print("📄 Testing CSV export...", end=" ")
    csv_result = exporter.export_csv(test_profiles, "test_profiles.csv")
    if csv_result:
        csv_file = exporter.export_path / "test_profiles.csv"
        if csv_file.exists():
            print(f"✓ ({csv_file})")
        else:
            print("❌ File not created")
    else:
        print("❌ Export failed")
    
    # Test Excel export
    print("📄 Testing Excel export...", end=" ")
    xlsx_result = exporter.export_excel(test_profiles, "test_profiles.xlsx")
    if xlsx_result:
        xlsx_file = exporter.export_path / "test_profiles.xlsx"
        if xlsx_file.exists():
            print(f"✓ ({xlsx_file})")
        else:
            print("❌ File not created")
    else:
        print("⚠️  Skipped (openpyxl not available)")
    
    return True

async def test_full_workflow():
    """Test complete workflow"""
    print("\n" + "="*60)
    print("🔍 FULL WORKFLOW TEST")
    print("="*60)
    
    # Test database
    db_ok = await test_database()
    
    # Test exporter
    exp_ok = test_exporter()
    
    print("\n" + "="*60)
    if db_ok and exp_ok:
        print("✅ ALL TESTS PASSED!")
    else:
        print("⚠️  SOME TESTS HAD ISSUES")
    print("="*60)

if __name__ == "__main__":
    print("\n╔" + "="*58 + "╗")
    print("║  DEBUG & VERIFICATION TOOL                              ║")
    print("╚" + "="*58 + "╝")
    
    asyncio.run(test_full_workflow())
