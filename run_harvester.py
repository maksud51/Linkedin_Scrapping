#!/usr/bin/env python
"""Start harvester with proper environment setup"""
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Now run harvester
if __name__ == "__main__":
    import uvicorn
    from captcha_harvester import app
    
    print("\n" + "="*70)
    print("🔐 CAPTCHA Harvester Service Starting...")
    print("="*70)
    print(f"Python: {sys.executable}")
    print(f"Working Dir: {os.getcwd()}")
    print(f"Port: 8000")
    print(f"URL: http://localhost:8000/harvester")
    print("="*70 + "\n")
    
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        log_level="info"
    )
