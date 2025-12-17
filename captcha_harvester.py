"""
CAPTCHA Harvester Service - Human-in-the-Loop CAPTCHA Solving
===============================================================
This FastAPI service provides a web interface for humans to solve CAPTCHAs
that appear during LinkedIn scraping. Solved tokens are stored in a queue
that the scraper polls automatically.

How it works:
1. Scraper detects CAPTCHA, sends {sitekey, page_url} to harvester
2. Harvester shows CAPTCHA in web UI
3. Human solves CAPTCHA in browser
4. Token is stored in queue
5. Scraper polls queue, gets token, injects it, continues automatically

No paid APIs needed - completely free and fully automatic after solving.
"""

import asyncio
import json
import logging
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from collections import deque
import uuid

from fastapi import FastAPI, WebSocket, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class CaptchaChallenge:
    """Represents a CAPTCHA challenge to be solved"""
    challenge_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sitekey: str = ""
    page_url: str = ""
    captcha_type: str = "recaptcha_v2"  # recaptcha_v2, hcaptcha, linkedin_challenge
    created_at: datetime = field(default_factory=datetime.utcnow)
    solved_at: Optional[datetime] = None
    token: Optional[str] = None
    status: str = "pending"  # pending, solving, solved, failed, expired
    attempts: int = 0
    error_message: str = ""
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        d = asdict(self)
        d['created_at'] = self.created_at.isoformat()
        d['solved_at'] = self.solved_at.isoformat() if self.solved_at else None
        return d
    
    def is_expired(self, timeout_seconds: int = 300) -> bool:
        """Check if challenge has expired (default 5 minutes)"""
        if self.status == "solved":
            return False
        return (datetime.utcnow() - self.created_at).total_seconds() > timeout_seconds


class CaptchaQueue:
    """Thread-safe queue for CAPTCHA challenges and solutions"""
    
    def __init__(self, max_size: int = 100):
        self.pending: deque = deque(maxlen=max_size)  # Waiting to be solved
        self.solved: Dict[str, CaptchaChallenge] = {}  # Successfully solved
        self.failed: deque = deque(maxlen=50)  # Failed attempts
        self.lock = asyncio.Lock()
    
    async def add_challenge(self, sitekey: str, page_url: str, 
                           captcha_type: str = "recaptcha_v2") -> str:
        """Add a new CAPTCHA challenge"""
        challenge = CaptchaChallenge(
            sitekey=sitekey,
            page_url=page_url,
            captcha_type=captcha_type
        )
        async with self.lock:
            self.pending.append(challenge)
            logger.info(f"[+] CAPTCHA challenge created: {challenge.challenge_id} "
                       f"({captcha_type}) from {page_url}")
        return challenge.challenge_id
    
    async def get_challenge(self) -> Optional[CaptchaChallenge]:
        """Get next pending CAPTCHA challenge"""
        async with self.lock:
            if self.pending:
                return self.pending[0]
        return None
    
    async def submit_solution(self, challenge_id: str, token: str) -> bool:
        """Submit a solved CAPTCHA token"""
        async with self.lock:
            # Find challenge in pending
            for i, challenge in enumerate(self.pending):
                if challenge.challenge_id == challenge_id:
                    challenge.token = token
                    challenge.status = "solved"
                    challenge.solved_at = datetime.utcnow()
                    self.solved[challenge_id] = self.pending.pop(i)
                    logger.info(f"[✓] CAPTCHA solved: {challenge_id} (token length: {len(token)})")
                    return True
        logger.warning(f"[!] Challenge not found: {challenge_id}")
        return False
    
    async def get_solution(self, challenge_id: str) -> Optional[str]:
        """Get token for a challenge if solved"""
        async with self.lock:
            if challenge_id in self.solved:
                challenge = self.solved[challenge_id]
                if not challenge.is_expired():
                    logger.info(f"[→] Token retrieved: {challenge_id}")
                    return challenge.token
                else:
                    logger.warning(f"[!] Token expired: {challenge_id}")
                    del self.solved[challenge_id]
        return None
    
    async def get_stats(self) -> Dict:
        """Get queue statistics"""
        async with self.lock:
            return {
                "pending": len(self.pending),
                "solved": len(self.solved),
                "failed": len(self.failed),
                "total": len(self.pending) + len(self.solved) + len(self.failed)
            }


# Initialize FastAPI app and CAPTCHA queue
app = FastAPI(title="CAPTCHA Harvester", version="1.0.0")
captcha_queue = CaptchaQueue()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# WebSocket connections for live updates
active_connections: List[WebSocket] = []


async def broadcast_update(message: Dict):
    """Send update to all connected WebSocket clients"""
    for connection in active_connections:
        try:
            await connection.send_json(message)
        except Exception as e:
            logger.error(f"Error broadcasting to WebSocket: {e}")


# ============================================================================
# REST API Endpoints
# ============================================================================

@app.post("/api/challenge/create")
async def create_challenge(
    sitekey: str,
    page_url: str,
    captcha_type: str = "recaptcha_v2"
) -> JSONResponse:
    """
    Create a new CAPTCHA challenge
    
    Args:
        sitekey: reCAPTCHA/hCaptcha sitekey
        page_url: URL where CAPTCHA appeared
        captcha_type: Type of CAPTCHA (recaptcha_v2, hcaptcha, linkedin_challenge)
    
    Returns:
        {challenge_id: "uuid", status: "pending"}
    """
    challenge_id = await captcha_queue.add_challenge(sitekey, page_url, captcha_type)
    
    # Notify web UI of new challenge
    await broadcast_update({
        "type": "new_challenge",
        "challenge_id": challenge_id,
        "captcha_type": captcha_type
    })
    
    return JSONResponse({
        "success": True,
        "challenge_id": challenge_id,
        "status": "pending"
    })


@app.post("/api/challenge/{challenge_id}/submit")
async def submit_challenge_solution(challenge_id: str, token: str) -> JSONResponse:
    """
    Submit solution for a CAPTCHA challenge
    
    Args:
        challenge_id: Challenge ID from /create
        token: Solved CAPTCHA token (g-recaptcha-response, h-captcha-response, etc.)
    
    Returns:
        {success: true, message: "CAPTCHA solved"}
    """
    if not token or len(token) < 10:
        raise HTTPException(status_code=400, detail="Invalid token")
    
    success = await captcha_queue.submit_solution(challenge_id, token)
    
    if success:
        await broadcast_update({
            "type": "challenge_solved",
            "challenge_id": challenge_id
        })
        return JSONResponse({
            "success": True,
            "message": "CAPTCHA solution submitted"
        })
    else:
        raise HTTPException(status_code=404, detail="Challenge not found")


@app.get("/api/challenge/{challenge_id}/solution")
async def get_challenge_solution(challenge_id: str) -> JSONResponse:
    """
    Poll for solution of a CAPTCHA challenge
    
    Args:
        challenge_id: Challenge ID from /create
    
    Returns:
        {token: "...", status: "solved"} or {status: "pending"}
    """
    token = await captcha_queue.get_solution(challenge_id)
    
    if token:
        return JSONResponse({
            "token": token,
            "status": "solved"
        })
    else:
        return JSONResponse({
            "token": None,
            "status": "pending"
        })


@app.get("/api/stats")
async def get_stats() -> JSONResponse:
    """Get harvester statistics"""
    stats = await captcha_queue.get_stats()
    return JSONResponse(stats)


@app.get("/api/current_challenge")
async def get_current_challenge() -> JSONResponse:
    """Get current CAPTCHA challenge waiting to be solved"""
    challenge = await captcha_queue.get_challenge()
    if challenge:
        return JSONResponse(challenge.to_dict())
    else:
        return JSONResponse({"status": "no_pending_challenges"})


# ============================================================================
# WebSocket Endpoint for Live Updates
# ============================================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket connection for live UI updates"""
    await websocket.accept()
    active_connections.append(websocket)
    
    logger.info(f"[+] WebSocket client connected. Total: {len(active_connections)}")
    
    try:
        while True:
            # Keep connection alive and listen for pings
            data = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            if data == "ping":
                await websocket.send_text("pong")
    except asyncio.TimeoutError:
        # Connection idle timeout - send stats
        stats = await captcha_queue.get_stats()
        await websocket.send_json({"type": "stats", "data": stats})
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        active_connections.remove(websocket)
        logger.info(f"[-] WebSocket client disconnected. Total: {len(active_connections)}")


# ============================================================================
# Web Interface
# ============================================================================

@app.get("/harvester", response_class=HTMLResponse)
async def harvester_ui():
    """Main CAPTCHA harvester web interface"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>CAPTCHA Harvester - LinkedIn Scraper</title>
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 20px;
            }
            
            .container {
                width: 100%;
                max-width: 900px;
                background: white;
                border-radius: 12px;
                box-shadow: 0 20px 60px rgba(0,0,0,0.3);
                overflow: hidden;
            }
            
            .header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }
            
            .header h1 {
                font-size: 28px;
                margin-bottom: 8px;
            }
            
            .header p {
                font-size: 14px;
                opacity: 0.9;
            }
            
            .content {
                padding: 40px;
            }
            
            .challenge-box {
                background: #f8f9fa;
                border: 2px dashed #667eea;
                border-radius: 8px;
                padding: 30px;
                text-align: center;
                margin-bottom: 30px;
            }
            
            .challenge-box.active {
                border-color: #28a745;
                background: #f0f8f5;
            }
            
            .challenge-box.inactive {
                opacity: 0.6;
            }
            
            .status-badge {
                display: inline-block;
                padding: 8px 16px;
                border-radius: 20px;
                font-weight: bold;
                font-size: 12px;
                margin-bottom: 20px;
            }
            
            .status-badge.pending {
                background: #fff3cd;
                color: #856404;
            }
            
            .status-badge.solving {
                background: #cce5ff;
                color: #004085;
            }
            
            .status-badge.solved {
                background: #d4edda;
                color: #155724;
            }
            
            .challenge-info {
                text-align: left;
                background: white;
                padding: 20px;
                border-radius: 8px;
                margin: 20px 0;
                font-family: 'Courier New', monospace;
                font-size: 12px;
            }
            
            .challenge-info label {
                display: block;
                margin-top: 10px;
                font-weight: bold;
                color: #666;
            }
            
            .challenge-info .value {
                margin-top: 4px;
                padding: 8px;
                background: #f8f9fa;
                border-radius: 4px;
                word-break: break-all;
                color: #333;
            }
            
            .iframe-container {
                margin: 20px 0;
                padding: 20px;
                background: white;
                border: 1px solid #ddd;
                border-radius: 8px;
                min-height: 78px;
            }
            
            .iframe-container iframe {
                width: 100%;
                height: 78px;
                border: none;
            }
            
            .token-input {
                width: 100%;
                padding: 12px;
                font-size: 14px;
                border: 2px solid #ddd;
                border-radius: 8px;
                margin: 10px 0;
                font-family: 'Courier New', monospace;
            }
            
            .token-input:focus {
                outline: none;
                border-color: #667eea;
            }
            
            .button-group {
                display: flex;
                gap: 10px;
                margin-top: 20px;
            }
            
            button {
                flex: 1;
                padding: 12px 20px;
                font-size: 14px;
                border: none;
                border-radius: 8px;
                cursor: pointer;
                font-weight: bold;
                transition: all 0.3s;
            }
            
            .btn-submit {
                background: #28a745;
                color: white;
            }
            
            .btn-submit:hover {
                background: #218838;
            }
            
            .btn-submit:disabled {
                background: #ccc;
                cursor: not-allowed;
            }
            
            .btn-clear {
                background: #6c757d;
                color: white;
            }
            
            .btn-clear:hover {
                background: #5a6268;
            }
            
            .stats {
                display: grid;
                grid-template-columns: repeat(4, 1fr);
                gap: 15px;
                margin-top: 30px;
                padding-top: 30px;
                border-top: 2px solid #eee;
            }
            
            .stat-card {
                background: #f8f9fa;
                padding: 15px;
                border-radius: 8px;
                text-align: center;
            }
            
            .stat-card .number {
                font-size: 32px;
                font-weight: bold;
                color: #667eea;
            }
            
            .stat-card .label {
                font-size: 12px;
                color: #666;
                margin-top: 8px;
                text-transform: uppercase;
            }
            
            .instructions {
                background: #e7f3ff;
                border-left: 4px solid #2196F3;
                padding: 15px;
                border-radius: 4px;
                margin: 20px 0;
                font-size: 13px;
                color: #0c5460;
            }
            
            .log-line {
                padding: 8px;
                border-bottom: 1px solid #eee;
                font-family: 'Courier New', monospace;
                font-size: 12px;
            }
            
            .log-line.success { color: #28a745; }
            .log-line.warning { color: #ffc107; }
            .log-line.error { color: #dc3545; }
            
            @media (max-width: 600px) {
                .stats {
                    grid-template-columns: repeat(2, 1fr);
                }
                .header h1 {
                    font-size: 20px;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🔐 CAPTCHA Harvester</h1>
                <p>LinkedIn Bulk Scraper - Human-in-the-Loop Solving</p>
            </div>
            
            <div class="content">
                <div id="challenge-container" class="challenge-box inactive">
                    <div class="status-badge" id="status-badge">No Active Challenge</div>
                    <p style="color: #999;">Waiting for CAPTCHA challenges from scraper...</p>
                </div>
                
                <div id="challenge-details" style="display:none;">
                    <div class="instructions">
                        <strong>Instructions:</strong>
                        <ol>
                            <li>The scraper has detected a CAPTCHA</li>
                            <li>Solve the CAPTCHA challenge below (or copy the sitekey to solve manually)</li>
                            <li>Paste the token (g-recaptcha-response or h-captcha-response) below</li>
                            <li>Click "Submit Solution" to continue scraping</li>
                        </ol>
                    </div>
                    
                    <div class="challenge-info">
                        <label>Challenge ID:</label>
                        <div class="value" id="challenge-id">-</div>
                        
                        <label>CAPTCHA Type:</label>
                        <div class="value" id="captcha-type">-</div>
                        
                        <label>Page URL:</label>
                        <div class="value" id="page-url">-</div>
                        
                        <label>Sitekey:</label>
                        <div class="value" id="sitekey">-</div>
                    </div>
                    
                    <div class="iframe-container" id="iframe-container"></div>
                    
                    <label>CAPTCHA Response Token:</label>
                    <textarea class="token-input" id="token-input" 
                              placeholder="Token will appear here automatically after solving... or paste manually"
                              rows="4"></textarea>
                    
                    <div class="button-group">
                        <button class="btn-submit" id="submit-btn" onclick="submitSolution()">
                            [OK] Submit Solution
                        </button>
                        <button class="btn-clear" onclick="copyToken()" style="background: #007bff;">
                            Copy Token
                        </button>
                        <button class="btn-clear" onclick="clearToken()">Clear</button>
                    </div>
                </div>
                
                <div class="stats">
                    <div class="stat-card">
                        <div class="number" id="stat-pending">0</div>
                        <div class="label">Pending</div>
                    </div>
                    <div class="stat-card">
                        <div class="number" id="stat-solved">0</div>
                        <div class="label">Solved</div>
                    </div>
                    <div class="stat-card">
                        <div class="number" id="stat-total">0</div>
                        <div class="label">Total</div>
                    </div>
                    <div class="stat-card">
                        <div class="number" id="time-now">-</div>
                        <div class="label">Status</div>
                    </div>
                </div>
            </div>
        </div>
        
        <script>
            let currentChallenge = null;
            let ws = null;
            
            // Initialize WebSocket connection
            function connectWebSocket() {
                const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                ws = new WebSocket(protocol + '//' + window.location.host + '/ws');
                
                ws.onopen = () => {
                    console.log('[+] WebSocket connected');
                    loadCurrentChallenge();
                    updateStats();
                };
                
                ws.onmessage = (event) => {
                    const message = JSON.parse(event.data);
                    console.log('[>] WebSocket message:', message);
                    
                    if (message.type === 'new_challenge') {
                        loadCurrentChallenge();
                    } else if (message.type === 'challenge_solved') {
                        showSuccess('Challenge solved! Scraper will continue shortly.');
                        setTimeout(loadCurrentChallenge, 2000);
                    } else if (message.type === 'stats') {
                        updateStatsDisplay(message.data);
                    }
                };
                
                ws.onerror = (error) => {
                    console.error('[!] WebSocket error:', error);
                };
                
                ws.onclose = () => {
                    console.log('[-] WebSocket disconnected. Reconnecting in 3s...');
                    setTimeout(connectWebSocket, 3000);
                };
            }
            
            // Load current challenge from server
            async function loadCurrentChallenge() {
                try {
                    const response = await fetch('/api/current_challenge');
                    const challenge = await response.json();
                    
                    if (challenge.challenge_id) {
                        currentChallenge = challenge;
                        displayChallenge(challenge);
                    } else {
                        clearChallenge();
                    }
                } catch (error) {
                    console.error('[!] Error loading challenge:', error);
                }
            }
            
            // Display challenge in UI
            function displayChallenge(challenge) {
                document.getElementById('challenge-container').className = 'challenge-box active';
                document.getElementById('challenge-details').style.display = 'block';
                
                // Update badge
                const badge = document.getElementById('status-badge');
                badge.textContent = '🟡 CAPTCHA Pending';
                badge.className = 'status-badge pending';
                
                // Update challenge info
                document.getElementById('challenge-id').textContent = challenge.challenge_id;
                document.getElementById('captcha-type').textContent = challenge.captcha_type;
                document.getElementById('page-url').textContent = challenge.page_url;
                document.getElementById('sitekey').textContent = challenge.sitekey;
                
                // Clear previous token
                document.getElementById('token-input').value = '';
                
                // Load reCAPTCHA iframe if applicable
                if (challenge.captcha_type === 'recaptcha_v2') {
                    const container = document.getElementById('iframe-container');
                    container.innerHTML = `
                        <script src="https://www.google.com/recaptcha/api.js" async defer></script>
                        <div class="g-recaptcha" data-sitekey="${challenge.sitekey}" data-callback="onRecaptchaSuccess"></div>
                    `;
                    // Wait for script to load and setup callback
                    setTimeout(() => {
                        window.onRecaptchaSuccess = function(token) {
                            console.log('[+] reCAPTCHA token received:', token.substring(0, 50) + '...');
                            document.getElementById('token-input').value = token;
                            setTimeout(submitSolution, 500);
                        };
                    }, 100);
                } else if (challenge.captcha_type === 'hcaptcha') {
                    const container = document.getElementById('iframe-container');
                    container.innerHTML = `
                        <script src="https://js.hcaptcha.com/1/api.js" async defer></script>
                        <div class="h-captcha" data-sitekey="${challenge.sitekey}" data-callback="onHcaptchaSuccess"></div>
                    `;
                    
                    // Setup callback when hCaptcha library loads
                    let hcaptchaReady = false;
                    const checkHCaptcha = setInterval(() => {
                        if (window.hcaptcha && !hcaptchaReady) {
                            hcaptchaReady = true;
                            clearInterval(checkHCaptcha);
                            console.log('[+] hCaptcha library loaded, setting up callback');
                            
                            window.onHcaptchaSuccess = function(token) {
                                console.log('[+] hCaptcha token received:', token.substring(0, 50) + '...');
                                document.getElementById('token-input').value = token;
                                setTimeout(submitSolution, 500);
                            };
                        }
                    }, 100);
                    
                    // Also setup polling to check for token in response
                    let pollAttempts = 0;
                    const pollForToken = setInterval(() => {
                        pollAttempts++;
                        if (pollAttempts > 100) { // Stop after 10 seconds
                            clearInterval(pollForToken);
                            return;
                        }
                        
                        try {
                            // Try to get token from hCaptcha response
                            const token = document.querySelector('textarea[name="h-captcha-response"]')?.value;
                            if (token && token.length > 10) {
                                console.log('[+] hCaptcha response found via textarea:', token.substring(0, 50) + '...');
                                clearInterval(pollForToken);
                                document.getElementById('token-input').value = token;
                                setTimeout(submitSolution, 500);
                            }
                        } catch (e) {
                            console.error('Error polling for token:', e);
                        }
                    }, 100);
                }
            }
            
            // Clear challenge display
            function clearChallenge() {
                document.getElementById('challenge-container').className = 'challenge-box inactive';
                document.getElementById('challenge-details').style.display = 'none';
                
                const badge = document.getElementById('status-badge');
                badge.textContent = '⏳ No Active Challenge';
                badge.className = 'status-badge';
            }
            
            // Submit solution
            async function submitSolution() {
                const token = document.getElementById('token-input').value.trim();
                
                if (!token) {
                    alert('Please paste the CAPTCHA token');
                    return;
                }
                
                if (!currentChallenge) {
                    alert('No active challenge');
                    return;
                }
                
                const submitBtn = document.getElementById('submit-btn');
                submitBtn.disabled = true;
                submitBtn.textContent = '⏳ Submitting...';
                
                try {
                    const response = await fetch(
                        '/api/challenge/' + currentChallenge.challenge_id + '/submit',
                        {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({token: token})
                        }
                    );
                    
                    const result = await response.json();
                    
                    if (response.ok) {
                        showSuccess('✓ CAPTCHA solution submitted! Scraper will continue now.');
                        setTimeout(() => {
                            document.getElementById('token-input').value = '';
                            submitBtn.disabled = false;
                            submitBtn.textContent = '✓ Submit Solution';
                            loadCurrentChallenge();
                        }, 1500);
                    } else {
                        alert('Error: ' + result.detail);
                        submitBtn.disabled = false;
                        submitBtn.textContent = '✓ Submit Solution';
                    }
                } catch (error) {
                    alert('Error submitting solution: ' + error);
                    submitBtn.disabled = false;
                    submitBtn.textContent = '✓ Submit Solution';
                }
            }
            
            // Clear token input
            function clearToken() {
                document.getElementById('token-input').value = '';
            }
            
            // Copy token to clipboard
            function copyToken() {
                const token = document.getElementById('token-input').value;
                if (token) {
                    navigator.clipboard.writeText(token).then(() => {
                        alert('Token copied to clipboard!');
                    }).catch(err => {
                        alert('Failed to copy: ' + err);
                    });
                } else {
                    alert('No token to copy');
                }
            }
            
            // Update statistics
            async function updateStats() {
                try {
                    const response = await fetch('/api/stats');
                    const stats = await response.json();
                    updateStatsDisplay(stats);
                } catch (error) {
                    console.error('[!] Error loading stats:', error);
                }
            }
            
            // Update stats display
            function updateStatsDisplay(stats) {
                document.getElementById('stat-pending').textContent = stats.pending;
                document.getElementById('stat-solved').textContent = stats.solved;
                document.getElementById('stat-total').textContent = stats.total;
                document.getElementById('time-now').textContent = 'Live ✓';
            }
            
            // Show success message
            function showSuccess(message) {
                const div = document.createElement('div');
                div.style.cssText = `
                    position: fixed;
                    top: 20px;
                    right: 20px;
                    background: #28a745;
                    color: white;
                    padding: 15px 20px;
                    border-radius: 8px;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.2);
                    z-index: 9999;
                    animation: slideIn 0.3s ease;
                `;
                div.textContent = message;
                document.body.appendChild(div);
                setTimeout(() => div.remove(), 4000);
            }
            
            // Initialize on load
            document.addEventListener('DOMContentLoaded', () => {
                connectWebSocket();
                setInterval(updateStats, 5000);
            });
        </script>
    </body>
    </html>
    """


@app.get("/", response_class=HTMLResponse)
async def root():
    """Redirect to harvester UI"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="refresh" content="0; url=/harvester">
    </head>
    <body>
        <p>Redirecting to <a href="/harvester">CAPTCHA Harvester</a>...</p>
    </body>
    </html>
    """


# ============================================================================
# Startup and Shutdown
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Log startup"""
    logger.info("="*70)
    logger.info("🔐 CAPTCHA Harvester Service Started")
    logger.info("="*70)
    logger.info(f"Web UI: http://localhost:8000/harvester")
    logger.info(f"API: http://localhost:8000/api/")
    logger.info("="*70)


@app.on_event("shutdown")
async def shutdown_event():
    """Log shutdown"""
    logger.info("🔐 CAPTCHA Harvester Service Stopped")


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    logger.info("Starting CAPTCHA Harvester Service on http://localhost:8000")
    logger.info("Open http://localhost:8000/harvester in your browser")
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        log_level="info"
    )
