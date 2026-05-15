import httpx
import os
import time
from dotenv import load_dotenv
import hashlib

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
BASE_URL = os.getenv("TTLOCK_API_URL")
CLIENT_ID = os.getenv("TTLOCK_CLIENT_ID")
CLIENT_SECRET = os.getenv("TTLOCK_CLIENT_SECRET")
USERNAME = os.getenv("TTLOCK_USERNAME")
PLAIN_PASSWORD = os.getenv("TTLOCK_PASSWORD")

def hash_password_md5(password: str) -> str:
    """Encrypts a plain-text password using MD5 and returns the 32-character lowercase hash."""
    return hashlib.md5(password.encode('utf-8')).hexdigest()

async def get_access_token() -> str:
    """
    STEP 1: Authenticate and get the access token.
    Endpoint: /oauth2/token
    """
    print("--- 1. Requesting Access Token ---")
    
    # --- CRITICAL STEP: HASH THE PASSWORD HERE ---
    hashed_password = hash_password_md5(PLAIN_PASSWORD)
    print(f"Password Hashed: {hashed_password[:4]}...{hashed_password[-4:]}") # Safe print
    # --------------------------------------------
    
    # url = f"{BASE_URL}/oauth2/token"
    
    relative_path = "/oauth2/token"

    # 2. Define the payload for the POST request body
    payload = {
        "clientId": CLIENT_ID,
        "clientSecret": CLIENT_SECRET,
        "username": USERNAME,
        "password": hashed_password, # The hashed password
        "grant_type": "password",
        "redirect_uri": "http://localhost"
    }

    # 3. Initialize client with BASE_URL
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        # 4. Use POST method, passing the payload to the 'data' argument.
        # httpx will automatically encode this as application/x-www-form-urlencoded.
        response = await client.post(relative_path, data=payload)
        
        # Check for non-2xx status codes (e.g., 400, 500)
        response.raise_for_status()
        
        data = response.json()
        
        # Check for TTLock's custom error code (errcode != 0)
        if data.get("errcode") != 0:
            print(f"Authentication Failed! Error: {data.get('errmsg', 'Unknown TTLock Error')}")
            return None
        
        print("✅ Token received successfully.")
        return data["access_token"]