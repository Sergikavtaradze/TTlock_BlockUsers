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
# ---------------------


def hash_password_md5(password: str) -> str:
    """Encrypts a plain-text password using MD5 and returns the 32-character lowercase hash."""
    return hashlib.md5(password.encode('utf-8')).hexdigest()

# async def get_access_token() -> str:
#     """
#     STEP 1: Authenticate and get the access token.
#     Endpoint: /oauth2/token
#     """
#     print("--- 1. Requesting Access Token ---")
    
#     # --- CRITICAL STEP: HASH THE PASSWORD HERE ---
#     hashed_password = hash_password_md5(PLAIN_PASSWORD)
#     print(f"Password Hashed: {hashed_password[:4]}...{hashed_password[-4:]}") # Safe print
#     # --------------------------------------------
    
#     # url = f"{BASE_URL}/oauth2/token"
    
#     relative_path = "/oauth2/token"

#     # 2. Define the payload for the POST request body
#     payload = {
#         "clientId": CLIENT_ID,
#         "clientSecret": CLIENT_SECRET,
#         "username": USERNAME,
#         "password": hashed_password, # The hashed password
#         "grant_type": "password",
#         "redirect_uri": "http://localhost"
#     }

#     # 3. Initialize client with BASE_URL
#     async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
#         # 4. Use POST method, passing the payload to the 'data' argument.
#         # httpx will automatically encode this as application/x-www-form-urlencoded.
#         response = await client.post(relative_path, data=payload)
        
#         # Check for non-2xx status codes (e.g., 400, 500)
#         response.raise_for_status()
        
#         data = response.json()
        
#         # Check for TTLock's custom error code (errcode != 0)
#         if data.get("errcode") != 0:
#             print(f"Authentication Failed! Error: {data.get('errmsg', 'Unknown TTLock Error')}")
#             return None
        
#         print("✅ Token received successfully.")
#         return data["access_token"]


# Assuming BASE_URL is still https://euapi.ttlock.com

async def get_lock_list(token: str, group_id: int | None = None):
    # build timestamp as integer milliseconds
    now_ms = int(time.time() * 1000)

    # normalize BASE_URL to avoid double slashes
    base = BASE_URL.rstrip('/')
    path = '/v3/lock/list'
    url = f"{base}{path}"

    params = {
        "clientId": CLIENT_ID,
        "accessToken": token,
        "pageNo": "1",
        "pageSize": "20",
        "date": str(now_ms),
    }

    if group_id is not None:
        params["groupId"] = str(group_id)

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.get(url, params=params)

        # print the final URL for exact parity check with curl
        print("REQUEST URL:", response.url)

        # always print raw response for debugging
        print("STATUS:", response.status_code)
        print("RAW RESPONSE:", response.text)

        # quick HTTP error handling
        if response.status_code >= 400:
            return None

        # try parse JSON
        try:
            data = response.json()
        except Exception:
            return None

        if data.get("errcode") != 0:
            print("TTLock error:", data.get("errcode"), data.get("errmsg"))
            return None

        return data.get("list", [])

async def fetch_and_map_users(token: str, locks: list):
    """
    Fetches all eKeys for all locks and groups them by user.
    """
    user_registry = {}  # { username: [ {lockName, keyId, startDate, endDate}, ... ] }
    
    print(f"\n--- Fetching Users for {len(locks)} Locks ---")

    async with httpx.AsyncClient(timeout=10.0) as client:
        for lock in locks:
            # First we get the ttlock account ekey permission --> After, we get the IC Card permission
            now_ms = int(time.time() * 1000) # Need to generate this for every request
            lock_id = lock['lockId']
            lock_alias = lock['name']
            
            print(f"Scanning: {lock_alias}...")

            # API Endpoint from your documentation
            url_ekey = f"{BASE_URL.rstrip('/')}/v3/lock/listKey"
            
            params = {
                "clientId": CLIENT_ID,
                "accessToken": token,
                "lockId": str(lock_id),
                "pageNo": "1",
                "pageSize": "20",
                "date": str(now_ms)
            }
            print(params)
            response = await client.get(url_ekey, params=params)
            data = response.json()
            #print(data)
            print(data.get("errcode"))
            #print(data.get("list", []))
            if data.get("errcode") is None:
                ekeys = data.get("list", [])
                for ekey in ekeys:
                    print(f"This is the ekey {ekey}")
                    username = ekey.get("username")
                    if not username: continue
                    
                    # Prepare the lock info for this user
                    lock_info = {
                        "lockName": lock_alias,
                        "keyId": ekey.get("keyId"),
                        "status": ekey.get("keyStatus"),
                        "expiry": ekey.get("endDate"),
                        "keyName": ekey.get("keyName")
                    }

                    # Group by username
                    if username not in user_registry:
                        user_registry[username]["ekey"] = []
                    user_registry[username]["ekey"].append(lock_info)
            else:
                # Capture the actual error message from TTLock
                error_msg = data.get('description') or data.get('errmsg') or "Unknown Error"
                print(f"  🛑 Error on {lock_alias}: {error_msg} (Code: {data.get('errcode')})")
            
    return user_registry

def display_user_report(user_registry):
    """Prints a clean summary of who has access to what."""
    print("\n" + "="*80)
    print(f"{'USERNAME':<30} | {'ACCESS TO LOCKS'}")
    print("-" * 80)
    
    for user, lock_list in user_registry.items():
        # Combine lock names into a string for display
        lock_names = ", ".join([l['lockName'] for l in lock_list])
        print(f"{user:<30} | {lock_names}")
    print("="*80)

async def main():
    # Use your confirmed token
    CONFIRMED_TOKEN = "e1986129bee22be76156c95a076a0f40" # "7f37b29390e92b7255c87b63d87a966b" This is 20skavtaradze token.
    
    # # 1. Get the lock list
    # locks = await get_lock_list(CONFIRMED_TOKEN)

    # 8 locks extracted from previous command above
    locks = [
    {"lockId": 26986212, "name": "ტერასა (Terrace)"},
    {"lockId": 26436420, "name": "II Hall Door"},
    {"lockId": 26411294, "name": "Parking 2"},
    {"lockId": 26382284, "name": "Parking 1"},
    {"lockId": 26294486, "name": "I Hall Door"},
    {"lockId": 22474898, "name": "II Hall Elevator"},
    {"lockId": 22166420, "name": "Right [I Hall]"},
    {"lockId": 21127013, "name": "Left [I Hall]"}
    ]
    
    if locks:
        # 2. Map users to those locks
        user_data = await fetch_and_map_users(CONFIRMED_TOKEN, locks)
        
        # 3. Print the report
        display_user_report(user_data)
        
        # 4. Optional: Save to a JSON for your own review before SQL
        import json
        with open("user_lock_map.json", "w", encoding="utf-8") as f:
            json.dump(user_data, f, ensure_ascii=False, indent=4)
        print("\n✅ Data exported to user_lock_map.json")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())