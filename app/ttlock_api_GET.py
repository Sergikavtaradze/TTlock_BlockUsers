import httpx
import os
import time
from dotenv import load_dotenv
from utils import now_ms

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
BASE_URL = os.getenv("TTLOCK_API_URL")
CLIENT_ID = os.getenv("TTLOCK_CLIENT_ID")
CLIENT_SECRET = os.getenv("TTLOCK_CLIENT_SECRET")
ACCESS_TOKEN = os.getenv("TTLOCK_ACCESS_TOKEN")
# ---------------------

# Assuming BASE_URL is still https://euapi.ttlock.com
async def get_lock_list():
    # normalize BASE_URL to avoid double slashes
    base = BASE_URL.rstrip('/')
    path = '/v3/lock/list'
    url = f"{base}{path}"

    params = {
        "clientId": CLIENT_ID,
        "accessToken": ACCESS_TOKEN,
        "pageNo": "1",
        "pageSize": "20",
        "date": now_ms,
    }

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

async def sync_access_IC_ekey(locks: list):
    """
    Fetches both eKeys and IC Cards for all locks.
    Groups them into a single 'Master Registry' for database import.
    """
    # master_registry structure:
    master_registry = {"ekeys": {}, "cards": {}}

    async with httpx.AsyncClient(timeout=15.0) as client:
        for lock in locks:
            lock_id = lock.get('lockId') or lock.get('id')
            lock_name = lock.get('lockAlias') or lock.get('name')

            print(f"--- Processing Lock: {lock_name} ---")

            # 1. FETCH E-KEYS (App Users)
            ekey_url = f"{BASE_URL}/v3/lock/listKey"
            ekey_params = {
                "clientId": CLIENT_ID, "accessToken": ACCESS_TOKEN,
                "lockId": lock_id, "pageNo": "1", "pageSize": "170", "date": now_ms()
            }
            ekey_resp = await client.get(ekey_url, params=ekey_params)
            ekey_data = ekey_resp.json()

            if ekey_data.get("errcode") is None:
                for k in ekey_data.get("list", []):
                    # We use 'keyName' as the person's identifier (e.g., "34 - ვანო გილგემიანი")
                    person = k.get("keyName") or k.get("username")
                    if person not in master_registry["ekeys"]:
                        master_registry["ekeys"][person] = []
                    
                    master_registry["ekeys"][person].append({
                        "username": k.get("username"),
                        "lockId": k.get("lockID"),
                        "keyId": k.get("keyId"),
                        "status": k.get("keyStatus"),
                    })
            
            # 2. FETCH IC CARDS (Physical Cards)
            card_url = f"{BASE_URL}/v3/identityCard/list"
            
            card_params = {
                "clientId": CLIENT_ID, "accessToken": ACCESS_TOKEN,
                "lockId": lock_id, "pageNo": "1", "pageSize": "460", "date": now_ms()
            }
            card_resp = await client.get(card_url, params=card_params)
            card_data = card_resp.json()

            if card_data.get("errcode", 0) == 0:
                for c in card_data.get("list", []):
                    person = c.get("cardName") or "Unnamed Card"
                    if person not in master_registry["cards"]:
                        master_registry["cards"][person] = []
                    
                    master_registry["cards"][person].append({
                        "cardNumber": c.get("cardNumber"),
                        "lockId": c.get("lockId"),
                        "cardId": c.get("cardId"),
                        "startDate": c.get("startDate"),
                        "endDate": c.get("endDate"),
                        "createDate": c.get("createDate")
                    })
            
            print(f"Done : {lock_name}")

    return master_registry

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
    # # 1. Get the lock list
    # locks = await get_lock_list(CONFIRMED_TOKEN)
    # #############
    # OR Use below during development
    # ##############
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
        # Map users to those locks
        user_data = await sync_access_IC_ekey(locks)
        
        # Print the report
        #display_user_report(user_data)
        
        # Save to a JSON
        import json
        # with open("user_lock_map.json", "w", encoding="utf-8") as f:
        #     json.dump(user_data, f, ensure_ascii=False, indent=4)

        with open("building_access_master_170_460.json", "w", encoding="utf-8") as f:
            json.dump(user_data, f, ensure_ascii=False, indent=4)
        print("\n✅ Data exported to building_access_master.json")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())



    # async def fetch_and_map_users(token: str, locks: list):
#     """
#     Fetches all eKeys for all locks and groups them by user.
#     """
#     user_registry = {}  # { username: [ {lockName, keyId, startDate, endDate}, ... ] }
    
#     print(f"\n--- Fetching Users for {len(locks)} Locks ---")

#     async with httpx.AsyncClient(timeout=10.0) as client:
#         for lock in locks:
#             # First we get the ttlock account ekey permission --> After, we get the IC Card permission
#             now_ms = int(time.time() * 1000) # Need to generate this for every request
#             lock_id = lock['lockId']
#             lock_alias = lock['name']
            
#             print(f"Scanning: {lock_alias}...")

#             # API Endpoint from your documentation
#             url_ekey = f"{BASE_URL.rstrip('/')}/v3/lock/listKey"
            
#             params = {
#                 "clientId": CLIENT_ID,
#                 "accessToken": token,
#                 "lockId": str(lock_id),
#                 "pageNo": "1",
#                 "pageSize": "20",
#                 "date": str(now_ms)
#             }
#             print(params)
#             response = await client.get(url_ekey, params=params)
#             data = response.json()
#             #print(data)
#             print(data.get("errcode"))
#             #print(data.get("list", []))
#             if data.get("errcode") is None:
#                 ekeys = data.get("list", [])
#                 for ekey in ekeys:
#                     print(f"This is the ekey {ekey}")
#                     username = ekey.get("username")
#                     if not username: continue
                    
#                     # Prepare the lock info for this user
#                     lock_info = {
#                         "lockName": lock_alias,
#                         "keyId": ekey.get("keyId"),
#                         "status": ekey.get("keyStatus"),
#                         "expiry": ekey.get("endDate"),
#                         "keyName": ekey.get("keyName")
#                     }

#                     # Group by username
#                     if username not in user_registry:
#                         user_registry[username]["ekey"] = []
#                     user_registry[username]["ekey"].append(lock_info)
#             else:
#                 # Capture the actual error message from TTLock
#                 error_msg = data.get('description') or data.get('errmsg') or "Unknown Error"
#                 print(f"  🛑 Error on {lock_alias}: {error_msg} (Code: {data.get('errcode')})")

#     return user_registry