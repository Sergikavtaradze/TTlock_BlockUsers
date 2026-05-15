import httpx
import os
from dotenv import load_dotenv
from utils import now_ms

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
BASE_URL = os.getenv("TTLOCK_API_URL")
CLIENT_ID = os.getenv("TTLOCK_CLIENT_ID")
CLIENT_SECRET = os.getenv("TTLOCK_CLIENT_SECRET")
ACCESS_TOKEN = os.getenv("TTLOCK_ACCESS_TOKEN")


# Freeze/unfreeze logic
async def _ttlock_ekey_request(apiUrlPath: str, keyId: int):
    """
    Internal helper to handle the common request/response logic.
    """
    url = f"{BASE_URL.rstrip('/')}{apiUrlPath}"
    params = {
        "clientId": CLIENT_ID,
        "accessToken": ACCESS_TOKEN,
        "keyId": keyId,
        "date": now_ms(),
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, params=params)
            
            print(f"REQUEST URL: {response.url}")
            print(f"STATUS: {response.status_code}")

            response.raise_for_status()
            data = response.json()

            if data.get("errcode") != 0:
                print(f"TTLock error: {data.get('errcode')} - {data.get('errmsg')}")
            
    except Exception as e:
        print(f"Request failed: {e}")

async def freeze_ekey(keyIds: list):
    for keyId in keyIds:
        await _ttlock_ekey_request('/v3/key/freeze', keyId)

async def unfreeze_ekey(keyIds: list):
    for keyId in keyIds:
        await _ttlock_ekey_request('/v3/key/unfreeze', keyId)


async def main():
    keyId = [255874974, 255874988, 258722588, 255874984, 255874982, 258724330, 258723452, 261011116, 255874974]

    keyIds = [
        255874148,
        258722422,
        250479458,
        250304944,
        258724164,
        258723286,
        261010950,
        255219484
    ]
            
    # Map users to those locks
    _ = await unfreeze_ekey(keyIds)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())