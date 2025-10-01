import asyncio
from utils.graph_networking import *
from dotenv import load_dotenv

load_dotenv() 

async def main():
    client = initializeClient()

    site_id = env_or_fail("SITE_ID")
    names = await list_all_drives(client, site_id)
    print("Drives:", names)

if __name__ == "__main__":
    asyncio.run(main())