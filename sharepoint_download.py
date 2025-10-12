import asyncio
from datetime import datetime
from utils.graph_networking import *
from dotenv import load_dotenv
import json

load_dotenv() 

def env_or_fail(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing environment variable {name}")
    return value

def setup_logging_from_env(local_root: str, timestamp: str):
    enable_logging = os.getenv('ENABLE_LOGGING', 'True').lower() in ('true', '1', 'yes')
    log_level = os.getenv('LOG_LEVEL', 'INFO')

    if not enable_logging:
        logging.getLogger().setLevel(logging.CRITICAL)
    
    log_directory = os.path.join(local_root, LOGS_DIR)
    log_file = os.path.join(log_directory, f"{timestamp}.log")

    os.makedirs(log_directory, exist_ok=True)
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[*([logging.FileHandler(log_file, encoding='utf-8')])]
    )

    http_loggers = [
        'azure', 'msgraph', 'httpx', 'aiohttp', 'urllib3',
        'azure.identity', 'msgraph_core'
    ]
    
    for logger_name in http_loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.WARNING)
        logger.propagate = False

    logging.info(f"Logging to file: {log_file}")

async def main():
    site_id = env_or_fail("SITE_ID")
    local_root = env_or_fail("LOCAL_ROOT")
    tenant_id = env_or_fail("TENANT_ID")
    client_id = env_or_fail("CLIENT_ID")
    client_secret = env_or_fail("CLIENT_SECRET")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    drives = json.loads(os.getenv('DRIVES', '[]'))
    setup_logging_from_env(local_root, timestamp)
    obj = SharePointDownloader(site_id, local_root, timestamp, tenant_id, client_id, client_secret)
    name_ids = await obj.list_all_drives()
    #await obj.test_hash_algorithm()
    for id, name in name_ids:
        if name in drives:
            try:
                await obj.download_drive(id, name)
            except Exception as e:
                print(e)  

if __name__ == "__main__":
    asyncio.run(main())