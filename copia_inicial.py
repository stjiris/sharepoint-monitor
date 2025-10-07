import asyncio
from datetime import datetime
from utils.graph_networking import *
from dotenv import load_dotenv

load_dotenv() 

def setup_logging_from_env(local_root: str):
    enable_logging = os.getenv('ENABLE_LOGGING', 'True').lower() in ('true', '1', 'yes')
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    log_directory = "logs"

    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    if not enable_logging:
        logging.getLogger().setLevel(logging.CRITICAL)
        return timestamp
    
    log_directory = os.path.join(local_root, log_directory)
    timed_local_root = local_root + '/' + timestamp
    os.makedirs(timed_local_root, exist_ok=True)

    log_file = os.path.join(timed_local_root, f"{timestamp}.log")
    
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
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
    return timestamp

async def main():
    site_id = env_or_fail("SITE_ID")
    local_root = env_or_fail("LOCAL_ROOT")
    formatted = setup_logging_from_env(local_root)
    timed_local_root = local_root + '/' + formatted


    obj = SharePointDownloader(site_id, local_root, timed_local_root)
    name_ids = await obj.list_all_drives()
    

    for id, name in name_ids:
        if name == 'Anonimização':
            try:
                await obj.download_drive(id, name)
            except Exception as e:
                print(e)  

if __name__ == "__main__":
    asyncio.run(main())