from datetime import datetime
import hashlib
import logging
import os
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.drive_item import DriveItem
from .aux import env_or_fail 

class SharePointDownloader:
    def __init__(self, site_id, local_root, timed_local_root):
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.client: GraphServiceClient = SharePointDownloader.initializeClient()
        self.site_id: str = site_id
        self.local_root: str = local_root
        self.timed_local_root: str = timed_local_root

        os.makedirs(timed_local_root, exist_ok=True)

    def initializeClient() -> GraphServiceClient:
        tenant_id = env_or_fail("TENANT_ID")
        client_id = env_or_fail("CLIENT_ID")
        client_secret = env_or_fail("CLIENT_SECRET")

        cred = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        scopes = ['https://graph.microsoft.com/.default']
        client = GraphServiceClient(credentials=cred, scopes=scopes)
        return client

    async def list_all_drives(self) -> list[str]:
        resp = await self.client.sites.by_site_id(self.site_id).drives.get()
        drives = resp.value or []
        return [(d.id, d.name) for d in drives if getattr(d, "name", None) and getattr(d, "id", None)]

    async def download_drive(self, drive_id: str, drive_name: str) -> None:
        resp = await self.client.drives.by_drive_id(drive_id).items.by_drive_item_id("root").children.get()
        items = resp.value
        drive_path = os.path.join(self.timed_local_root, drive_name)
        os.makedirs(drive_path, exist_ok=True)

        for item in items:
            if getattr(item, "folder", None) is not None and getattr(item, "name", None):
                await self.download_folder(drive_id, drive_name, item)
            else:
                if getattr(item, "name", None):
                    await self.download_file(drive_id, drive_name, item)

    def make_folder(self, folder_path):
        real_path = os.path.join(self.timed_local_root, folder_path)
        os.makedirs(real_path, exist_ok=True)
        self.logger.debug(f"Created Folder: {real_path}")



    async def download_folder(self, drive_id: str, current_folder: str, folder_item: DriveItem) -> int:
        folder_path = os.path.join(current_folder, folder_item.name)
        self.make_folder(folder_path)

        resp = await self.client.drives.by_drive_id(drive_id).items.by_drive_item_id(folder_item.id).children.get()
        items = resp.value or []
        
        folder_total_size = 0
        
        for item in items:
            if getattr(item, "folder", None) is not None and getattr(item, "name", None):
                subfolder_size = await self.download_folder(drive_id, folder_path, item)
                folder_total_size += subfolder_size
            else:
                if getattr(item, "name", None):
                    file_size = await self.download_file(drive_id, folder_path, item)
                    folder_total_size += file_size
        
        return folder_total_size

            
    async def download_file(self, drive_id: str, folder_path: str, item: DriveItem) -> int:
        
        file_path = os.path.join(folder_path, item.name)
        content = await self.client.drives.by_drive_id(drive_id).items.by_drive_item_id(item.id).content.get()
        
        remote_content = content if isinstance(content, bytes) else b""
        remote_size = len(remote_content)
        
        if os.path.exists(os.path.join(self.timed_local_root, file_path)):
            local_size = os.path.getsize(file_path)
            
            if local_size != remote_size:
                self.logger.info(f"File {item.name} exists but sizes differ ({local_size} vs {remote_size}), downloading...")
            else:
                local_hash = await calculate_file_hash(file_path)
                remote_hash = hashlib.sha256(remote_content).hexdigest()
                
                if local_hash == remote_hash:
                    self.logger.info(f"File {item.name} exists and content is identical, skipping download.")
                    return 0
                else:
                    self.logger.info(f"File {item.name} exists but content differs, downloading...")
        else:
            self.logger.info(f"File {item.name} doesn't exist locally, downloading...")
        
        with open(os.path.join(self.timed_local_root, file_path), "wb") as f:
            f.write(remote_content)
        
        self.logger.info(f"Downloaded ({remote_size} bytes) {item.name}")
        return remote_size

async def calculate_file_hash(file_path: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()