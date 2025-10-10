import hashlib
import logging
import os
import shutil
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.drive_item import DriveItem

LOGS_DIR = "logs"
SAVES_DIR = "saves"

class SharePointDownloader:
    def __init__(self, site_id, local_root, timestamp, tenant_id: str, client_id: str, client_secret: str):
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.client: GraphServiceClient = SharePointDownloader.initializeClient(tenant_id, client_id, client_secret)
        self.site_id: str = site_id
        self.local_root: str = local_root
        self.timestamp: str = timestamp

    def initializeClient(tenant_id: str, client_id: str, client_secret: str) -> GraphServiceClient:
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
        self.make_folder(drive_name)

        for item in items:
            if getattr(item, "folder", None) is not None and getattr(item, "name", None):
                await self.download_folder(drive_id, drive_name, item)
            else:
                if getattr(item, "name", None):
                    await self.download_file(drive_id, drive_name, item)

    async def list_listeners(self):
        result = await self.client.identity.authentication_event_listeners.get()
        print(result)

    ###################
    ### AUX METHODS ###
    ###################
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
        full_file_path = os.path.join(self.local_root, file_path)

        content = await self.client.drives.by_drive_id(drive_id).items.by_drive_item_id(item.id).content.get()
        
        remote_content = content if isinstance(content, bytes) else b""
        remote_size = len(remote_content)

        if os.path.exists(os.path.join(full_file_path)):
            local_size = os.path.getsize(full_file_path)

            if local_size != remote_size:
                self.logger.info(f"UPDATE --- {full_file_path} --- File exists but content differs, downloading...")
                self.save_outdated_file(folder_path, file_path)
                write_file(full_file_path, remote_content)
            else:
                local_hash = await calculate_file_hash(full_file_path)
                remote_hash = hashlib.sha256(remote_content).hexdigest()
                
                if local_hash == remote_hash:
                    self.logger.info(f"SKIP --- {full_file_path} --- File exists and content is identical")
                    return 0
                else:
                    self.logger.info(f"UPDATE --- {full_file_path} --- File exists but content differs, downloading...")
                    self.save_outdated_file(folder_path, file_path)
                    write_file(full_file_path, remote_content)

        else:
            self.logger.info(f"INSERT --- {full_file_path} --- File doesn't exist locally, downloading...")
            write_file(full_file_path, remote_content)
        return remote_size
    
    def make_folder(self, folder_path: str):
        folder_path = os.path.join(self.local_root, folder_path)
        os.makedirs(folder_path, exist_ok=True)
    
    def save_outdated_file(self, folder_path: str, file_path: str):
        saves_dir = os.path.join(self.local_root, SAVES_DIR)
        saves_dir = os.path.join(saves_dir, self.timestamp)
        destination_folder_path = os.path.join(saves_dir, folder_path)

        os.makedirs(destination_folder_path, exist_ok=True)

        destination_file_path = os.path.join(saves_dir, file_path)
        
        origin = os.path.join(self.local_root, file_path)
        shutil.copy(origin, destination_file_path)

        print("Copied file: " + origin + " to file " + destination_file_path)
        

async def calculate_file_hash(file_path: str) -> str:
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()

def write_file(file_path: str, content: bytes):
    with open(os.path.join(file_path), "wb") as f:
        f.write(content)
