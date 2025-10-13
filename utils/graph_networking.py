import asyncio
import signal
import logging
import os
from pathlib import Path
import shutil
import struct
from typing import Optional
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.drive_item import DriveItem
import base64
from .quickxorhash import quickxorhash_file_base64
from .aux import list_files_relative
LOGS_DIR = "logs"
SAVES_DIR = "saves"

class SharePointDownloader:
    def __init__(self, site_id, local_root, timestamp, tenant_id: str, client_id: str, client_secret: str):
        self.logger: logging.Logger = logging.getLogger(__name__)
        self.client: GraphServiceClient = SharePointDownloader.initializeClient(tenant_id, client_id, client_secret)
        self.site_id: str = site_id
        self.local_root: str = local_root
        self.timestamp: str = timestamp
        self.external_files: set[str] = set()
        self.drive_name: str = ""
        self.drive_name_ids: set[tuple[str, str]]

    def initializeClient(tenant_id: str, client_id: str, client_secret: str) -> GraphServiceClient:
        cred = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
        scopes = ['https://graph.microsoft.com/.default']
        client = GraphServiceClient(credentials=cred, scopes=scopes)
        return client

    async def getDriveNames(self, drive_names: list[str]):
        drive_names = set(drive_names)
        existing_drive_name_ids = await self.list_all_drives()
        existing_drive_names = set([name for _, name in existing_drive_name_ids])
        non_existing_drives = drive_names - existing_drive_names
        if non_existing_drives:
            self.logger.info(f'{non_existing_drives} - Drive(s) don\'t exist, skipping...')
        self.drive_name_ids = {
            (drive_id, drive_name)
            for drive_id, drive_name in existing_drive_name_ids
            if drive_name in drive_names
        }

    async def list_all_drives(self) -> set[tuple[str]]:
        resp = await self.client.sites.by_site_id(self.site_id).drives.get()
        drives = resp.value or []
        return [(d.id, d.name) for d in drives if getattr(d, "name", None) and getattr(d, "id", None)]

    async def download_drive(self, drive_id: str, drive_name: str) -> None:
        resp = await self.client.drives.by_drive_id(drive_id).items.by_drive_item_id("root").children.get()
        items = resp.value
        self.make_folder(drive_name)
        self.drive_name = drive_name

        for item in items:
            if getattr(item, "folder", None) is not None and getattr(item, "name", None):
                await self.download_folder(drive_id, drive_name, item)
            else:
                if getattr(item, "name", None):
                    await self.download_file(drive_id, drive_name, item)
        self.save_files()

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
        remote_size = 0
        self.external_files.add(file_path)

        if os.path.exists(os.path.join(full_file_path)):
            if item.file and item.file.hashes and item.file.hashes.quick_xor_hash:
                remote_hash = item.file.hashes.quick_xor_hash
                local_hash = quickxorhash_file_base64(full_file_path)

                if local_hash == remote_hash:
                    self.logger.info(f"SKIP --- {full_file_path} --- File exists and content is identical")
                    return remote_size
                else:
                    self.logger.info(f"UPDATE --- {full_file_path} --- File exists but content differs, downloading...")
                    self.save_outdated_file(file_path)
                    content = await self.client.drives.by_drive_id(drive_id).items.by_drive_item_id(item.id).content.get()
                    remote_content = content if isinstance(content, bytes) else b""
                    remote_size = item.size
                    write_file(full_file_path, remote_content)
        else:
            self.logger.info(f"INSERT --- {full_file_path} --- File doesn't exist locally, downloading...")
            remote_content = await self.client.drives.by_drive_id(drive_id).items.by_drive_item_id(item.id).content.get()
            remote_size = item.size
            write_file(full_file_path, remote_content)
        return remote_size
    
    def delete_outdated_files(self, file_path: str):
        file_path = os.path.join(self.local_root, file_path)
        os.remove(file_path)

    def save_files(self):
        drive_path = os.path.join(self.local_root, self.drive_name)
        existing_files: set[str] = list_files_relative(drive_path, self.drive_name)
        deleted_files = existing_files - self.external_files
        for deleted_file in deleted_files:
            self.save_outdated_file(deleted_file)
            self.delete_outdated_files(deleted_file)

    def make_folder(self, folder_path: str):
        folder_path = os.path.join(self.local_root, folder_path)
        os.makedirs(folder_path, exist_ok=True)
    
    def save_outdated_file(self, file_path: str):
        saves_dir = os.path.join(self.local_root, SAVES_DIR)
        saves_dir = os.path.join(saves_dir, self.timestamp)
        destination_folder_path = os.path.join(saves_dir, Path(file_path).parent)
        os.makedirs(destination_folder_path, exist_ok=True)
        destination_file_path = os.path.join(saves_dir, file_path)
        origin = os.path.join(self.local_root, file_path)
        shutil.copy(origin, destination_file_path)
    
    async def download_drives(self):
        for id, name in self.drive_name_ids:
            self.drive_name = name
            self.external_files = set()
            try:
                await self.download_drive(id, name)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("Error downloading drive %s (%s)", name, id)
        
def install_signal_handlers(loop: asyncio.AbstractEventLoop, downloader: SharePointDownloader, task: asyncio.Task):
    def _on_shutdown():
        try:
            loop.call_soon_threadsafe(downloader.save_files)
        except Exception:
            try:
                downloader.save_files()
            except Exception:
                downloader.logger.exception("Failed to call save_files from signal handler")

        if not task.done():
            task.cancel()

    loop.add_signal_handler(signal.SIGINT, _on_shutdown)
    loop.add_signal_handler(signal.SIGTERM, _on_shutdown)

def write_file(file_path: str, content: bytes):
    with open(os.path.join(file_path), "wb") as f:
        f.write(content)
