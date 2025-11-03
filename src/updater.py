import logging
import os
from pathlib import Path
import shutil
import signal
import traceback
import aiofiles
import aiohttp
import json
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from .aux import file_changed, list_files_relative

SAVES_DIR = "saves"

GRAPH_BATCH_URL = "https://graph.microsoft.com/v1.0/$batch"
GRAPH_BATCH_LIMIT = 20

WORKER_LIMIT = 4

CHUNK_SIZE = 64 * 1024


class SharePointDownloader:

	def __init__(self, site_id, local_root, timestamp, tenant_id: str, client_id: str, client_secret: str):
		self.logger: logging.Logger = logging.getLogger(__name__)
		self.credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
		self.scopes = ["https://graph.microsoft.com/.default"]
		self.client: GraphServiceClient = SharePointDownloader.initializeClient(self.credential, self.scopes)
		self.site_id: str = site_id
		self.local_root: str = local_root
		self.timestamp: str = timestamp
		self.external_files: set[str] = set()
		self.drive_name_ids: set[tuple[str, str]] = set()

	@staticmethod
	def initializeClient(cred: ClientSecretCredential, scopes):
		client = GraphServiceClient(credentials=cred, scopes=scopes)
		return client

	async def initializeDriveNames(self, drive_names: list[str]) -> None:
		wanted = set(drive_names)
		resp = await self.client.sites.by_site_id(self.site_id).drives.get()
		found = {(d.id, d.name) for d in (resp.value or []) if getattr(d, "id", None) and getattr(d, "name", None)}
		missing = wanted - {name for _, name in found}
		if missing:
			self.logger.info(f"{missing} - Drive(s) don't exist, skipping...")
		self.drive_name_ids = {pair for pair in found if pair[1] in wanted}
