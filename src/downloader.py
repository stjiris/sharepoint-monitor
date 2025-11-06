import asyncio
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

		self.pending: list[dict] = []

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

	async def download_drives(self):
		for id, name in self.drive_name_ids:
			self.external_files = set()
			try:
				await self.download_drive(id, name)
			except asyncio.CancelledError:
				raise
			except Exception:
				self.logger.error("Error downloading drive %s (%s)", name, id)
				self.logger.error(traceback.format_exc())

	async def download_drive(self, drive_id: str, drive_name: str) -> None:
		resp = await self.client.drives.by_drive_id(drive_id).items.by_drive_item_id("root").children.get()
		items = resp.value or []

		self.make_folder(drive_name)

		for item in items:
			name = getattr(item, "name", None)
			if not name:
				continue
			if getattr(item, "folder", None) is not None:
				await self.collect_folder_files(drive_id, drive_name, item)
			else:
				self.pending.append({"drive_id": drive_id, "folder_path": drive_name, "item": item})

		while True:
			await self.process_pending_files(True)
			if not self.pending:
				break

		#self.save_outdated_files(drive_name)

	async def collect_folder_files(self, drive_id: str, current_folder: str, folder_item) -> list[dict]:
		path = os.path.join(current_folder, folder_item.name)
		self.make_folder(path)

		resp = await self.client.drives.by_drive_id(drive_id).items.by_drive_item_id(folder_item.id).children.get()
		items = resp.value or []

		for it in items:
			name = getattr(it, "name", None)
			if not name:
				continue
			if getattr(it, "folder", None) is not None:
				await self.collect_folder_files(drive_id, path, it)
			else:
				self.pending.append({"drive_id": drive_id, "folder_path": path, "item": it})
		await self.process_pending_files()
		return self.pending

	async def process_pending_files(self, final: bool = False) -> None:
		if not final and len(self.pending) < GRAPH_BATCH_LIMIT:
			return

		if len(self.pending) >= GRAPH_BATCH_LIMIT:
			batch = self.pending[:GRAPH_BATCH_LIMIT]
			self.pending = self.pending[GRAPH_BATCH_LIMIT:]
		else:
			batch = self.pending
			self.pending = []
		await self.process_batch(batch)

	async def process_batch(self, batch: list[dict]) -> None:
		session = None
		try:
			try:
				token = self._get_bearer_token()
			except Exception:
				self.logger.error("process_batch failed to obtain token; re-queueing batch")
				self.pending = batch + self.pending
				return

			timeout = aiohttp.ClientTimeout(total=60)
			session = aiohttp.ClientSession(timeout=timeout)
			requests_payload = []
			#self.logger.info(f"Processing batch of {len(batch)} items")
			for j, e in enumerate(batch):
				requests_payload.append({
				    "id":
				    str(j),
				    "method":
				    "GET",
				    "url":
				    f"/drives/{e['drive_id']}/items/{e['item'].id}?$select=id,name,size,@microsoft.graph.downloadUrl,file,hashes"
				})

			headers = {"Authorization": token, "Content-Type": "application/json"}
			try:
				async with session.post(GRAPH_BATCH_URL, json={"requests": requests_payload}, headers=headers) as resp:
					if resp.status == 200:
						try:
							res_json = await asyncio.wait_for(resp.json(), timeout=30)
							responses = {r["id"]: r for r in res_json.get("responses", [])}
						except asyncio.TimeoutError:
							self.logger.error("Timed out reading batch JSON response")
							responses = {}
					else:
						self.logger.error(f"Graph batch failed: {resp.status}")
						responses = {}
			except asyncio.CancelledError:
				self.logger.error("process_batch cancelled during batch POST; re-queueing batch")
				self.pending = batch + self.pending
				return
			except Exception:
				self.logger.error("Exception when posting batch to Graph")
				responses = {}

			for j, entry in enumerate(batch):
				try:
					resp_item = responses.get(str(j), {})
					body = resp_item.get("body", {}) if resp_item.get("status") == 200 else {}
					download_url = body.get("@microsoft.graph.downloadUrl")
					size = body.get("size") or getattr(entry["item"], "size", 0)
					hashes = body.get("file", {}).get("hashes",
					                                  {}) if body.get("file") else body.get("hashes", {}) or {}
					xor_hash = hashes.get("quickXorHash")
					web_url = entry["item"].web_url
					creation_date = entry["item"].created_date_time

					# get only the day
					creation_date = creation_date.strftime("%Y-%m-%d")
					folder_rel = os.path.join(entry["folder_path"], entry["item"].name)
					full_folder = os.path.join(self.local_root, folder_rel)
					full_file = os.path.join(full_folder, entry["item"].name)

					if not file_changed(full_file, size, xor_hash, web_url, creation_date, folder_rel):
						self.logger.info(f"SKIP --- File {folder_rel} up to date")
						#self.external_files.add(folder_rel)
						continue

					if download_url:
						url, hdrs = download_url, None
					else:
						url = f"https://graph.microsoft.com/v1.0/drives/{entry['drive_id']}/items/{entry['item'].id}/content"
						hdrs = {"Authorization": token}

					try:
						async with session.get(url, headers=hdrs) as r:
							if r.status not in (200, 206):
								self.logger.error(f"Failed to download {folder_rel}: {r.status}")
								self.pending.append({
								    "drive_id": entry["drive_id"],
								    "folder_path": os.path.dirname(folder_rel),
								    "item": entry["item"]
								})
								continue
							os.makedirs(full_folder, exist_ok=True)

							if os.path.exists(full_file):
								self.logger.info(f"UPDATE --- File {folder_rel} outdated, updating...")
								self.save_outdated_file(folder_rel)
							else:
								self.logger.info(f"INSERT --- File {folder_rel} new, inserting...")

							async with aiofiles.open(full_file, "wb") as fh:
								async for chunk in r.content.iter_chunked(CHUNK_SIZE):
									await fh.write(chunk)

							async with aiofiles.open(os.path.join(full_folder, "metadata.json"), "w",
							                         encoding="utf-8") as mf:
								metadata = {"size": size, "original_path": Path(folder_rel).as_posix()}
								if xor_hash:
									metadata["xor_hash"] = xor_hash
								if web_url:
									metadata["url"] = web_url
								if creation_date:
									metadata["creation_date"] = creation_date
								await mf.write(json.dumps(metadata, ensure_ascii=False, indent=2))

							#self.external_files.add(folder_rel)
					except asyncio.CancelledError:
						self.logger.error("Download cancelled; re-queueing remaining items")
						remaining = batch[j:]
						self.pending = remaining + self.pending
						return
					except Exception:
						self.logger.error(traceback.format_exc())
						self.logger.error(f"Error downloading {entry['item'].name}")

				except Exception:
					self.logger.error(traceback.format_exc())
					self.logger.error(
					    f"Error processing entry {str(entry['item'].size) + ' ' + entry['folder_path'] + entry['item'].name}"
					)
					remaining = batch[j:]
					self.pending = remaining + self.pending
					return
			#self.logger.info(f"Finished processing batch of {len(batch)} items")

		except asyncio.CancelledError:
			self.logger.info("Worker cancelled; re-queueing entire batch")
			self.pending = batch + self.pending
			raise
		except Exception:
			self.logger.error("Worker encountered an exception")
			raise
		finally:
			try:
				if session:
					await session.close()
			except Exception:
				pass

	def _get_bearer_token(self) -> str:
		try:
			tok = self.credential.get_token(*self.scopes)
			return f"Bearer {tok.token}"
		except Exception:
			self.logger.error("Failed to obtain token")
			raise

	def delete_outdated_files(self, file_path: str) -> None:
		file_path = os.path.join(self.local_root, file_path)
		shutil.rmtree(file_path)
		self.logger.info(f"Deleted from official repository: {file_path}")

	def save_outdated_files(self, drive_name: str, finished: bool = False) -> None:
		drive_path = os.path.join(self.local_root, drive_name)
		existing_files: set[str] = list_files_relative(drive_path, drive_name)

		deleted_files = existing_files - self.external_files
		for deleted_file in deleted_files:
			self.save_outdated_file(deleted_file)
			if finished:
				self.delete_outdated_files(deleted_file)

	def make_folder(self, folder_path: str) -> None:
		folder_path = os.path.join(self.local_root, folder_path)
		os.makedirs(folder_path, exist_ok=True)

	def save_outdated_file(self, file_path: str) -> None:
		saves_dir = os.path.join(self.local_root, SAVES_DIR)
		saves_dir = os.path.join(saves_dir, self.timestamp)
		destination_folder_path = os.path.join(saves_dir, file_path)
		os.makedirs(destination_folder_path, exist_ok=True)
		origin = os.path.join(self.local_root, file_path)
		shutil.copytree(origin, destination_folder_path, dirs_exist_ok=True)
		self.logger.info(f"Saved outdated file: {file_path}")


def install_signal_handlers(loop: asyncio.AbstractEventLoop, downloader: SharePointDownloader, task: asyncio.Task):

	def _on_shutdown():
		try:
			loop.call_soon_threadsafe(downloader.save_outdated_files)
		except Exception:
			try:
				downloader.save_outdated_files()
			except Exception:
				downloader.logger.error("Failed to call save_files from signal handler")

		if not task.done():
			task.cancel()

	loop.add_signal_handler(signal.SIGINT, _on_shutdown)
	loop.add_signal_handler(signal.SIGTERM, _on_shutdown)
