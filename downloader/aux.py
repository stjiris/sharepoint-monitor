import json
import os
from pathlib import Path


def env_or_fail(name: str) -> str:
	value = os.environ.get(name)
	if not value:
		raise RuntimeError(f"Missing environment variable {name}")
	return value


def list_files_relative(base: str, drive_name: str) -> set[str]:
	base_path = Path(base)
	if not base_path.exists():
		raise FileNotFoundError(f"Base path does not exist: {base_path!s}")
	if not base_path.is_dir():
		raise NotADirectoryError(f"Base path is not a directory: {base_path!s}")

	files = []
	for p in base_path.rglob('*'):
		if not p.is_file():
			continue
		rel = p.relative_to(base_path)
		rel = os.path.join(drive_name, rel.as_posix())
		files.append(Path(rel).parent.as_posix())

	return set(sorted(files))


def file_changed(file_path: str, size: int, xor_hash: str, url: str, creation_date: str, folder_rel: str) -> bool:
	if not os.path.exists(file_path) or not os.path.exists(os.path.join(os.path.dirname(file_path), "metadata.json")):
		return True

	with open(os.path.join(os.path.dirname(file_path), "metadata.json"), "r") as f:
		metadata = json.load(f)
	if metadata and metadata.get("size") == size and metadata.get("url") == url and metadata.get(
	    "creation_date") == creation_date and metadata.get("original_path") == folder_rel:
		return False
	else:
		return True
