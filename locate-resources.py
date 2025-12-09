"""
Minecraft File Verifier and Downloader TUI
"""

import os
import json
import requests
import hashlib
import asyncio
import aiohttp
import time
from textual.app import App, ComposeResult
from textual.widgets import SelectionList, Button, ProgressBar, Label, Header, Footer
from textual.containers import Vertical
import argparse

VERSIONS_JSON = "https://launchermeta.mojang.com/mc/game/version_manifest.json"

ASSET_DOWNLOAD = "https://resources.download.minecraft.net/%s/%s"

ALL_OS = ['osx', 'linux', 'windows']

def fetch_json(url, filename):
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            return json.load(f)
    else:
        for attempt in range(3):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                dirname = os.path.dirname(filename)
                if dirname:
                    os.makedirs(dirname, exist_ok=True)
                with open(filename, "w") as f:
                    f.write(response.text)
                return response.json()
            except Exception as e:
                if attempt < 2:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise e

def parse_rules(rules):
    if not rules:
        return set(ALL_OS)

    allowed_os = set()

    for rule in rules:
        action = rule['action']
        change = set()
        if 'os' in rule:
            if not (action == 'disallow' and 'version' in rule['os']):
                change.add(rule['os']['name'])
        else:
            change = set(ALL_OS)

        if action == 'allow':
            allowed_os |= change
        else:
            allowed_os -= change
    return allowed_os

def get_libraries(libs):
    libraries = []
    for lib in libs:
        allowed_os = parse_rules(lib.get('rules', []))
        if 'downloads' in lib:
            if 'artifact' in lib['downloads']:
                libraries.append(lib['downloads']['artifact'])
            if 'classifiers' in lib['downloads']:
                for classifier, info in lib['downloads']['classifiers'].items():
                    if classifier.startswith('natives-'):
                        os_name = classifier[8:]
                        if os_name in allowed_os:
                            libraries.append(info)
    return libraries

def get_assets(asset_index_url, asset_id, cache_dir):
    index_file = os.path.join(cache_dir, 'assets', f"{asset_id}.json")
    index = fetch_json(asset_index_url, index_file)
    if not index:
        return []
    assets = []
    for obj in index.get('objects', {}).values():
        hash = obj['hash']
        url = ASSET_DOWNLOAD % (hash[:2], hash)
        local_path = os.path.join(cache_dir, 'assets', 'objects', hash[:2], hash)
        # The asset index's `hash` is the sha1 for the object, so include it
        assets.append({'url': url, 'path': local_path, 'sha1': hash})
    return assets

def compute_sha1(file_path):
    sha1 = hashlib.sha1()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            sha1.update(chunk)
    return sha1.hexdigest()

async def download_file_async(session, url, path, expected_sha=None):
    if os.path.exists(path):
        if expected_sha:
            if compute_sha1(path) == expected_sha:
                return False  # Already have
        else:
            return False  # Already have
    dirname = os.path.dirname(path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    async with session.get(url) as response:
        response.raise_for_status()
        content = await response.read()
        with open(path, 'wb') as f:
            f.write(content)
    if expected_sha:
        actual_sha = compute_sha1(path)
        if actual_sha != expected_sha:
            raise ValueError(f"SHA1 mismatch for {path}: expected {expected_sha}, got {actual_sha}")
    return True  # Downloaded


async def download_file_async_with_retry(session, url, path, expected_sha=None, retries=3, backoff=2):
    attempt = 0
    last_exc = None
    while attempt < retries:
        try:
            downloaded = await download_file_async(session, url, path, expected_sha)
            # If no download occurred, file exists and was valid
            if not downloaded:
                return False
            # If download happened, verify sha
            if expected_sha:
                actual = compute_sha1(path)
                if actual != expected_sha:
                    raise ValueError(f"SHA1 mismatch for {path} after download: expected {expected_sha}, got {actual}")
            return True
        except Exception as e:
            last_exc = e
            attempt += 1
            if attempt < retries:
                await asyncio.sleep(backoff ** attempt)
            else:
                raise

class MinecraftVerifier(App):
    CSS = """
    Screen {
        layout: vertical;
    }
    Vertical {
        height: 100%;
    }
    SelectionList {
        height: 50%;
        border: solid white;
    }
    ProgressBar {
        margin: 1;
    }
    """

    def __init__(self):
        super().__init__()
        self.installed_versions = []
        self.selected_versions = []

    def compose(self) -> ComposeResult:
        yield Header()
        yield Vertical(
            SelectionList(id="versions"),
            Button("Start Verification", id="start", disabled=True),
            ProgressBar(id="progress", total=100),
            Label("Status: Loading versions...", id="status"),
        )
        yield Footer()

    def on_mount(self):
        self.load_versions()

    def load_versions(self):
        # Find Minecraft directory
        appdata = os.environ.get('APPDATA')
        if not appdata:
            self.query_one("#status").update("Error: APPDATA not found")
            return
        minecraft_dir = os.path.join(appdata, '.minecraft')
        if not os.path.exists(minecraft_dir):
            self.query_one("#status").update(f"Error: Minecraft directory not found: {minecraft_dir}")
            return

        versions_dir = os.path.join(minecraft_dir, 'versions')
        if not os.path.exists(versions_dir):
            self.query_one("#status").update(f"Error: Versions directory not found: {versions_dir}")
            return

        # Get all installed versions
        for item in os.listdir(versions_dir):
            version_path = os.path.join(versions_dir, item)
            if os.path.isdir(version_path):
                json_file = os.path.join(version_path, f"{item}.json")
                jar_file = os.path.join(version_path, f"{item}.jar")
                if os.path.exists(json_file):
                    self.installed_versions.append((item, json_file, jar_file))

        selection_list = self.query_one("#versions")
        for version, _, _ in self.installed_versions:
            selection_list.add_option((version, version))

        self.query_one("#status").update(f"Found {len(self.installed_versions)} installed versions. Select versions and click Start.")
        self.query_one("#start").disabled = False

    def on_button_pressed(self, event: Button.Pressed):
        if event.button.id == "start":
            selection_list = self.query_one("#versions")
            selected = selection_list.selected
            self.selected_versions = [v for v in self.installed_versions if v[0] in selected]
            if not self.selected_versions:
                self.query_one("#status").update("No versions selected.")
                return
            self.query_one("#start").disabled = True
            self.query_one("#status").update("Processing selected versions...")
            self.run_worker(self.verify_and_download())

    async def verify_and_download(self):
        minecraft_dir = os.path.join(os.environ['APPDATA'], '.minecraft')
        tasks = []
        for version, json_file, jar_file in self.selected_versions:
            try:
                version_json = json.load(open(json_file))
            except Exception as e:
                self.query_one("#status").update(f"Error loading {json_file}: {e}")
                continue

            # Check client jar
            if 'downloads' in version_json and 'client' in version_json['downloads']:
                client_info = version_json['downloads']['client']
                tasks.append((client_info['url'], jar_file, client_info.get('sha1')))

            # Get libraries
            libs = get_libraries(version_json.get('libraries', []))
            for lib in libs:
                url = lib.get('url')
                # `path` in the artifact/classifier info provides the relative library path
                rel = lib.get('path')
                if rel:
                    local_path = os.path.join(minecraft_dir, 'libraries', *rel.split('/'))
                else:
                    # Fallback: compute from libraries.minecraft.net URL
                    local_path = url.replace('https://libraries.minecraft.net/', '')
                    local_path = os.path.join(minecraft_dir, 'libraries', local_path)
                tasks.append((url, local_path, lib.get('sha1')))

            # Get assets
            if 'assetIndex' in version_json:
                asset_index_url = version_json['assetIndex']['url']
                asset_id = version_json['assetIndex']['id']
                assets = get_assets(asset_index_url, asset_id, minecraft_dir)
                for asset in assets:
                    tasks.append((asset['url'], asset['path'], asset['sha1']))

        # Filter tasks to only missing ones
        missing_tasks = []
        for url, path, sha in tasks:
            if not os.path.exists(path):
                missing_tasks.append((url, path, sha))
            elif sha and compute_sha1(path) != sha:
                missing_tasks.append((url, path, sha))

        if not missing_tasks:
            self.query_one("#status").update("All files are up to date!")
            self.query_one("#start").disabled = False
            return

        self.query_one("#progress").total = len(missing_tasks)
        self.query_one("#progress").value = 0
        self.query_one("#status").update(f"Downloading {len(missing_tasks)} files...")

        async def reliable_download(session, url, path, sha, retries=3):
            # Use the download_file_async_with_retry wrapper which already has backoff and sha verification
            return await download_file_async_with_retry(session, url, path, sha, retries)

        async def download_with_progress(session, url, path, sha):
            try:
                downloaded = await reliable_download(session, url, path, sha)
                if downloaded:
                    self.query_one("#progress").advance(1)
            except Exception as e:
                # Update status with a brief message but continue with other downloads
                self.query_one("#status").update(f"Error downloading {url}: {e}")

        async with aiohttp.ClientSession() as session:
            semaphore = asyncio.Semaphore(10)
            async def download_task(url, path, sha):
                async with semaphore:
                    await download_with_progress(session, url, path, sha)
            await asyncio.gather(*[download_task(url, path, sha) for url, path, sha in missing_tasks])

        self.query_one("#status").update("Verification and download complete!")
        self.query_one("#start").disabled = False

async def headless(args):
    # Find Minecraft directory
    appdata = os.environ.get('APPDATA')
    if not appdata:
        print('APPDATA not found')
        return
    minecraft_dir = os.path.join(appdata, '.minecraft')
    if not os.path.exists(minecraft_dir):
        print(f'Minecraft directory not found: {minecraft_dir}')
        return
    versions_dir = os.path.join(minecraft_dir, 'versions')
    if not os.path.exists(versions_dir):
        print(f'Versions directory not found: {versions_dir}')
        return

    # Build list of installed versions
    installed_versions = []
    for item in os.listdir(versions_dir):
        version_path = os.path.join(versions_dir, item)
        if os.path.isdir(version_path):
            json_file = os.path.join(version_path, f"{item}.json")
            jar_file = os.path.join(version_path, f"{item}.jar")
            if os.path.exists(json_file):
                installed_versions.append((item, json_file, jar_file))

    print(f'Found {len(installed_versions)} installed versions')
    # Collect tasks
    tasks = []
    for version, json_file, jar_file in installed_versions:
        print(f'Processing {version}')
        try:
            version_json = json.load(open(json_file))
        except Exception as e:
            print(f'Error loading {json_file}: {e}')
            continue
        if 'downloads' in version_json and 'client' in version_json['downloads']:
            c = version_json['downloads']['client']
            tasks.append((c['url'], jar_file, c.get('sha1')))
        libs = get_libraries(version_json.get('libraries', []))
        for lib in libs:
            url = lib.get('url')
            rel = lib.get('path')
            if rel:
                local_path = os.path.join(minecraft_dir, 'libraries', *rel.split('/'))
            else:
                local_path = url.replace('https://libraries.minecraft.net/', '')
                local_path = os.path.join(minecraft_dir, 'libraries', local_path)
            tasks.append((url, local_path, lib.get('sha1')))
        if 'assetIndex' in version_json:
            asset_info = version_json['assetIndex']
            assets = get_assets(asset_info['url'], asset_info['id'], minecraft_dir)
            for a in assets:
                # Use asset index hash as expected SHA1
                tasks.append((a['url'], a['path'], a.get('sha1')))

    # Filter tasks to missing or mismatched sha
    to_download = []
    for url, path, sha in tasks:
        if not sha:
            # For assets we sometimes have no explicit sha; try to derive from url/known hash
            # For assets we set sha to hash in get_assets
            pass
        if not os.path.exists(path):
            to_download.append((url, path, sha, 'missing'))
        elif sha and compute_sha1(path) != sha:
            to_download.append((url, path, sha, 'corrupt'))

    if not to_download:
        print('All files are up to date and valid')
        return

    print(f'Files to fetch: {len(to_download)}')
    for url, path, sha, reason in to_download:
        print(f'  - {reason}: {path} (sha: {sha})')

    if args.dry_run:
        print('Dry run; not downloading any files')
        return

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(8)
        async def dl(url, path, sha):
            async with semaphore:
                try:
                    ok = await download_file_async_with_retry(session, url, path, sha)
                    print(f'Downloaded: {path}') if ok else print(f'OK: {path}')
                except Exception as e:
                    print(f'Error downloading {url} -> {path}: {e}')
        await asyncio.gather(*[dl(url, path, sha) for url, path, sha, _ in to_download])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Minecraft resource verifier and downloader')
    parser.add_argument('--nogui', action='store_true', help='Run in headless CLI mode')
    parser.add_argument('--dry-run', action='store_true', help='List missing/corrupt files without downloading')
    args = parser.parse_args()
    if args.nogui:
        # Run CLI headless mode
        asyncio.run(headless(args))
    else:
        app = MinecraftVerifier()
        app.run()

def main():
    parser = argparse.ArgumentParser(description='Minecraft resource verifier and downloader')
    parser.add_argument('--nogui', action='store_true', help='Run in headless CLI mode')
    parser.add_argument('--dry-run', action='store_true', help='List missing/corrupt files without downloading')
    args = parser.parse_args()
    if args.nogui:
        # Run CLI headless mode
        asyncio.run(headless(args))
    else:
        app = MinecraftVerifier()
        app.run()