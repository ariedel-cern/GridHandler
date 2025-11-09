import os
import logging
import ROOT
from multiprocessing import Pool, current_process

try:
    import alienpy.alien as alien
except ImportError:
    alien = None

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,  # log everything by default
    format="%(asctime)s [%(levelname)s] [PID %(process)d] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# per work grid connection
_worker_grid_connection = None  # process-local TGrid connection


class GridHandler:
    def __init__(self, config: dict):
        self.backend = config.get("backend", "TGrid")
        self.output_dir = config.get("output_dir", "grid")
        self.remote_files = config.get("remote_files", None)
        self.remote_files_glob = config.get("remote_files_glob", None)
        self.num_workers = config.get("num_workers", 16)
        self.keep_depth = config.get("keep_depth", 5)
        self.alien_args = config.get(
            "alien_xrd_args", ["-timeout", "600", "-retry", "3"]
        )
        self.alien_args.append(["-T", f"{self.num_workers}"])

        os.makedirs(self.output_dir, exist_ok=True)
        logger.debug(
            f"Initialized GridHandler with backend {self.backend}, output_dir {self.output_dir}"
        )

        self.alien_session = None

    # Helper functions
    def _unique_local_path(self, remote_path: str) -> str:
        parts = remote_path.strip("/").split("/")
        filename = parts[-1]

        # keep full directory structure relative to last 'keep_depth' dirs if set
        if self.keep_depth is None:
            return os.path.join(self.output_dir, *parts)  # preserve full path
        else:
            sub_path = os.path.join(
                *parts[-(self.keep_depth + 1) : -1]
            )  # last N dirs before filename
            return os.path.join(self.output_dir, sub_path, filename)

    def _auto_unique_path(self, remote_path, filename):
        seen = set()
        for depth in range(1, 10):
            local_name = "_".join(remote_path.strip("/").split("/")[-depth:])
            if local_name not in seen:
                seen.add(local_name)
                return os.path.join(self.output_dir, local_name)
        return os.path.join(self.output_dir, filename)

    # AliEn connection
    def _ensure_alien_connection(self):
        if not alien:
            logger.error("❌ alienpy not installed.")
            return None
        if self.alien_session is None:
            logger.info("🔗 Connecting to AliEn...")
            self.alien_session = alien.InitConnection()
            if not self.alien_session:
                logger.error("❌ Failed to connect to AliEn.")
        return self.alien_session

    # Download helpers
    def _download_file(self, args):
        remote_file, local_file = args
        os.makedirs(os.path.dirname(local_file), exist_ok=True)

        if os.path.exists(local_file):
            logger.info(
                f"✅ [Worker {current_process().pid}] Skipping existing file: {local_file}"
            )
            return local_file

        if self.backend == "TGrid":
            return self._download_tgrid(remote_file, local_file)
        elif self.backend == "alienpy":
            logger.warning(
                f"⚠️ [Worker {current_process().pid}] alienpy backend does not use multiprocessing. Skipping."
            )
            return None
        else:
            logger.error(f"❌ Unknown backend: {self.backend}")
            return None

    def _download_tgrid(self, remote_file, local_file):
        global _worker_grid_connection

        if _worker_grid_connection is None:
            ROOT.gROOT.ProcessLine('TGrid::Connect("alien://")')
            _worker_grid_connection = ROOT.gGrid
            if not _worker_grid_connection or not _worker_grid_connection.IsConnected():
                logger.error(
                    f"❌ [Worker {current_process().pid}] TGrid connection failed"
                )
                return None

        grid = _worker_grid_connection

        src = (
            f"alien://{remote_file}"
            if not remote_file.startswith("alien://")
            else remote_file
        )
        dst = f"file:{local_file}" if not local_file.startswith("file:") else local_file

        logger.info(f"⬇️ [Worker {current_process().pid}] {src} → {local_file}")
        if ROOT.TFile.Cp(src, dst):
            return local_file
        else:
            logger.error(f"❌ Download failed: {remote_file}")
            return None

    def _download_alien(self, remote_files, local_files):
        session = self._ensure_alien_connection()
        if not session:
            return 0

        prefixed_remote = [
            f"alien://{r}" if not r.startswith("alien://") else r for r in remote_files
        ]
        prefixed_local = [
            f"file:{l}" if not l.startswith("file:") else l for l in local_files
        ]

        logger.info(f"⬇️ Starting AliEn download of {len(remote_files)} files...")
        try:
            result = alien.DO_XrootdCp(
                wb=session,
                xrd_copy_command=self.alien_args,
                api_src=prefixed_remote,
                api_dst=prefixed_local,
            )
        except Exception as e:
            logger.error(f"❌ AliEn download failed: {e}")
            return 0

        logger.info("✅ AliEn download finished successfully.")
        return len(remote_files)

    # resolve remote_files_glob (always uses alien)
    def _resolve_remote_globs(self):
        """Find files using alien.DO_find2 regardless of backend."""
        found_files = []
        if not self.remote_files_glob:
            return found_files

        if not alien:
            logger.error(
                "❌ alienpy is required for remote_files_glob but not installed."
            )
            return found_files

        session = self._ensure_alien_connection()
        if not session:
            return found_files

        logger.info(
            f"🔍 Resolving {len(self.remote_files_glob)} glob search entries via AliEn..."
        )

        for base, pattern in self.remote_files_glob:
            search_args = ["-glob", pattern, base]
            try:
                result = alien.DO_find2(session, search_args)
                if not result or not result.out:
                    logger.warning(f"⚠️ No results for {base}/{pattern}")
                    continue
                files = [f for f in result.out.split("\n") if f.strip()]
                logger.debug(f"Found {len(files)} files in {base} matching {pattern}")
                found_files.extend(files)
            except Exception as e:
                logger.error(f"❌ Error during alien find in {base}: {e}")

        return found_files

    # Main download
    def download(self):
        # Step 1: expand remote globs
        found_files = self._resolve_remote_globs()
        if found_files:
            logger.info(f"📂 Found {len(found_files)} files via remote_files_glob")
            if self.remote_files:
                self.remote_files.extend(found_files)
            else:
                self.remote_files = found_files

        if not self.remote_files:
            logger.warning("⚠️ No remote files specified.")
            return

        # Step 2: build local paths
        copy_list = [(f, self._unique_local_path(f)) for f in self.remote_files]
        logger.info(
            f"🚀 Starting download using backend: {self.backend} with {len(copy_list)} files"
        )

        # Step 3: perform download
        if self.backend == "TGrid":
            # Remove alien object to allow pickling
            if hasattr(self, "alien_session"):
                del self.alien_session
            with Pool(processes=self.num_workers) as pool:
                results = pool.map(self._download_file, copy_list)
            n_ok = sum(r is not None for r in results)

        elif self.backend == "alienpy":
            remote_files, local_files = zip(*copy_list)
            n_ok = self._download_alien(remote_files, local_files)

        else:
            logger.error(f"❌ Unsupported backend: {self.backend}")
            return

        logger.info(f"✅ Done. {n_ok}/{len(copy_list)} files downloaded.")
