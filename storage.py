"""
Enterprise Web Scraping Framework - Storage Module Implementation
"""
import json
import os
import pickle
import sqlite3
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import logging
import hashlib

from core_framework import (
    StorageInterface, ScrapedResponse, ParsedData, ConfigProvider
)


class DatabaseManager:
    """Thread-safe database connection manager"""

    def __init__(self, db_path: str, pragmas: Optional[Dict[str, Any]] = None):
        self.db_path = db_path
        self.pragmas = pragmas or {
            "journal_mode": "WAL",
            "synchronous": "NORMAL",
            "cache_size": -1024 * 32,  # 32MB cache
            "foreign_keys": "ON"
        }
        self._local = threading.local()
        self._ensure_db_directory()
        self._initialize_db()
        self.logger = logging.getLogger("database_manager")

    def _ensure_db_directory(self) -> None:
        """Ensure the directory for database file exists"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

    def _initialize_db(self) -> None:
        """Initialize database schema"""
        with self.get_connection() as conn:
            # Create responses table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS responses (
                    url TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    headers TEXT NOT NULL,
                    content BLOB,
                    content_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    response_meta TEXT
                )
            """)

            # Create data table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS extracted_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    schema_name TEXT NOT NULL,
                    data TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    validated BOOLEAN NOT NULL,
                    FOREIGN KEY (url) REFERENCES responses(url)
                )
            """)

            # Create index on task_id
            conn.execute("CREATE INDEX IF NOT EXISTS idx_responses_task_id ON responses(task_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_data_task_id ON extracted_data(task_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_data_schema ON extracted_data(schema_name)")

            # Create crawler state table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS crawler_state (
                    id TEXT PRIMARY KEY,
                    state BLOB NOT NULL,
                    timestamp TEXT NOT NULL
                )
            """)

    @contextmanager
    def get_connection(self):
        """Get a connection to the database in a thread-safe way"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.connect(self.db_path)

            # Apply pragmas
            for pragma, value in self.pragmas.items():
                self._local.connection.execute(f"PRAGMA {pragma} = {value}")

        try:
            yield self._local.connection
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            # Force new connection on next use
            if hasattr(self._local, 'connection'):
                try:
                    self._local.connection.close()
                except Exception:
                    pass
                del self._local.connection
            raise

    def close_all(self) -> None:
        """Close all database connections"""
        if hasattr(self._local, 'connection'):
            try:
                self._local.connection.close()
            except Exception as e:
                self.logger.error(f"Error closing connection: {e}")
            del self._local.connection


class CacheManager:
    """Manages caching of scraped responses"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.memory_cache: Dict[str, Tuple[ScrapedResponse, float]] = {}
        self.cache_lock = threading.RLock()
        self.logger = logging.getLogger("cache_manager")

    def store(self, response: ScrapedResponse) -> None:
        """Store a response in both memory and disk cache"""
        url = response.task.url

        # Store in memory cache
        with self.cache_lock:
            self.memory_cache[url] = (response, time.time())

            # Maintain memory cache size
            if len(self.memory_cache) > 1000:  # Limit to 1000 entries
                # Remove oldest 20% entries
                items = sorted(self.memory_cache.items(), key=lambda x: x[1][1])
                for key, _ in items[:len(items) // 5]:
                    del self.memory_cache[key]

        # Store in database
        with self.db_manager.get_connection() as conn:
            try:
                headers_json = json.dumps(response.headers)
                response_meta = {
                    "session_id": response.session_id,
                    "proxy_used": response.proxy_used,
                    "final_url": response.final_url,
                    "fetch_time": response.fetch_time
                }
                response_meta_json = json.dumps(response_meta)

                conn.execute(
                    """
                    INSERT OR REPLACE INTO responses
                    (url, task_id, status_code, headers, content, content_type, timestamp, response_meta)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        url,
                        response.task.task_id,
                        response.status_code,
                        headers_json,
                        response.content,
                        response.content_type,
                        response.timestamp.isoformat(),
                        response_meta_json
                    )
                )
                conn.commit()
            except Exception as e:
                self.logger.error(f"Error storing response for {url}: {e}")
                conn.rollback()
                raise

    def get(self, url: str, max_age_seconds: int) -> Optional[ScrapedResponse]:
        """Get a cached response if available and not expired"""
        # Check memory cache first
        with self.cache_lock:
            if url in self.memory_cache:
                response, timestamp = self.memory_cache[url]
                if time.time() - timestamp <= max_age_seconds:
                    return response

        # Check database cache
        with self.db_manager.get_connection() as conn:
            try:
                query = """
                    SELECT 
                        task_id, status_code, headers, content, content_type, 
                        timestamp, response_meta
                    FROM responses
                    WHERE url = ?
                """
                cursor = conn.execute(query, (url,))
                row = cursor.fetchone()

                if row:
                    task_id, status_code, headers_json, content, content_type, timestamp_str, response_meta_json = row
                    timestamp = datetime.fromisoformat(timestamp_str)

                    # Check if cache is expired
                    if datetime.now() - timestamp > timedelta(seconds=max_age_seconds):
                        return None

                    # Reconstruct task
                    from core_framework import ScrapingTask  # Import here to avoid circular imports
                    task = ScrapingTask(url=url, task_id=task_id)

                    # Parse headers and metadata
                    headers = json.loads(headers_json)
                    response_meta = json.loads(response_meta_json)

                    # Create response object
                    response = ScrapedResponse(
                        task=task,
                        status_code=status_code,
                        headers=headers,
                        content=content,
                        content_type=content_type,
                        fetch_time=response_meta.get("fetch_time", 0.0),
                        timestamp=timestamp,
                        session_id=response_meta.get("session_id"),
                        proxy_used=response_meta.get("proxy_used"),
                        final_url=response_meta.get("final_url")
                    )

                    # Update memory cache
                    with self.cache_lock:
                        self.memory_cache[url] = (response, time.time())

                    return response
            except Exception as e:
                self.logger.error(f"Error retrieving cached response for {url}: {e}")
                return None

        return None


class FileSystemStorage:
    """Manages storage of large files and backups"""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.content_dir = self.base_dir / "content"
        self.backup_dir = self.base_dir / "backups"
        self.state_dir = self.base_dir / "state"

        # Create directories
        self.content_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger("filesystem_storage")

    def store_file(self, content: bytes, file_type: str, metadata: Dict[str, Any]) -> str:
        """Store a file on the filesystem and return its path"""
        # Generate a unique filename based on content hash and metadata
        content_hash = hashlib.sha256(content).hexdigest()
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

        # Create subdirectory based on first 2 chars of hash for better distribution
        subdir = self.content_dir / content_hash[:2]
        subdir.mkdir(exist_ok=True)

        # Create filename with metadata
        url_part = metadata.get("url", "unknown")
        url_hash = hashlib.md5(url_part.encode()).hexdigest()[:8]

        filename = f"{timestamp}_{url_hash}_{content_hash[:10]}.{file_type}"
        filepath = subdir / filename

        # Write content to file
        with open(filepath, "wb") as f:
            f.write(content)

        # Write metadata
        meta_path = filepath.with_suffix(".meta")
        with open(meta_path, "w") as f:
            json.dump(metadata, f)

        return str(filepath.relative_to(self.base_dir))

    def get_file(self, relative_path: str) -> Tuple[Optional[bytes], Optional[Dict[str, Any]]]:
        """Retrieve a file and its metadata"""
        full_path = self.base_dir / relative_path

        if not full_path.exists():
            return None, None

        # Read content
        with open(full_path, "rb") as f:
            content = f.read()

        # Read metadata if available
        meta_path = full_path.with_suffix(".meta")
        metadata = {}

        if meta_path.exists():
            with open(meta_path, "r") as f:
                try:
                    metadata = json.load(f)
                except json.JSONDecodeError:
                    self.logger.error(f"Failed to parse metadata for {relative_path}")

        return content, metadata

    def save_state(self, state_id: str, state_data: bytes) -> str:
        """Save state data to filesystem"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"{state_id}_{timestamp}.state"
        filepath = self.state_dir / filename

        with open(filepath, "wb") as f:
            f.write(state_data)

        return str(filepath.relative_to(self.base_dir))

    def load_state(self, state_path: str) -> Optional[bytes]:
        """Load state data from filesystem"""
        full_path = self.base_dir / state_path

        if not full_path.exists():
            return None

        with open(full_path, "rb") as f:
            return f.read()

    def backup_database(self, db_path: str) -> str:
        """Create a backup of the database"""
        db_file = Path(db_path)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        backup_filename = f"{db_file.stem}_{timestamp}{db_file.suffix}.bak"
        backup_path = self.backup_dir / backup_filename

        # Copy database file
        with open(db_file, "rb") as src, open(backup_path, "wb") as dst:
            dst.write(src.read())

        return str(backup_path)


class StateManager:
    """Manages persistent storage and retrieval of crawler state"""

    def __init__(self, db_manager: DatabaseManager, fs_storage: FileSystemStorage):
        self.db_manager = db_manager
        self.fs_storage = fs_storage
        self.logger = logging.getLogger("state_manager")

    def save_state(self, state_id: str, state: Dict[str, Any]) -> bool:
        """Serialize and store state data"""
        try:
            # Serialize state
            state_data = pickle.dumps(state)

            # Store in filesystem
            fs_path = self.fs_storage.save_state(state_id, state_data)

            # Update database record
            with self.db_manager.get_connection() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO crawler_state
                    (id, state, timestamp)
                    VALUES (?, ?, ?)
                    """,
                    (state_id, state_data, datetime.now().isoformat())
                )
                conn.commit()

            return True
        except Exception as e:
            self.logger.error(f"Failed to save state {state_id}: {e}")
            return False

    def load_state(self, state_id: str) -> Optional[Dict[str, Any]]:
        """Load and deserialize state data"""
        try:
            # Query database for state record
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT state FROM crawler_state WHERE id = ?",
                    (state_id,)
                )
                row = cursor.fetchone()

                if row:
                    state_data = row[0]
                    # Deserialize
                    return pickle.loads(state_data)

            return None
        except Exception as e:
            self.logger.error(f"Failed to load state {state_id}: {e}")
            return None

    def list_states(self) -> List[Dict[str, Any]]:
        """List available states with metadata"""
        states = []

        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.execute(
                    "SELECT id, timestamp FROM crawler_state ORDER BY timestamp DESC"
                )

                for row in cursor:
                    state_id, timestamp_str = row
                    states.append({
                        "id": state_id,
                        "timestamp": datetime.fromisoformat(timestamp_str)
                    })

        except Exception as e:
            self.logger.error(f"Failed to list states: {e}")

        return states


class StandardStorage(StorageInterface):
    """Standard implementation of the storage interface"""

    def __init__(self, config: ConfigProvider):
        self.config = config

        # Initialize database
        db_path = config.get_config("database_path", "data/scraped_data.db")
        self.db_manager = DatabaseManager(db_path)

        # Initialize filesystem storage
        storage_path = config.get_config("storage_path", "data/storage")
        self.fs_storage = FileSystemStorage(storage_path)

        # Initialize cache manager
        self.cache_manager = CacheManager(self.db_manager)

        # Initialize state manager
        self.state_manager = StateManager(self.db_manager, self.fs_storage)

        # Thread pool for async operations
        max_workers = config.get_config("storage_thread_pool_size", 4)
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)

        self.logger = logging.getLogger("storage")

    def store_data(self, parsed_data: ParsedData) -> str:
        """Store extracted data and return an identifier"""
        try:
            # Convert data to JSON
            data_json = json.dumps(parsed_data.data)

            with self.db_manager.get_connection() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO extracted_data
                    (url, task_id, schema_name, data, timestamp, validated)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        parsed_data.response.task.url,
                        parsed_data.response.task.task_id,
                        parsed_data.schema_name,
                        data_json,
                        parsed_data.response.timestamp.isoformat(),
                        parsed_data.validated
                    )
                )
                conn.commit()

                # Return the ID of the inserted record
                return str(cursor.lastrowid)

        except Exception as e:
            self.logger.error(f"Error storing parsed data: {e}")
            raise

    def store_response(self, response: ScrapedResponse) -> str:
        """Store raw response for caching or auditing"""
        try:
            # Store in cache
            self.cache_manager.store(response)

            # If content is large, also store separately in filesystem
            content_size = len(response.content)
            large_content_threshold = self.config.get_config("large_content_threshold", 1024 * 1024)  # 1MB

            if content_size > large_content_threshold:
                # Determine file type extension
                extension = "html"
                if response.content_type:
                    if "json" in response.content_type:
                        extension = "json"
                    elif "xml" in response.content_type:
                        extension = "xml"
                    elif "pdf" in response.content_type:
                        extension = "pdf"
                    elif "image" in response.content_type:
                        extension = response.content_type.split("/")[-1] or "bin"

                # Store large content in filesystem
                metadata = {
                    "url": response.task.url,
                    "task_id": response.task.task_id,
                    "content_type": response.content_type,
                    "timestamp": response.timestamp.isoformat(),
                    "status_code": response.status_code
                }

                # Store asynchronously to not block the main thread
                self.thread_pool.submit(
                    self.fs_storage.store_file,
                    response.content,
                    extension,
                    metadata
                )

            return response.task.task_id

        except Exception as e:
            self.logger.error(f"Error storing response: {e}")
            raise

    def get_cached_response(self, url: str, max_age_seconds: int) -> Optional[ScrapedResponse]:
        """Retrieve a cached response if available and not expired"""
        return self.cache_manager.get(url, max_age_seconds)

    def save_state(self) -> Dict[str, Any]:
        """Save crawler state for resumable operations"""
        # Generate a state ID
        state_id = f"state_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Collect state data (this would include URL frontier, seen URLs, etc.)
        # In a real implementation, you would collect this from other components
        state = {
            "timestamp": datetime.now().isoformat(),
            "id": state_id,
            # This would be populated with actual state data from components
            "data": {}
        }

        # Save state
        success = self.state_manager.save_state(state_id, state)

        if success:
            return {"state_id": state_id}
        else:
            raise RuntimeError("Failed to save state")

    def load_state(self, state: Dict[str, Any]) -> None:
        """Restore crawler state from saved state"""
        state_id = state.get("state_id")

        if not state_id:
            raise ValueError("Invalid state data: missing state_id")

        loaded_state = self.state_manager.load_state(state_id)

        if not loaded_state:
            raise RuntimeError(f"Failed to load state: {state_id}")

        # In a real implementation, you would distribute this state to components
        # to restore their internal state
        return loaded_state

    def clean_up(self) -> None:
        """Clean up resources"""
        self.thread_pool.shutdown()
        self.db_manager.close_all()

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get statistics about storage usage"""
        stats = {}

        try:
            with self.db_manager.get_connection() as conn:
                # Count responses
                cursor = conn.execute("SELECT COUNT(*) FROM responses")
                stats["response_count"] = cursor.fetchone()[0]

                # Count parsed data entries
                cursor = conn.execute("SELECT COUNT(*) FROM extracted_data")
                stats["data_count"] = cursor.fetchone()[0]

                # Count schemas
                cursor = conn.execute(
                    "SELECT schema_name, COUNT(*) FROM extracted_data GROUP BY schema_name"
                )
                stats["schema_counts"] = {row[0]: row[1] for row in cursor.fetchall()}

                # Database size (approximate)
                cursor = conn.execute("PRAGMA page_count")
                page_count = cursor.fetchone()[0]

                cursor = conn.execute("PRAGMA page_size")
                page_size = cursor.fetchone()[0]

                stats["database_size_bytes"] = page_count * page_size
        except Exception as e:
            self.logger.error(f"Error getting storage stats: {e}")

        return stats


# Factory function for creating storage instances
def create_storage(config: ConfigProvider) -> StorageInterface:
    """Create a storage instance based on configuration"""
    storage_type = config.get_config("storage_type", "standard")

    if storage_type == "standard":
        return StandardStorage(config)
    else:
        raise ValueError(f"Unknown storage type: {storage_type}")
