"""
Enterprise Web Scraping Framework - Core Components
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Type, Union
import logging
import time
import uuid
from datetime import datetime
from urllib.parse import urlparse


# Core Domain Models

class TaskPriority(Enum):
    """Priority levels for scraping tasks"""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass
class ScrapingTask:
    """Represents a single unit of scraping work"""
    url: str
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    priority: TaskPriority = TaskPriority.NORMAL
    depth: int = 0
    parent_task_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    retries: int = 0
    max_retries: int = 3

    def get_domain(self) -> str:
        """Extract domain from URL"""
        return urlparse(self.url).netloc


@dataclass
class ScrapedResponse:
    """Represents the raw response from a scraping request"""
    task: ScrapingTask
    status_code: int
    headers: Dict[str, str]
    content: bytes
    content_type: str
    fetch_time: float
    timestamp: datetime = field(default_factory=datetime.now)
    session_id: Optional[str] = None
    proxy_used: Optional[str] = None
    final_url: Optional[str] = None  # For redirects


@dataclass
class ParsedData:
    """Represents structured data extracted from a response"""
    response: ScrapedResponse
    data: Dict[str, Any]
    schema_name: str
    validated: bool = False
    extraction_time: float = 0.0


@dataclass
class Link:
    """Represents a discovered URL during crawling"""
    url: str
    source_url: str
    text: Optional[str] = None
    nofollow: bool = False
    depth: int = 0
    discovered_at: datetime = field(default_factory=datetime.now)


# Core Interfaces

class ConfigProvider(ABC):
    """Interface for configuration management"""

    @abstractmethod
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key"""
        pass

    @abstractmethod
    def set_config(self, key: str, value: Any) -> None:
        """Set configuration value"""
        pass


class CrawlerInterface(ABC):
    """Interface for URL discovery and frontier management"""

    @abstractmethod
    def add_seed_urls(self, urls: List[str], priority: TaskPriority = TaskPriority.NORMAL) -> None:
        """Add initial URLs to the crawler"""
        pass

    @abstractmethod
    def get_next_task(self) -> Optional[ScrapingTask]:
        """Get the next URL to crawl based on priority and policies"""
        pass

    @abstractmethod
    def add_discovered_links(self, links: List[Link], parent_task: ScrapingTask) -> None:
        """Add newly discovered links to the frontier"""
        pass

    @abstractmethod
    def mark_task_complete(self, task: ScrapingTask) -> None:
        """Mark a task as successfully completed"""
        pass

    @abstractmethod
    def mark_task_failed(self, task: ScrapingTask, error: Exception) -> None:
        """Mark a task as failed, potentially scheduling for retry"""
        pass


class FetcherInterface(ABC):
    """Interface for HTTP requests and related concerns"""

    @abstractmethod
    def fetch(self, task: ScrapingTask) -> ScrapedResponse:
        """Fetch content from a URL with appropriate policies"""
        pass

    @abstractmethod
    def setup_session(self, domain: str) -> str:
        """Set up a session for a specific domain"""
        pass

    @abstractmethod
    def close_session(self, session_id: str) -> None:
        """Close and clean up a session"""
        pass


class ParserInterface(ABC):
    """Interface for data extraction from responses"""

    @abstractmethod
    def parse(self, response: ScrapedResponse, schema_name: str) -> ParsedData:
        """Extract structured data from a response using a named schema"""
        pass

    @abstractmethod
    def register_schema(self, schema_name: str, schema_definition: Dict[str, Any]) -> None:
        """Register a new extraction schema"""
        pass

    @abstractmethod
    def extract_links(self, response: ScrapedResponse) -> List[Link]:
        """Extract links from a response for further crawling"""
        pass


class StorageInterface(ABC):
    """Interface for data persistence"""

    @abstractmethod
    def store_data(self, parsed_data: ParsedData) -> str:
        """Store extracted data and return an identifier"""
        pass

    @abstractmethod
    def store_response(self, response: ScrapedResponse) -> str:
        """Store raw response for caching or auditing"""
        pass

    @abstractmethod
    def get_cached_response(self, url: str, max_age_seconds: int) -> Optional[ScrapedResponse]:
        """Retrieve a cached response if available and not expired"""
        pass

    @abstractmethod
    def save_state(self) -> Dict[str, Any]:
        """Save crawler state for resumable operations"""
        pass

    @abstractmethod
    def load_state(self, state: Dict[str, Any]) -> None:
        """Restore crawler state from saved state"""
        pass


class ComplianceInterface(ABC):
    """Interface for ethical and legal compliance"""

    @abstractmethod
    def check_robots_txt(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt"""
        pass

    @abstractmethod
    def get_crawl_delay(self, domain: str) -> float:
        """Get required delay between requests for a domain"""
        pass

    @abstractmethod
    def get_user_agent(self, domain: str) -> str:
        """Get appropriate user agent for a domain"""
        pass

    @abstractmethod
    def check_compliance_policies(self, url: str, task: ScrapingTask) -> bool:
        """Check if URL complies with all legal and ethical policies"""
        pass


class CircuitBreaker:
    """Implementation of the circuit breaker pattern"""

    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN

    def record_success(self) -> None:
        """Record a successful operation"""
        if self.state == "HALF-OPEN":
            self.failure_count = 0
            self.state = "CLOSED"
            logging.info("Circuit breaker reset to CLOSED state")

    def record_failure(self) -> None:
        """Record a failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.state == "CLOSED" and self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logging.warning(f"Circuit breaker tripped to OPEN state after {self.failure_count} failures")

    def allow_request(self) -> bool:
        """Check if a request should be allowed based on circuit state"""
        if self.state == "CLOSED":
            return True

        if self.state == "OPEN":
            time_since_last_failure = time.time() - self.last_failure_time
            if time_since_last_failure > self.reset_timeout:
                self.state = "HALF-OPEN"
                logging.info("Circuit breaker moved to HALF-OPEN state")
                return True
            return False

        # HALF-OPEN state allows a single request
        return True


class Orchestrator:
    """Core component that orchestrates the scraping process"""

    def __init__(
            self,
            config_provider: ConfigProvider,
            crawler: CrawlerInterface,
            fetcher: FetcherInterface,
            parser: ParserInterface,
            storage: StorageInterface,
            compliance: ComplianceInterface
    ):
        self.config = config_provider
        self.crawler = crawler
        self.fetcher = fetcher
        self.parser = parser
        self.storage = storage
        self.compliance = compliance
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.logger = logging.getLogger("orchestrator")

    def run(self, max_tasks: Optional[int] = None) -> None:
        """Run the scraping process for a specified number of tasks or indefinitely"""
        task_count = 0

        while True:
            if max_tasks is not None and task_count >= max_tasks:
                self.logger.info(f"Reached maximum task count: {max_tasks}")
                break

            task = self.crawler.get_next_task()
            if task is None:
                self.logger.info("No more tasks in queue")
                break

            task_count += 1
            self.process_task(task)

    def process_task(self, task: ScrapingTask) -> None:
        """Process a single scraping task"""
        domain = task.get_domain()

        # Check compliance
        if not self.compliance.check_compliance_policies(task.url, task):
            self.logger.warning(f"Task {task.task_id} for URL {task.url} failed compliance check")
            self.crawler.mark_task_complete(task)  # Skip non-compliant URLs
            return

        # Check circuit breaker
        if domain not in self.circuit_breakers:
            self.circuit_breakers[domain] = CircuitBreaker()

        if not self.circuit_breakers[domain].allow_request():
            self.logger.warning(f"Circuit breaker open for domain {domain}, skipping task")
            self.crawler.mark_task_failed(task, Exception("Circuit breaker open"))
            return

        # Check cache
        cache_ttl = self.config.get_config("cache_ttl_seconds", 3600)
        cached_response = self.storage.get_cached_response(task.url, cache_ttl)

        if cached_response:
            self.logger.info(f"Using cached response for {task.url}")
            response = cached_response
        else:
            # Enforce politeness
            delay = self.compliance.get_crawl_delay(domain)
            if delay > 0:
                time.sleep(delay)

            # Fetch content
            try:
                response = self.fetcher.fetch(task)
                self.storage.store_response(response)
                self.circuit_breakers[domain].record_success()
            except Exception as e:
                self.logger.error(f"Error fetching {task.url}: {str(e)}")
                self.circuit_breakers[domain].record_failure()
                self.crawler.mark_task_failed(task, e)
                return

        # Extract links if this is a crawling task
        if self.config.get_config(f"extract_links_for_{task.metadata.get('task_type', 'default')}", True):
            try:
                links = self.parser.extract_links(response)
                self.crawler.add_discovered_links(links, task)
            except Exception as e:
                self.logger.error(f"Error extracting links from {task.url}: {str(e)}")

        # Parse data if this is a scraping task
        schema_name = task.metadata.get("schema_name")
        if schema_name:
            try:
                parsed_data = self.parser.parse(response, schema_name)
                self.storage.store_data(parsed_data)
            except Exception as e:
                self.logger.error(f"Error parsing {task.url} with schema {schema_name}: {str(e)}")
                self.crawler.mark_task_failed(task, e)
                return

        # Mark task as complete
        self.crawler.mark_task_complete(task)


# Plugin system for extensions

class PluginRegistry:
    """Registry for framework plugins"""
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PluginRegistry, cls).__new__(cls)
            cls._instance.plugins = {}
        return cls._instance

    def register_plugin(self, plugin_type: str, name: str, plugin_class: Type[Any]) -> None:
        """Register a plugin with the framework"""
        if plugin_type not in self.plugins:
            self.plugins[plugin_type] = {}

        self.plugins[plugin_type][name] = plugin_class
        logging.info(f"Registered {plugin_type} plugin: {name}")

    def get_plugin(self, plugin_type: str, name: str) -> Optional[Type[Any]]:
        """Get a plugin by type and name"""
        if plugin_type not in self.plugins or name not in self.plugins[plugin_type]:
            return None
        return self.plugins[plugin_type][name]

    def list_plugins(self, plugin_type: Optional[str] = None) -> Dict[str, List[str]]:
        """List available plugins, optionally filtered by type"""
        if plugin_type:
            if plugin_type not in self.plugins:
                return {plugin_type: []}
            return {plugin_type: list(self.plugins[plugin_type].keys())}

        return {pt: list(plugins.keys()) for pt, plugins in self.plugins.items()}
    