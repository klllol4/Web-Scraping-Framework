"""
Enterprise Web Scraping Framework - Crawler Module Implementation
"""
import heapq
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
import logging
import re
from urllib.parse import urlparse, urljoin

from core_framework import (
    CrawlerInterface, ScrapingTask, Link, TaskPriority, ConfigProvider
)


class PriorityQueue:
    """Thread-safe priority queue implementation"""

    def __init__(self):
        self._queue = []
        self._entry_count = 0
        self._lock = threading.RLock()

    def push(self, item: ScrapingTask) -> None:
        """Add item to queue with priority"""
        with self._lock:
            # Use a tuple of (priority, count, item) to ensure stable sorting
            count = self._entry_count
            self._entry_count += 1
            heapq.heappush(self._queue, (item.priority.value, count, item))

    def pop(self) -> Optional[ScrapingTask]:
        """Remove and return lowest priority item"""
        with self._lock:
            if not self._queue:
                return None
            _, _, item = heapq.heappop(self._queue)
            return item

    def peek(self) -> Optional[ScrapingTask]:
        """View lowest priority item without removing"""
        with self._lock:
            if not self._queue:
                return None
            _, _, item = self._queue[0]
            return item

    def __len__(self) -> int:
        """Return queue size"""
        with self._lock:
            return len(self._queue)


class UrlFrontier:
    """Manages the crawl queue with prioritization and politeness"""

    def __init__(self, config: ConfigProvider):
        self.config = config
        self.domain_queues: Dict[str, PriorityQueue] = {}
        self.domain_last_access: Dict[str, float] = {}
        self.seen_urls: Set[str] = set()
        self.active_tasks: Dict[str, ScrapingTask] = {}
        self.lock = threading.RLock()
        self.logger = logging.getLogger("url_frontier")

    def add_task(self, task: ScrapingTask) -> bool:
        """Add a task to the frontier"""
        with self.lock:
            # Normalize URL for deduplication
            normalized_url = self._normalize_url(task.url)

            # Skip if already seen and not forced
            if normalized_url in self.seen_urls and not task.metadata.get("force_recrawl", False):
                self.logger.debug(f"Skipping already seen URL: {task.url}")
                return False

            # Add to seen URLs
            self.seen_urls.add(normalized_url)

            # Add to domain queue
            domain = task.get_domain()
            if domain not in self.domain_queues:
                self.domain_queues[domain] = PriorityQueue()

            self.domain_queues[domain].push(task)
            self.logger.debug(f"Added task for URL {task.url} to domain queue {domain}")
            return True

    def get_next_task(self) -> Optional[ScrapingTask]:
        """Get next task respecting politeness and priorities"""
        with self.lock:
            if not self.domain_queues:
                return None

            # Find the domain with highest priority task that respects politeness
            current_time = time.time()
            best_domain = None
            best_priority = float('inf')

            for domain, queue in self.domain_queues.items():
                if len(queue) == 0:
                    continue

                # Check domain politeness
                last_access = self.domain_last_access.get(domain, 0)
                delay = self.config.get_config(f"domain_delay.{domain}",
                                               self.config.get_config("default_domain_delay", 1.0))

                if current_time - last_access < delay:
                    continue

                # Check task priority
                task = queue.peek()
                if task and task.priority.value < best_priority:
                    best_domain = domain
                    best_priority = task.priority.value

            if best_domain is None:
                return None

            # Get the task and update domain access time
            task = self.domain_queues[best_domain].pop()
            self.domain_last_access[best_domain] = current_time
            self.active_tasks[task.task_id] = task

            return task

    def complete_task(self, task_id: str) -> None:
        """Mark a task as completed"""
        with self.lock:
            if task_id in self.active_tasks:
                del self.active_tasks[task_id]

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for deduplication"""
        # Parse URL
        parsed = urlparse(url)

        # Lowercase scheme and netloc
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        # Sort query parameters
        path = parsed.path
        if path == "":
            path = "/"

        # Handle query parameters if present
        query = parsed.query
        if query:
            params = sorted(query.split("&"))
            query = "&".join(params)

        # Reconstruct URL without fragments
        normalized = f"{scheme}://{netloc}{path}"
        if query:
            normalized += f"?{query}"

        return normalized

    def get_stats(self) -> Dict[str, int]:
        """Get statistics about the frontier"""
        with self.lock:
            stats = {
                "total_seen_urls": len(self.seen_urls),
                "active_tasks": len(self.active_tasks),
                "domain_count": len(self.domain_queues),
            }

            total_queued = 0
            for queue in self.domain_queues.values():
                total_queued += len(queue)

            stats["total_queued"] = total_queued

            return stats


class LinkExtractor:
    """Extracts links from HTML content"""

    def __init__(self, config: ConfigProvider):
        self.config = config
        self.link_regex = re.compile(r'<a\s+(?:[^>]*?\s+)?href="([^"]*)"', re.IGNORECASE)
        self.text_regex = re.compile(r'<a\s+(?:[^>]*?\s+)?href="[^"]*"[^>]*>(.*?)</a>', re.IGNORECASE)
        self.nofollow_regex = re.compile(r'rel=["\'](?:[^"\']*\s)?nofollow(?:\s[^"\']*)?["\']', re.IGNORECASE)

    def extract_links(self, html_content: str, source_url: str) -> List[Link]:
        """Extract all links from HTML content"""
        links = []

        # Find all link tags
        for match in self.link_regex.finditer(html_content):
            href = match.group(1)

            # Skip empty, javascript, mailto links
            if not href or href.startswith(('javascript:', 'mailto:', 'tel:')):
                continue

            # Convert relative URLs to absolute
            if not href.startswith(('http://', 'https://')):
                href = urljoin(source_url, href)

            # Get link text (if available)
            start_pos = match.end()
            text_match = self.text_regex.search(html_content[match.start():])
            text = text_match.group(1).strip() if text_match else None

            # Check for nofollow attribute
            tag_text = html_content[match.start():start_pos]
            nofollow = bool(self.nofollow_regex.search(tag_text))

            # Create link object
            link = Link(
                url=href,
                source_url=source_url,
                text=text,
                nofollow=nofollow
            )

            links.append(link)

        return links


class StandardCrawler(CrawlerInterface):
    """Standard implementation of the crawler interface"""

    def __init__(self, config: ConfigProvider):
        self.config = config
        self.frontier = UrlFrontier(config)
        self.link_extractor = LinkExtractor(config)
        self.logger = logging.getLogger("crawler")

        # Load crawl patterns from config
        self.allowed_patterns = [
            re.compile(pattern)
            for pattern in config.get_config("allowed_url_patterns", [r".*"])
        ]

        self.denied_patterns = [
            re.compile(pattern)
            for pattern in config.get_config("denied_url_patterns", [])
        ]

        self.max_depth = config.get_config("max_crawl_depth", 10)

    def add_seed_urls(self, urls: List[str], priority: TaskPriority = TaskPriority.NORMAL) -> None:
        """Add initial URLs to the crawler"""
        for url in urls:
            task = ScrapingTask(
                url=url,
                priority=priority,
                depth=0,
                metadata={"is_seed": True}
            )
            self.frontier.add_task(task)
            self.logger.info(f"Added seed URL: {url}")

    def get_next_task(self) -> Optional[ScrapingTask]:
        """Get the next URL to crawl based on priority and policies"""
        return self.frontier.get_next_task()

    def add_discovered_links(self, links: List[Link], parent_task: ScrapingTask) -> None:
        """Add newly discovered links to the frontier"""
        # Skip if we've reached max depth
        if parent_task.depth >= self.max_depth:
            self.logger.debug(f"Skipping links from {parent_task.url} due to max depth")
            return

        for link in links:
            # Skip links that don't match allowed patterns
            if not any(pattern.match(link.url) for pattern in self.allowed_patterns):
                continue

            # Skip links that match denied patterns
            if any(pattern.match(link.url) for pattern in self.denied_patterns):
                continue

            # Create new task with appropriate depth and priority
            new_priority = parent_task.priority

            # Adjust priority based on nofollow
            if link.nofollow:
                # Lower priority for nofollow links
                new_priority = TaskPriority(min(TaskPriority.BACKGROUND.value,
                                                parent_task.priority.value + 1))

            task = ScrapingTask(
                url=link.url,
                priority=new_priority,
                depth=parent_task.depth + 1,
                parent_task_id=parent_task.task_id,
                metadata={
                    "source_url": parent_task.url,
                    "link_text": link.text,
                    "nofollow": link.nofollow
                }
            )

            self.frontier.add_task(task)

    def mark_task_complete(self, task: ScrapingTask) -> None:
        """Mark a task as successfully completed"""
        self.frontier.complete_task(task.task_id)
        self.logger.debug(f"Marked task {task.task_id} as complete")

    def mark_task_failed(self, task: ScrapingTask, error: Exception) -> None:
        """Mark a task as failed, potentially scheduling for retry"""
        # Check if we should retry
        if task.retries < task.max_retries:
            # Create retry task with backoff
            retry_task = ScrapingTask(
                url=task.url,
                task_id=task.task_id,
                priority=TaskPriority(min(TaskPriority.BACKGROUND.value,
                                          task.priority.value + 1)),  # Lower priority for retries
                depth=task.depth,
                parent_task_id=task.parent_task_id,
                metadata=task.metadata,
                retries=task.retries + 1,
                max_retries=task.max_retries
            )

            self.frontier.add_task(retry_task)
            self.logger.warning(f"Scheduled retry {retry_task.retries}/{retry_task.max_retries} "
                                f"for task {task.task_id}")
        else:
            self.frontier.complete_task(task.task_id)
            self.logger.error(f"Task {task.task_id} failed after {task.retries} retries: {error}")


# Factory function for creating crawler instances
def create_crawler(config: ConfigProvider) -> CrawlerInterface:
    """Create a crawler instance based on configuration"""
    crawler_type = config.get_config("crawler_type", "standard")

    if crawler_type == "standard":
        return StandardCrawler(config)
    else:
        raise ValueError(f"Unknown crawler type: {crawler_type}")
