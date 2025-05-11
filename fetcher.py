"""
Enterprise Web Scraping Framework - Fetcher Module Implementation
"""
import asyncio
import json
import logging
import random
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import hashlib
import ssl

import aiohttp
import backoff
import requests
from aiohttp import ClientSession, TCPConnector
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from tenacity import retry, stop_after_attempt, wait_exponential

from core_framework import (
    FetcherInterface, ScrapingTask, ScrapedResponse, ConfigProvider
)


@dataclass
class ProxyConfig:
    """Configuration for a proxy"""
    url: str
    username: Optional[str] = None
    password: Optional[str] = None
    proxy_type: str = "http"  # http, https, socks5
    location: Optional[str] = None
    last_used: float = 0
    failure_count: int = 0
    success_count: int = 0


@dataclass
class RateLimitRule:
    """Rule for rate limiting requests"""
    requests_per_second: float
    burst: int = 1
    domain_pattern: str = ".*"
    path_pattern: Optional[str] = None
    last_request_time: float = 0
    token_bucket: float = 0


class SessionManager:
    """Manages persistent sessions for crawling"""

    def __init__(self, config: ConfigProvider):
        self.config = config
        self.sessions: Dict[str, Dict[str, Any]] = {}
        self.logger = logging.getLogger("session_manager")

    def create_session(self, domain: str) -> str:
        """Create a new session for a domain"""
        session_id = f"{domain}_{int(time.time())}_{random.randint(1000, 9999)}"

        # Create session state
        self.sessions[session_id] = {
            "domain": domain,
            "created_at": datetime.now(),
            "last_used": datetime.now(),
            "cookies": {},
            "headers": self._get_default_headers(domain),
            "requests_count": 0,
            "auth_status": "none"  # none, pending, authenticated
        }

        self.logger.debug(f"Created new session {session_id} for domain {domain}")
        return session_id

    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data by ID"""
        session = self.sessions.get(session_id)
        if session:
            session["last_used"] = datetime.now()
        return session

    def update_session(self, session_id: str, key: str, value: Any) -> None:
        """Update session data"""
        if session_id in self.sessions:
            self.sessions[session_id][key] = value
            self.sessions[session_id]["last_used"] = datetime.now()

    def update_cookies(self, session_id: str, cookies: Dict[str, str]) -> None:
        """Update cookies for a session"""
        if session_id in self.sessions:
            self.sessions[session_id]["cookies"].update(cookies)
            self.sessions[session_id]["last_used"] = datetime.now()

    def close_session(self, session_id: str) -> None:
        """Close and remove a session"""
        if session_id in self.sessions:
            del self.sessions[session_id]
            self.logger.debug(f"Closed session {session_id}")

    def cleanup_stale_sessions(self, max_age_seconds: int = 3600) -> int:
        """Clean up sessions that haven't been used recently"""
        now = datetime.now()
        stale_sessions = []

        for session_id, session in self.sessions.items():
            age_seconds = (now - session["last_used"]).total_seconds()
            if age_seconds > max_age_seconds:
                stale_sessions.append(session_id)

        for session_id in stale_sessions:
            self.close_session(session_id)

        self.logger.info(f"Cleaned up {len(stale_sessions)} stale sessions")
        return len(stale_sessions)

    def _get_default_headers(self, domain: str) -> Dict[str, str]:
        """Get default headers for a domain"""
        user_agent = self.config.get_config(
            f"user_agents.{domain}",
            self.config.get_config(
                "default_user_agent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
            )
        )

        return {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
        }


class ProxyManager:
    """Manages and rotates proxies"""

    def __init__(self, config: ConfigProvider):
        self.config = config
        self.proxies: List[ProxyConfig] = []
        self.domain_proxy_mapping: Dict[str, List[str]] = {}
        self.lock = asyncio.Lock()
        self.logger = logging.getLogger("proxy_manager")

        # Load proxies from config
        self._load_proxies()

    def _load_proxies(self) -> None:
        """Load proxies from configuration"""
        proxy_configs = self.config.get_config("proxies", [])

        for proxy_config in proxy_configs:
            proxy = ProxyConfig(**proxy_config)
            self.proxies.append(proxy)

        self.logger.info(f"Loaded {len(self.proxies)} proxies")

    async def get_proxy(self, domain: str = None) -> Optional[ProxyConfig]:
        """Get an appropriate proxy for a domain"""
        async with self.lock:
            if not self.proxies:
                return None

            # Check if we have domain-specific proxies
            if domain and domain in self.domain_proxy_mapping:
                proxy_ids = self.domain_proxy_mapping[domain]
                domain_proxies = [p for p in self.proxies if p.url in proxy_ids]
                if domain_proxies:
                    return self._select_best_proxy(domain_proxies)

            return self._select_best_proxy(self.proxies)

    def _select_best_proxy(self, proxies: List[ProxyConfig]) -> ProxyConfig:
        """Select the best proxy from a list based on performance"""
        # Simple selection: least recently used with low failure count
        current_time = time.time()

        # Sort by (failure ratio, time since last used)
        def proxy_score(p: ProxyConfig) -> Tuple[float, float]:
            total_requests = p.success_count + p.failure_count
            failure_ratio = p.failure_count / max(1, total_requests)
            time_score = current_time - p.last_used
            return (failure_ratio, -time_score)  # Negative time score to sort descending

        selected = min(proxies, key=proxy_score)
        selected.last_used = current_time
        return selected

    async def mark_proxy_result(self, proxy_url: str, success: bool) -> None:
        """Update proxy statistics based on request result"""
        async with self.lock:
            for proxy in self.proxies:
                if proxy.url == proxy_url:
                    if success:
                        proxy.success_count += 1
                    else:
                        proxy.failure_count += 1
                    break


class RateLimiter:
    """Controls request rate to respect server limits"""

    def __init__(self, config: ConfigProvider):
        self.config = config
        self.rules: List[RateLimitRule] = []
        self.domain_rules: Dict[str, List[RateLimitRule]] = {}
        self.lock = asyncio.Lock()
        self.logger = logging.getLogger("rate_limiter")

        # Load rules from config
        self._load_rules()

    def _load_rules(self) -> None:
        """Load rate limiting rules from configuration"""
        rule_configs = self.config.get_config("rate_limit_rules", [])

        for rule_config in rule_configs:
            rule = RateLimitRule(**rule_config)
            self.rules.append(rule)

            # Extract domain from pattern for quicker lookup
            if rule.domain_pattern != ".*":
                domain = rule.domain_pattern.lstrip("^").rstrip("$").split("/")[0]
                if domain not in self.domain_rules:
                    self.domain_rules[domain] = []
                self.domain_rules[domain].append(rule)

        # Add default rule if none specified
        if not self.rules:
            self.rules.append(RateLimitRule(
                requests_per_second=1.0,
                burst=2,
                domain_pattern=".*"
            ))

        self.logger.info(f"Loaded {len(self.rules)} rate limit rules")

    async def wait_if_needed(self, url: str) -> float:
        """Wait if necessary to respect rate limits, returns wait time"""
        domain = url.split("://", 1)[1].split("/", 1)[0] if "://" in url else url.split("/", 1)[0]
        path = "/" + url.split("/", 1)[1] if "/" in url.split("://", 1)[1] else "/"

        matching_rules = []

        # Check domain-specific rules first
        if domain in self.domain_rules:
            for rule in self.domain_rules[domain]:
                domain_pattern = re.compile(rule.domain_pattern)
                if domain_pattern.match(domain):
                    if rule.path_pattern:
                        path_pattern = re.compile(rule.path_pattern)
                        if path_pattern.match(path):
                            matching_rules.append(rule)
                    else:
                        matching_rules.append(rule)

        # Check global rules
        for rule in self.rules:
            if rule.domain_pattern == ".*":
                matching_rules.append(rule)

        if not matching_rules:
            return 0.0

        # Use the most restrictive rule
        rule = min(matching_rules, key=lambda r: r.requests_per_second)

        current_time = time.time()
        time_since_last = current_time - rule.last_request_time

        # Token bucket algorithm
        rule.token_bucket = min(
            rule.burst,
            rule.token_bucket + time_since_last * rule.requests_per_second
        )

        if rule.token_bucket < 1.0:
            # Need to wait
            wait_time = (1.0 - rule.token_bucket) / rule.requests_per_second
            await asyncio.sleep(wait_time)
            rule.token_bucket = 0.0
            rule.last_request_time = time.time()
            return wait_time
        else:
            # No need to wait
            rule.token_bucket -= 1.0
            rule.last_request_time = current_time
            return 0.0


class CaptchaSolver:
    """Handles CAPTCHA detection and solving"""

    def __init__(self, config: ConfigProvider):
        self.config = config
        self.captcha_service = self.config.get_config("captcha_service", None)
        self.captcha_api_key = self.config.get_config("captcha_api_key", "")
        self.captcha_patterns = [
            re.compile(pattern) for pattern in self.config.get_config(
                "captcha_detection_patterns",
                [
                    r'recaptcha',
                    r'captcha',
                    r'cf-challenge',
                    r'denied access',
                    r'are you a robot',
                    r'verify you are human'
                ]
            )
        ]
        self.logger = logging.getLogger("captcha_solver")

    async def detect_captcha(self, content: str, status_code: int) -> bool:
        """Detect if response contains a CAPTCHA"""
        # Check status code that often indicates CAPTCHA
        if status_code in (403, 429, 503):
            for pattern in self.captcha_patterns:
                if pattern.search(content.lower()):
                    return True

        # Check content for CAPTCHA indicators
        for pattern in self.captcha_patterns:
            if pattern.search(content.lower()):
                return True

        return False

    async def solve_captcha(self, page: Page) -> bool:
        """Attempt to solve a CAPTCHA using configured service"""
        if not self.captcha_service:
            self.logger.warning("CAPTCHA detected but no solving service configured")
            return False

        self.logger.info(f"Attempting to solve CAPTCHA using {self.captcha_service}")

        # Placeholder for actual CAPTCHA solving implementation
        if self.captcha_service == "2captcha":
            return await self._solve_with_2captcha(page)
        elif self.captcha_service == "anticaptcha":
            return await self._solve_with_anticaptcha(page)
        else:
            self.logger.error(f"Unsupported CAPTCHA service: {self.captcha_service}")
            return False

    async def _solve_with_2captcha(self, page: Page) -> bool:
        """Solve CAPTCHA using 2captcha service"""
        # This would include actual 2captcha API integration
        self.logger.info("Solving with 2captcha (placeholder implementation)")
        return False

    async def _solve_with_anticaptcha(self, page: Page) -> bool:
        """Solve CAPTCHA using Anti-Captcha service"""
        # This would include actual Anti-Captcha API integration
        self.logger.info("Solving with Anti-Captcha (placeholder implementation)")
        return False


class DynamicFetcher(FetcherInterface):
    """Implements the fetcher interface with dynamic page execution capabilities"""

    def __init__(self, config: ConfigProvider):
        self.config = config
        self.session_manager = SessionManager(config)
        self.proxy_manager = ProxyManager(config)
        self.rate_limiter = RateLimiter(config)
        self.captcha_solver = CaptchaSolver(config)
        self.logger = logging.getLogger("fetcher")

        # Initialize state
        self.browser: Optional[Browser] = None
        self.contexts: Dict[str, BrowserContext] = {}

        # Configure settings
        self.default_timeout = config.get_config("request_timeout_seconds", 30)
        self.max_retries = config.get_config("max_request_retries", 3)
        self.dynamic_mode_enabled = config.get_config("dynamic_mode_enabled", True)
        self.use_stealth_plugin = config.get_config("use_stealth_plugin", True)

    async def _ensure_browser(self) -> Browser:
        """Ensure browser is launched"""
        if not self.browser:
            playwright = await async_playwright().start()

            # Configure browser launch options
            browser_type = self.config.get_config("browser_type", "chromium")
            headless = self.config.get_config("browser_headless", True)

            launch_options = {
                "headless": headless,
            }

            if browser_type == "firefox":
                self.browser = await playwright.firefox.launch(**launch_options)
            elif browser_type == "webkit":
                self.browser = await playwright.webkit.launch(**launch_options)
            else:
                self.browser = await playwright.chromium.launch(**launch_options)

            self.logger.info(f"Launched {browser_type} browser (headless: {headless})")

        return self.browser

    async def setup_session(self, domain: str) -> str:
        """Set up a session for a specific domain"""
        session_id = self.session_manager.create_session(domain)

        # Create browser context if using dynamic mode
        if self.dynamic_mode_enabled:
            browser = await self._ensure_browser()

            # Configure context
            context_options = {
                "user_agent": self.session_manager.get_session(session_id)["headers"]["User-Agent"],
                "viewport": {"width": 1280, "height": 800},
                "locale": "en-US",
                "timezone_id": "America/New_York",
                "has_touch": False,
                "ignore_https_errors": True,
            }

            # Apply proxy if available
            proxy = await self.proxy_manager.get_proxy(domain)
            if proxy:
                proxy_config = {
                    "server": proxy.url,
                }

                if proxy.username and proxy.password:
                    proxy_config["username"] = proxy.username
                    proxy_config["password"] = proxy.password

                context_options["proxy"] = proxy_config

            # Create context
            context = await browser.new_context(**context_options)

            # Apply stealth plugin if enabled
            if self.use_stealth_plugin:
                await context.add_init_script(path="./plugins/stealth.min.js")

            self.contexts[session_id] = context

        return session_id

    async def close_session(self, session_id: str) -> None:
        """Close and clean up a session"""
        # Close browser context if exists
        if session_id in self.contexts:
            await self.contexts[session_id].close()
            del self.contexts[session_id]

        # Close session in manager
        self.session_manager.close_session(session_id)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def fetch(self, task: ScrapingTask) -> ScrapedResponse:
        """Fetch content from a URL with appropriate policies"""
        domain = task.get_domain()
        url = task.url

        # Apply rate limiting
        await self.rate_limiter.wait_if_needed(url)

        # Get or create session
        session_id = task.metadata.get("session_id")
        if not session_id:
            session_id = await self.setup_session(domain)
            task.metadata["session_id"] = session_id

        # Get session data
        session = self.session_manager.get_session(session_id)
        if not session:
            session_id = await self.setup_session(domain)
            session = self.session_manager.get_session(session_id)
            task.metadata["session_id"] = session_id

        # Determine fetch mode
        force_dynamic = task.metadata.get("force_dynamic", False)
        dynamic_mode = force_dynamic or self.dynamic_mode_enabled

        start_time = time.time()

        # Switch between dynamic (browser-based) and static fetching
        if dynamic_mode and domain not in self.config.get_config("static_only_domains", []):
            response = await self._fetch_dynamic(task, session_id)
        else:
            response = await self._fetch_static(task, session_id)

        fetch_time = time.time() - start_time

        # Create response object
        result = ScrapedResponse(
            task=task,
            status_code=response["status_code"],
            headers=response["headers"],
            content=response["content"],
            content_type=response["content_type"],
            fetch_time=fetch_time,
            session_id=session_id,
            proxy_used=response.get("proxy_used"),
            final_url=response.get("final_url", url)
        )

        self.logger.info(
            f"Fetched {url} ({result.status_code}, {len(result.content)} bytes) "
            f"in {fetch_time:.2f}s using {'dynamic' if dynamic_mode else 'static'} mode"
        )

        return result

    async def _fetch_static(self, task: ScrapingTask, session_id: str) -> Dict[str, Any]:
        """Fetch using standard HTTP requests"""
        url = task.url
        session_data = self.session_manager.get_session(session_id)

        # Prepare request
        headers = session_data["headers"].copy()
        cookies = session_data["cookies"].copy()

        # Set up request parameters
        request_params = {
            "headers": headers,
            "cookies": cookies,
            "timeout": self.default_timeout,
            "allow_redirects": True,
            "verify": not self.config.get_config("ignore_ssl_errors", False)
        }

        # Apply proxy if available
        proxy = await self.proxy_manager.get_proxy(task.get_domain())
        proxy_url = None

        if proxy:
            proxy_auth = f"{proxy.username}:{proxy.password}@" if proxy.username else ""
            proxy_url = f"{proxy.proxy_type}://{proxy_auth}{proxy.url}"
            request_params["proxies"] = {
                "http": proxy_url,
                "https": proxy_url
            }

        # Make request with tenacity retry
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, **request_params) as response:
                    content = await response.read()
                    headers_dict = dict(response.headers)

                    # Extract cookies from response
                    cookies_dict = {}
                    for cookie in response.cookies.values():
                        cookies_dict[cookie.key] = cookie.value

                    # Update session cookies
                    if cookies_dict:
                        self.session_manager.update_cookies(session_id, cookies_dict)

                    # Check for CAPTCHA
                    content_str = content.decode("utf-8", errors="ignore")
                    if await self.captcha_solver.detect_captcha(content_str, response.status):
                        self.logger.warning(f"CAPTCHA detected at {url}, falling back to dynamic mode")
                        return await self._fetch_dynamic(task, session_id)

                    # Create result
                    result = {
                        "status_code": response.status,
                        "headers": headers_dict,
                        "content": content,
                        "content_type": headers_dict.get("Content-Type", "text/html"),
                        "final_url": str(response.url)
                    }

                    if proxy:
                        result["proxy_used"] = proxy.url
                        await self.proxy_manager.mark_proxy_result(proxy.url, True)

                    return result

        except Exception as e:
            self.logger.error(f"Error during static fetch of {url}: {str(e)}")
            if proxy:
                await self.proxy_manager.mark_proxy_result(proxy.url, False)
            raise

    async def _fetch_dynamic(self, task: ScrapingTask, session_id: str) -> Dict[str, Any]:
        """Fetch using browser automation"""
        url = task.url

        # Get or create context
        if session_id not in self.contexts:
            browser = await self._ensure_browser()
            context = await browser.new_context()
            self.contexts[session_id] = context

        context = self.contexts[session_id]

        # Create page
        page = await context.new_page()

        try:
            # Set user agent if specified
            session_data = self.session_manager.get_session(session_id)
            if "User-Agent" in session_data["headers"]:
                await page.set_extra_http_headers({"User-Agent": session_data["headers"]["User-Agent"]})

            # Configure timeout
            page.set_default_timeout(self.default_timeout * 1000)  # Convert to ms

            # Navigate to URL
            response = await page.goto(
                url,
                wait_until=self.config.get_config("wait_until", "networkidle"),
                timeout=self.default_timeout * 1000
            )

            if not response:
                raise Exception(f"No response received from {url}")

            # Wait for required selectors if specified
            wait_for_selector = task.metadata.get("wait_for_selector")
            if wait_for_selector:
                await page.wait_for_selector(wait_for_selector, timeout=10000)

            # Auto-scroll if enabled
            if self.config.get_config("auto_scroll", False):
                await self._auto_scroll(page)

            # Check for CAPTCHA
            content = await page.content()
            if await self.captcha_solver.detect_captcha(content, response.status):
                self.logger.warning(f"CAPTCHA detected at {url}, attempting to solve")
                solved = await self.captcha_solver.solve_captcha(page)
                if solved:
                    # Re-fetch content after solving
                    content = await page.content()
                else:
                    self.logger.error(f"Failed to solve CAPTCHA at {url}")

            # Extract cookies
            cookies = await context.cookies()
            cookies_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
            self.session_manager.update_cookies(session_id, cookies_dict)

            # Extract headers
            headers_dict = {}
            for header in response.headers():
                headers_dict[header["name"]] = header["value"]

            # Get content
            content_bytes = await page.content().encode("utf-8")

            # Take screenshot if configured
            screenshot_path = task.metadata.get("screenshot_path")
            if screenshot_path:
                await page.screenshot(path=screenshot_path)

            # Create result
            result = {
                "status_code": response.status,
                "headers": headers_dict,
                "content": content_bytes,
                "content_type": headers_dict.get("content-type", "text/html"),
                "final_url": page.url
            }

            return result

        finally:
            await page.close()

    async def _auto_scroll(self, page: Page) -> None:
        """Auto-scroll the page to load lazy content"""
        try:
            await page.evaluate("""
                async () => {
                    await new Promise((resolve) => {
                        let totalHeight = 0;
                        const distance = 100;
                        const timer = setInterval(() => {
                            const scrollHeight = document.body.scrollHeight;
                            window.scrollBy(0, distance);
                            totalHeight += distance;

                            if (totalHeight >= scrollHeight) {
                                clearInterval(timer);
                                resolve();
                            }
                        }, 100);
                    });
                }
            """)
        except Exception as e:
            self.logger.warning(f"Error during auto-scroll: {str(e)}")


# Factory function for creating fetcher instances
def create_fetcher(config: ConfigProvider) -> FetcherInterface:
    """Create a fetcher instance based on configuration"""
    fetcher_type = config.get_config("fetcher_type", "dynamic")

    if fetcher_type == "dynamic":
        return DynamicFetcher(config)
    else:
        raise ValueError(f"Unknown fetcher type: {fetcher_type}")
