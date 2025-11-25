from typing import Any, Dict, Optional
import time
import requests
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse


class PoliteSession:
    """A requests session that respects robots.txt and implements polite delays."""

    def __init__(self, default_headers: Optional[Dict[str, str]] = None, respect_robots: bool = True, delay: float = 1.0):
        self.session = requests.Session()
        if default_headers:
            self.session.headers.update(default_headers)
        self.respect_robots = respect_robots
        self.delay = delay
        self.last_request_time = 0
        self.robots_cache: Dict[str, RobotFileParser] = {}

    def _can_fetch(self, url: str) -> bool:
        """Check if we can fetch this URL according to robots.txt."""
        if not self.respect_robots:
            return True

        try:
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"

            if robots_url not in self.robots_cache:
                rp = RobotFileParser()
                rp.set_url(robots_url)
                try:
                    rp.read()
                    self.robots_cache[robots_url] = rp
                except Exception:
                    # If we can't read robots.txt, assume we can fetch
                    self.robots_cache[robots_url] = None

            rp = self.robots_cache[robots_url]
            if rp:
                return rp.can_fetch("*", url)
            return True
        except Exception:
            return True

    def _wait_if_needed(self):
        """Implement polite delay between requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.delay:
            time.sleep(self.delay - time_since_last)
        self.last_request_time = time.time()

    def get(self, url: str, **kwargs) -> requests.Response:
        """Make a GET request with politeness."""
        if not self._can_fetch(url):
            raise Exception(f"Blocked by robots.txt: {url}")

        self._wait_if_needed()
        return self.session.get(url, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        """Make a POST request with politeness."""
        if not self._can_fetch(url):
            raise Exception(f"Blocked by robots.txt: {url}")

        self._wait_if_needed()
        return self.session.post(url, **kwargs)

    def fetch_json(self, url: str, **kwargs) -> Dict[str, Any]:
        """Fetch JSON from a URL."""
        response = self.get(url, **kwargs)
        response.raise_for_status()
        return response.json()
