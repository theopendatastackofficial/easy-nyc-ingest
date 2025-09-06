# ---------------------------------------------------------------------------
#  SocrataResource – v4
# ---------------------------------------------------------------------------
import time
from typing import Any, Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ReadTimeout, ConnectionError
from urllib3.util.retry import Retry
from dagster import ConfigurableResource, EnvVar

# ────────────────────────────────────────────────────────────────────────────
_TIMEOUT_S             = 90           # seconds to wait for one HTTP response
_MAX_READ_RETRIES      = 3            # retries when we *raise* ReadTimeout/ConnErr
_MAX_EMPTY_RETRIES     = 2            # extra retries when response body is empty
_BACKOFF_FACTOR_S      = 1            # 1 s → 2 s → 4 s …

class SocrataResource(ConfigurableResource):
    """Reusable resource for hitting Socrata endpoints with robust retries."""

    api_token: str = EnvVar("SOCRATA_API_TOKEN")

    # singletons shared across the Dagster process
    _session: requests.Session | None = None
    _cache:   Tuple[str, Dict[str, Any], List[dict]] | None = None
    _retry   = Retry(
        total=5,
        backoff_factor=_BACKOFF_FACTOR_S,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    # ------------------------------------------------------------------ #
    def _get_session(self) -> requests.Session:
        if self._session is None:
            s = requests.Session()
            s.headers.update({"X-App-Token": self.api_token})
            s.mount("http://",  HTTPAdapter(max_retries=self._retry))
            s.mount("https://", HTTPAdapter(max_retries=self._retry))
            self.__class__._session = s
        return self._session

    # ------------------------------------------------------------------ #
    def fetch_data(
        self,
        endpoint: str,
        query_params: Dict[str, Any],
    ) -> List[dict]:
        """
        GET <endpoint>?<query_params>  →  list-of-dict records
        (geojson endpoints return .features[].properties)
        """

        # ── tiny one-item cache ──
        if (
            self._cache
            and self._cache[0] == endpoint
            and self._cache[1] == query_params
        ):
            return self._cache[2]

        sess = self._get_session()

        read_attempt = 0
        while True:
            read_attempt += 1
            try:
                resp = sess.get(endpoint, params=query_params, timeout=_TIMEOUT_S)
                resp.raise_for_status()           # HTTP-error → urllib3 auto-retry
            except (ReadTimeout, ConnectionError):
                if read_attempt > _MAX_READ_RETRIES:
                    raise
                time.sleep(_BACKOFF_FACTOR_S * 2 ** (read_attempt - 1))
                continue                          # retry the GET

            # ── got an HTTP 200 OK … make sure we actually received rows ──
            if endpoint.endswith(".geojson"):
                payload = [
                    f.get("properties", {}) for f in resp.json().get("features", [])
                ]
            else:
                payload = resp.json()

            if payload or read_attempt > _MAX_EMPTY_RETRIES:
                break                             # success (or give up on empties)
            # Empty body when limit > 0 → very likely a silent timeout / truncation
            time.sleep(_BACKOFF_FACTOR_S * 2 ** (read_attempt - 1))
            continue                              # retry GET from same offset

        # update cache
        self.__class__._cache = (endpoint, query_params, payload)
        return payload

    # ------------------------------------------------------------------ #
    def __del__(self):
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
