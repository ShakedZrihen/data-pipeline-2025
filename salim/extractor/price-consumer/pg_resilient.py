
"""
pg_resilient.py — drop-in helper for AWS Lambda + pg8000
- Handles stale sockets across warm invocations
- Retries on transient network errors
- Uses Supabase/RDS CA if provided
- Compatible with pg8000 variants where DB-API exceptions live in pg8000.dbapi
"""

from __future__ import annotations
import os
import ssl
import time
import socket
from typing import Any, Iterable, Optional, Tuple, Union, Sequence, Dict

import pg8000


try:
    from pg8000.dbapi import InterfaceError, DatabaseError, ProgrammingError  # type: ignore
except Exception:
    from pg8000.exceptions import InterfaceError, DatabaseError  # type: ignore
    class ProgrammingError(DatabaseError):  # type: ignore
        pass


try:
    from pg8000.dbapi import OperationalError  # type: ignore
except Exception:  # pragma: no cover
    class OperationalError(DatabaseError):  # type: ignore
        pass


_TRANSIENT_ERRORS = (
    InterfaceError,             # e.g., "network error"
    ConnectionResetError,
    BrokenPipeError,
    TimeoutError,
)

def _make_ssl_context(ca_file: Optional[str]) -> Optional[ssl.SSLContext]:
    if not ca_file:
        ctx = ssl.create_default_context()
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_verify_locations(cafile=ca_file)
    return ctx

class Postgres:
    def __init__(
        self,
        host: str,
        port: int = 5432,
        user: str = "",
        password: str = "",
        database: str = "",
        ca_file: Optional[str] = None,
        timeout: int = 10,
        pooled: bool = False,
        max_retries: int = 3,
        retry_backoff_base: float = 0.35,
        application_name: str = "lambda-consumer",
    ) -> None:
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        self.ca_file = ca_file
        self.timeout = timeout
        self.pooled = pooled
        self.max_retries = max_retries
        self.retry_backoff_base = retry_backoff_base
        self.application_name = application_name

        self._conn = None  # type: ignore

    # ---------- connection management ----------
    def _connect(self) -> None:
        ssl_context = _make_ssl_context(self.ca_file)
        self._conn = pg8000.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            ssl_context=ssl_context,
            timeout=self.timeout,
            application_name=self.application_name,
        )
        if self.pooled:
            self._conn.autocommit = True  # type: ignore

    def _close(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        finally:
            self._conn = None

    def ping(self) -> bool:
        try:
            cur = self.cursor()
            cur.execute("SELECT 1")
            getattr(cur, "fetchall", lambda: None)()
            return True
        except _TRANSIENT_ERRORS:
            self._close()
            return False
        except Exception:
            return False

    def ensure(self) -> None:
        """Ensure the connection is alive; reconnect if needed."""
        if self._conn is None:
            self._connect()
            return
        if not self.ping():
            self._connect()

    def cursor(self):
        if self._conn is None:
            raise InterfaceError("No connection")
        return self._conn.cursor()

    # ---------- execution helpers ----------
    def _execute_once(
        self,
        query: str,
        params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
        fetch: Optional[str] = None,
    ):
        cur = self.cursor()
        cur.execute(query, params or ())
        if fetch == "one":
            row = getattr(cur, "fetchone", lambda: None)()
            return (row,) if row is not None else ()
        elif fetch == "all":
            return getattr(cur, "fetchall", lambda: [])()
        return None

    def execute(
        self,
        query: str,
        params: Optional[Union[Sequence[Any], Dict[str, Any]]] = None,
        fetch: Optional[str] = None,
        in_txn: bool = False,
    ):
        """
        Execute with retries on transient network errors.
        If not in_txn, we wrap in a transaction (autocommit off) on direct port.
        """
        self.ensure()
        attempts = 0
        while True:
            attempts += 1
            try:
                if not self.pooled and not in_txn:
                    self._conn.autocommit = False  # type: ignore
                result = self._execute_once(query, params, fetch=fetch)
                if not self.pooled and not in_txn:
                    self._conn.commit()  # type: ignore
                return result
            except _TRANSIENT_ERRORS as e:
                self._close()
                if attempts > self.max_retries:
                    raise
                time.sleep(self.retry_backoff_base * (2 ** (attempts - 1)))
                self.ensure()
            except Exception:
                if not self.pooled and not in_txn:
                    try:
                        self._conn.rollback()  # type: ignore
                    except Exception:
                        pass
                raise

    def transaction(self):
        """Context manager for multi-statement transactions (direct port only)."""
        class _Txn:
            def __init__(self, outer: 'Postgres'):
                self.outer = outer
                self.active = False
            def __enter__(self):
                self.outer.ensure()
                if self.outer.pooled:
                    self.outer._conn.autocommit = False  # type: ignore
                self.active = True
                return self.outer
            def __exit__(self, exc_type, exc, tb):
                if not self.active:
                    return False
                try:
                    if exc is None:
                        self.outer._conn.commit()  # type: ignore
                    else:
                        self.outer._conn.rollback()  # type: ignore
                finally:
                    if self.outer.pooled:
                        self.outer._conn.autocommit = True  # type: ignore
                return False

        return _Txn(self)

# ---------- convenience factory from env ----------

def _autodetect_ca_file() -> Optional[str]:
    # Prefer explicit env var
    cf = os.environ.get("PGSSLROOTCERT")
    if cf and os.path.exists(cf):
        return cf
    # Common Lambda locations
    candidates = [
        "/var/task/certs/supabase.crt",
        "/opt/certs/supabase.crt",
        "/var/task/supabase.crt",
        "/opt/supabase.crt",
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return None

def from_env() -> Postgres:

    """
    Expected env vars:
      PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
      PGSSLROOTCERT (optional path), PGPOOL (optional "true" to use pooled port 6543)
      PGCONNECT_TIMEOUT (seconds, default 10)
    """
    host = os.environ.get("PGHOST", "")
    port = int(os.environ.get("PGPORT", "5432"))
    user = os.environ.get("PGUSER", "")
    password = os.environ.get("PGPASSWORD", "")
    database = os.environ.get("PGDATABASE", "")
    ca_file = _autodetect_ca_file()
    pooled = os.environ.get("PGPOOL", "false").lower() in ("1", "true", "yes")
    timeout = int(os.environ.get("PGCONNECT_TIMEOUT", "10"))
    return Postgres(
        host=host, port=port, user=user, password=password, database=database,
        ca_file=ca_file, pooled=pooled, timeout=timeout
    )
