# astra_connect.py
import os
import sys
import pathlib
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Tuple

from dotenv import load_dotenv, find_dotenv

# Cassandra / DataStax driver
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy


# ----------------------------- Repo / .env -----------------------------
def _find_repo_root(start: Optional[pathlib.Path] = None) -> pathlib.Path:
    """Walk upward from `start` (or CWD) until a project marker is found."""
    here = (start or pathlib.Path.cwd()).resolve()
    markers = (".env", "pyproject.toml", ".git")
    for p in [here, *here.parents]:
        if any((p / m).exists() for m in markers):
            return p
    # Fallback: topmost ancestor
    return here.anchor and pathlib.Path(here.anchor) or here


def _load_env(override: bool = False) -> pathlib.Path:
    """
    Load .env from the nearest project root. Returns the root path used.
    If the shell already has env vars you want the .env to win over, pass override=True.
    """
    # Prefer python-dotenv's search from CWD; if it misses, use our root finder.
    path = find_dotenv(usecwd=True)
    if not path:
        root = _find_repo_root()
        path = str(root / ".env")
    else:
        root = pathlib.Path(path).resolve().parent

    load_dotenv(dotenv_path=path, override=override)
    return root


def _norm_path(p: Optional[str]) -> Optional[str]:
    if not p:
        return p
    # Expand ~ and env vars; ensure native separators; strip quotes if any
    p = os.path.expandvars(os.path.expanduser(p.strip().strip('"').strip("'")))
    return str(pathlib.Path(p))


# ----------------------------- Config model -----------------------------
@dataclass(frozen=True)
class AstraConfig:
    bundle_path: str
    token: str
    keyspace: str
    request_timeout_sec: int = 60
    connect_timeout_sec: int = 15
    fetch_size: int = 1000

    @staticmethod
    def from_env() -> "AstraConfig":
        root = _load_env(override=False)

        bundle = (
            os.getenv("ASTRA_BUNDLE_PATH")
            or os.getenv("ASTRA_BUNDLE")
            or "secure-connect.zip"
        )
        bundle = _norm_path(bundle)

        token = os.getenv("ASTRA_TOKEN") or ""
        keyspace = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

        req = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))
        conn = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
        fetch_size = int(os.getenv("FETCH_SIZE", "1000"))

        # Validate early with actionable messages
        errors = []
        if not token:
            errors.append(
                "ASTRA_TOKEN is missing. Create a Database Admin token in Astra DB and set ASTRA_TOKEN."
            )
        if not bundle or not pathlib.Path(bundle).exists():
            # Try resolving relative to repo root if a bare filename was provided
            candidate = pathlib.Path(bundle)
            if not candidate.is_absolute():
                candidate = root / candidate
            if not candidate.exists():
                errors.append(
                    f"Secure bundle not found at: {bundle!r}. "
                    f"Set ASTRA_BUNDLE_PATH to the full path "
                    f"(e.g. C:\\Users\\you\\Downloads\\secure-connect-*.zip)."
                )
            else:
                bundle = str(candidate)

        if errors:
            msg = "Astra configuration error:\n  - " + "\n  - ".join(errors)
            raise RuntimeError(msg)

        # Friendly hint if token doesn’t look like an AstraCS token (non-fatal)
        if not token.startswith("AstraCS:"):
            print("[astra_connect] Warning: ASTRA_TOKEN doesn’t start with 'AstraCS:' — is this the correct token?")

        return AstraConfig(
            bundle_path=str(bundle),
            token=token,
            keyspace=keyspace,
            request_timeout_sec=req,
            connect_timeout_sec=conn,
            fetch_size=fetch_size,
        )


# ----------------------------- Public API -----------------------------
@lru_cache(maxsize=1)
def _get_cluster_and_profile(cfg: AstraConfig) -> Tuple[Cluster, ExecutionProfile]:
    auth = PlainTextAuthProvider("token", cfg.token)
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        request_timeout=cfg.request_timeout_sec,
    )
    cluster = Cluster(
        cloud={"secure_connect_bundle": cfg.bundle_path},
        auth_provider=auth,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        connect_timeout=cfg.connect_timeout_sec,
    )
    return cluster, profile


def get_session(
    keyspace: Optional[str] = None,
    *,
    override_env_with_dotenv: bool = False,
    return_cluster: bool = False,
):
    """
    Get a cached Astra Session (and optional Cluster).
    - keyspace: override the default ASTRA_KEYSPACE from env.
    - override_env_with_dotenv: if True, values from .env overwrite existing shell env vars.
    - return_cluster: if True, returns (session, cluster); otherwise just session.
    """
    # Build config (loads .env exactly once per process run)
    cfg = AstraConfig.from_env()
    if keyspace:
        cfg = AstraConfig(
            bundle_path=cfg.bundle_path,
            token=cfg.token,
            keyspace=keyspace,
            request_timeout_sec=cfg.request_timeout_sec,
            connect_timeout_sec=cfg.connect_timeout_sec,
            fetch_size=cfg.fetch_size,
        )

    cluster, _ = _get_cluster_and_profile(cfg)
    session = cluster.connect(cfg.keyspace)
    # Set default fetch_size if caller uses SimpleStatement without one
    session.default_fetch_size = cfg.fetch_size

    if return_cluster:
        return session, cluster
    return session


def close_cached_cluster() -> None:
    """Close the cached Cluster (if created). Safe to call multiple times."""
    try:
        cluster, _ = _get_cluster_and_profile.cache_info()  # type: ignore[attr-defined]
    except Exception:
        cluster = None
    # The lru_cache doesn't expose values directly; re-create config to touch cache,
    # then clear it and close if possible.
    try:
        cfg = AstraConfig.from_env()
        cluster, _ = _get_cluster_and_profile(cfg)
    except Exception:
        cluster = None
    finally:
        _get_cluster_and_profile.cache_clear()

    try:
        if cluster:
            cluster.shutdown()
    except Exception:
        pass


# ----------------------------- Convenience helpers -----------------------------
def ensure_repo_root_on_sys_path() -> pathlib.Path:
    """
    Add repo root to sys.path (so `from paths import rel`-type imports work).
    Returns the path that was added (or the existing one).
    """
    root = _find_repo_root()
    if str(root) not in sys.path:
        sys.path.append(str(root))
    return root
