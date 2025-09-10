# lambda_handlers.py
import os, sys, importlib
from datetime import datetime

def _log(msg): print(f"[{datetime.utcnow().isoformat()}Z] {msg}")

def _run(mod_name: str, func_name: str = "main"):
    _log(f"→ importing {mod_name}.{func_name}()")
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, func_name)
    _log(f"→ running {mod_name}.{func_name}()")
    return fn()

def realtime_handler(event, context):
    """Handler: runs AA then BB then CC (live → ranked → 10m)."""
    _log("realtime_handler start")
    # Ensure SCB path is set (bundled next to code)
    os.environ.setdefault("ASTRA_BUNDLE_PATH", "/var/task/secure-connect.zip")
    _run("AA_gck_load_prices_live")        
    _run("BB_gck_update_prices_live_ranked")    
    _run("CC_gck_append_10m_from_live.py")    
    _log("realtime_handler done")
    return {"ok": True}

def candles_handler(event, context):
    """Handler: runs DD then EE (hourly → daily)."""
    _log("candles_handler start")
    os.environ.setdefault("ASTRA_BUNDLE_PATH", "/var/task/secure-connect.zip")
    _run("DD_gck_create_hourly_from_10m")
    _run("EE_gck_create_daily_from_10m")
    _log("candles_handler done")
    return {"ok": True}
