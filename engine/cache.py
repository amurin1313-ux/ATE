import os, shutil

def clear_cache(data_dir: str) -> None:
    targets = [
        os.path.join(data_dir, "cache"),
        os.path.join(data_dir, "logs"),
        os.path.join(data_dir, "orders_dryrun"),
        os.path.join(data_dir, "trade_history"),
    ]
    for t in targets:
        if os.path.exists(t):
            shutil.rmtree(t, ignore_errors=True)
        os.makedirs(t, exist_ok=True)
