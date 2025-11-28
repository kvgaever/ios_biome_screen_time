from pathlib import PurePosixPath
from zipfile import ZipFile

import pandas as pd

from parser import parse_segb_file


def parse_infocus_zip(zip_path: str):
    """Parse App.InFocus ZIP; skip tombstones, __MACOSX, .DS_Store, and lock files."""
    local_results: dict[str, pd.DataFrame] = {}
    remote_results: dict[str, dict[str, pd.DataFrame]] = {}

    with ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue

            path = PurePosixPath(info.filename)
            parts = path.parts
            name = path.name.lower()

            # Skip macOS metadata and lock files
            if "__macosx" in (p.lower() for p in parts):
                continue
            if name == ".ds_store" or name == "lock" or name.endswith(".lock"):
                continue

            # Require App.InFocus root
            if not parts or parts[0] != "App.InFocus":
                continue

            # Skip tombstone folders anywhere in the path
            if any(p.lower() == "tombstone" for p in parts):
                continue

            # Local: App.InFocus/local/<file>
            if len(parts) >= 3 and parts[1] == "local":
                fname = path.name
                with zf.open(info) as fh:
                    df = parse_segb_file(
                        fh,
                        original_name=fname,
                    )
                local_results[fname] = df.assign(
                    source="local", device=None, file=fname
                )
                continue

            # Remote: App.InFocus/remote/<device>/<file>
            if len(parts) >= 4 and parts[1] == "remote":
                device = parts[2]
                fname = path.name
                with zf.open(info) as fh:
                    df = parse_segb_file(
                        fh,
                        original_name=fname,
                    )
                remote_results.setdefault(device, {})[fname] = df.assign(
                    source="remote", device=device, file=fname
                )

    return {"local": local_results, "remote": remote_results}


def combine_infocus_results(parsed: dict[str, dict]) -> dict[str, dict[str, pd.DataFrame]]:
    """
    Combine parsed {"local": {file: df}, "remote": {device: {file: df}}}
    into {"local": DataFrame, "remote": {device: DataFrame}}.

    - Local files are concatenated into a single DataFrame.
    - Remote files are concatenated per device key.
    """
    local_frames = list(parsed.get("local", {}).values())
    local_df = (
        pd.concat(local_frames, ignore_index=True) if local_frames else pd.DataFrame()
    )

    remote_combined: dict[str, pd.DataFrame] = {}
    for device, files in parsed.get("remote", {}).items():
        frames = list(files.values())
        remote_combined[device] = (
            pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        )

    return {"local": local_df, "remote": remote_combined}


def enrich_infocus_with_devices(
    combined: dict[str, dict],
    df_devices: pd.DataFrame,
    device_col: str = "device",
    devices_identifier_col: str = "device_identifier",
    fallback_device: str = "unknown_device",
) -> dict[str, dict[str, pd.DataFrame]]:
    """
    Enrich combined dataframes with device metadata by merging:
      combined_df[device_col] -> df_devices[devices_identifier_col]
    """

    devices = df_devices.copy() if df_devices is not None else pd.DataFrame()

    def _merge_single(df: pd.DataFrame, device_key: str | None) -> pd.DataFrame:
        # Handle None / empty df
        if df is None:
            return pd.DataFrame()

        df = df.copy()

        # Ensure device column exists
        if device_col not in df.columns:
            df[device_col] = device_key or fallback_device
        else:
            df[device_col] = df[device_col].fillna(device_key or fallback_device)

        # Perform merge only if device table is available
        if not devices.empty and devices_identifier_col in devices.columns:
            df = df.merge(
                devices,
                left_on=device_col,
                right_on=devices_identifier_col,
                how="left",
                suffixes=("", "_device")
            )

        # Ensure identifier exists even if merge failed
        if devices_identifier_col not in df.columns:
            df[devices_identifier_col] = fallback_device
        else:
            df[devices_identifier_col] = df[devices_identifier_col].fillna(
                fallback_device
            )

        return df

    # Apply merge to local
    enriched_local = _merge_single(
        combined.get("local"),
        device_key="mac"
    )

    # Apply merge to remote devices
    enriched_remote = {
        device: _merge_single(df, device_key=device)
        for device, df in combined.get("remote", {}).items()
    }

    return {
        "local": enriched_local,
        "remote": enriched_remote
    }


def filter_complete_sessions(group):
    # If the first row is a Stop (0), remove it (can't calculate duration)
    if group.iloc[0]["f3"] == 0:
        group = group.iloc[1:]

    # If the group is now empty, return it
    if group.empty:
        return group

    # If the last row is a Start (1), remove it (session incomplete)
    if group.iloc[-1]["f3"] == 1:
        group = group.iloc[:-1]

    return group


def clean_start_stop(
    df: pd.DataFrame, clean_orphans: None | bool = True
) -> pd.DataFrame:
    """
    Remove consecutive duplicate start/stop events per app (f6).
    If clean_orphans is True, also remove orphaned start/stop events that
    do not form complete sessions.
    """
    # Sort by Timestamp (ascending), and then by State (ascending)
    # 'f3' ascending means 0 (Stop) comes before 1 (Start) at the same timestamp.
    df = df.sort_values(by=["ts", "f3"], ascending=[True, True])

    # We use shift(1) to look at the row immediately before the current one within the group
    df["prev_state"] = df.groupby("f6")["f3"].shift(1)

    # We define a 'clean' row as one where the current state is NOT the same as the previous state
    # We also keep the first row for every app (where prev_state is NaN)
    clean_df = df[(df["f3"] != df["prev_state"]) | (df["prev_state"].isna())].copy()

    print(f"Removed {len(df) - len(clean_df)} consecutive duplicate start/stop events.")

    # Drop the helper column
    clean_df = clean_df.drop(columns=["prev_state"])

    if clean_orphans:
        clean_df = clean_df.groupby("f6", group_keys=False).apply(
            filter_complete_sessions
        )

    return clean_df.sort_values(["ts", "f3"])
