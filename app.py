# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "bbpb==1.4.2",
#     "duckdb==1.4.2",
#     "marimo",
#     "openpyxl==3.1.5",
#     "pandas==2.3.3",
# ]
# ///

import marimo

__generated_with = "0.18.1"
app = marimo.App(
    width="medium",
    app_title="iOS Screen Time Parser",
    css_file="/usr/local/_marimo/custom.css",
    auto_download=["html"],
)


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import sqlite3
    import pandas as pd
    import io
    import base64
    import tempfile
    import os
    return io, mo, os, pd, sqlite3, tempfile


@app.cell(hide_code=True)
def _():
    from zip_helper import (
        clean_start_stop,
        parse_infocus_zip,
        combine_infocus_results,
        enrich_infocus_with_devices,
    )
    return (
        clean_start_stop,
        combine_infocus_results,
        enrich_infocus_with_devices,
        parse_infocus_zip,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # iOS Screentime Parser

    This version of our app:

    - parses a zipped `App.InFocus` biome folder
    - queries a `sync.db` file for device identifiers
    - creates combined dataframes of iOS screentime per device
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    # 1. Connect to sync.db database

    Upload the sync.db file below
    """)
    return


@app.cell
def _(mo):
    db_file_upload = mo.ui.file(
        filetypes=[".db"], kind="area", label="Upload .db file"
    )
    return (db_file_upload,)


@app.cell
def _(db_file_upload):
    db_file_upload
    return


@app.cell
def _(db_file_upload, mo):
    # Disable the button until a .db file is present; status explains why.
    disabled = len(db_file_upload.value) == 0
    db_button = mo.ui.run_button(
        kind="neutral",
        tooltip="Run the query after uploading your sync.db file",
        label="Run Query",
        disabled=disabled,
    )
    db_status_msg = (
        mo.md("Upload a `.db` file to enable the query.")
        if disabled
        else mo.md("")
    )
    return db_button, db_status_msg


@app.cell
def _(mo):
    query_box = mo.ui.text_area(
        value="""SELECT
                last_sync_date,
                device_identifier,
                name,
                CASE platform
                    WHEN 1 THEN 'iPad'
                    WHEN 2 THEN 'iPhone'
                    WHEN 3 THEN 'Mac'
                    WHEN 4 THEN 'Mac'
                    WHEN 5 THEN 'AppleTV'
                    WHEN 6 THEN 'Watch'
                    WHEN 7 THEN 'AppleTV'
                    ELSE 'Unknown'
                END AS 'device_name',
                model,
                CASE me
                    WHEN 0 THEN ''
                    WHEN 1 THEN 'Yes'
                END AS 'Local Device'
            FROM DevicePeer;
                """,
        label="SQL query",
        disabled=True,
        full_width=True,
    )
    return (query_box,)


@app.cell
def _(query_box):
    query_box
    return


@app.cell
def _(db_button, db_status_msg, mo):
    mo.vstack(
        [
            mo.callout(value=mo.md("During upload the button will be *greyed out*.<br/>Please click the `Run Query` button when the upload finishes."), kind="danger"),
            db_button,
            db_status_msg,
        ]
    )
    return


@app.cell
def _(db_button, db_file_upload, mo, sqlite3, tempfile):
    # 1. STRICT GUARD: Stop if button not clicked OR file list is empty
    # This prevents the "index out of range" crash
    mo.stop(
        not (db_button.value and len(db_file_upload.value) > 0),
        mo.md("Please upload a file and click 'Run Query'")
    )

    # 2. Spinner gives visual feedback immediately (solving the 'did it work?' feeling)
    with mo.status.spinner("Processing database..."):
        # 3. READ CONTENTS HERE: Reading contents inside the gate prevents race conditions
        db_file_bytes = db_file_upload.contents(0)

        # Temporarily write .db to create a connection
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        tmp.write(db_file_bytes)
        tmp.flush()
        tmp_path = tmp.name
        tmp.close()

        con = sqlite3.connect(tmp_path)
    return con, tmp_path


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Device data: the data preview will be shown below when query has succesfully been run.

    Re-click the Run Query button in case no data is shown.
    """)
    return


@app.cell
def _(con, pd, query_box):
    # This cell listens to 'con'. When 'con' is created by the button above,
    # this runs automatically.
    df_devices = pd.read_sql_query(query_box.value, con)

    # Output the table
    df_devices
    return (df_devices,)


@app.cell
def _(mo):
    mo.md(r"""
    # 2. Parse .zip file
    Browse to the folder "App.InFocus" and zip the contents of this folder.

    Upload this zip file below and click the Parse button.
    """)
    return


@app.cell
def _(mo):
    zip_file_upload = mo.ui.file(
        filetypes=[".zip"], kind="area", label="Upload your .zip file"
    )
    return (zip_file_upload,)


@app.cell
def _(zip_file_upload):
    zip_file_upload
    return


@app.cell
def _(mo):
    zip_button = mo.ui.run_button(
        label="Parse uploaded .zip", tooltip="Parses the uploaded .zip file"
    )
    return (zip_button,)


@app.cell
def _(zip_button):
    zip_button
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ⚠️ Beware, this might take a minute to fully load and parse the data.
    """)
    return


@app.cell
def _(io, mo, parse_infocus_zip, zip_button, zip_file_upload):
    # 1. GUARD: Check both button and file existence
    mo.stop(not (zip_button.value and len(zip_file_upload.value) > 0))

    with mo.status.spinner("Parsing Zip file..."):
        # 2. READ CONTENTS HERE: prevents memory lag/race conditions
        file_bytes = zip_file_upload.contents()
        result = parse_infocus_zip(io.BytesIO(file_bytes))
    return (result,)


@app.cell
def _(combine_infocus_results, result):
    combined_by_device = combine_infocus_results(parsed=result)
    return (combined_by_device,)


@app.cell
def _(clean_start_stop, combined_by_device):
    cleaned_local = clean_start_stop(combined_by_device.get("local"))
    cleaned_remote = {
        device: clean_start_stop(df)
        for device, df in combined_by_device.get("remote", {}).items()
    }
    cleaned_by_device = {"local": cleaned_local, "remote": cleaned_remote}
    return (cleaned_by_device,)


@app.cell
def _(mo):
    mo.md(r"""
    # 3. Combine results with device info

    Preview the dataframes per device below. Click on `Download` to export as CSV, JSON or Parquet.
    """)
    return


@app.cell
def _(cleaned_by_device, df_devices, enrich_infocus_with_devices):
    df_by_device = enrich_infocus_with_devices(cleaned_by_device, df_devices)
    return (df_by_device,)


@app.cell(hide_code=True)
def _(df_by_device, mo):
    sections = {}

    def _pretty_label(df, default_device: str) -> str:
        """
        Try to extract a readable device name from the dataframe.
        Always falls back to `default_device` if nothing is found.
        """
        if df is not None and "device_name" in df.columns:
            non_null = df["device_name"].dropna()
            if not non_null.empty:
                return str(non_null.iloc[0])

        return default_device

    # ----- Local section -----
    if df_by_device.get("local") is not None and not df_by_device["local"].empty:
        label = f"Local - {_pretty_label(df_by_device['local'], 'Mac')}"
        sections[label] = mo.ui.table(df_by_device["local"], show_column_summaries = False)


    # ----- Remote sections -----
    for device, df in df_by_device.get("remote", {}).items():
        if df is None or df.empty:
            continue

        label = _pretty_label(df, device)
        sections[f"Remote - {label} - {device}"] = mo.ui.table(df, show_column_summaries = False)

    tables = mo.accordion(sections) if sections else mo.md("No data available.")
    tables
    return


@app.cell
def _(mo):
    mo.md(r"""
    # 4. Clean-up

    Clean up temporary files below.
    """)
    return


@app.cell
def _(mo):
    cleanup_button = mo.ui.run_button(
        label="Delete Temporary File",
        kind="danger",
        tooltip="Click this after you are done to free up server space.",
    )
    return (cleanup_button,)


@app.cell
def _(cleanup_button):
    cleanup_button
    return


@app.cell
def _(cleanup_button, con, mo, os, tmp_path):
    # Logic: Only show the result if the button is pressed
    # mo.md(clean_up()) if cleanup_button.value else mo.md("")

    # Stop unless cleanup is clicked
    mo.stop(not cleanup_button.value)

    try:
        con.close()  # Vital: Close connection before deleting
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
            msg = f"✅ Success: Removed {tmp_path}"
        else:
            msg = "File already deleted."
    except Exception as e:
        msg = f"Error: {e}"

    mo.md(msg)
    return


if __name__ == "__main__":
    app.run()
