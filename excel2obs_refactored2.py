# -*- coding: utf-8 -*-
import pandas as pd
import openpyxl
import obsws_python as obs
from obsws_python.error import OBSSDKRequestError
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog
import logging
import os
import threading
import time
import queue
import json

# --- Configuration (Defaults) ---
DEFAULT_OBS_WS_HOST = "localhost"
DEFAULT_OBS_WS_PORT = 4444
DEFAULT_OBS_WS_PASSWORD = "MianSoft3216"
UPDATE_INTERVAL_SECONDS = 0.5 # Maybe increase this if still sluggish (e.g., 1.0 or 2.0)
DEFAULT_THEME = "litera"
CONNECTION_TIMEOUT_SECONDS = 5
DISCONNECT_TIMEOUT_SECONDS = 2
STATUS_QUEUE_CHECK_MS = 100
LOG_LEVEL = logging.INFO

# --- Logging Setup ---
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)

# --- Main Application Class ---
class ExcelToOBS:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel2OBS (Optimized) - 原版: B站: 直播说")

        # --- Style ---
        self.style = ttk.Style(theme=DEFAULT_THEME)

        # --- OBS Connection (Vars for UI) ---
        self.obs_host_var = ttk.StringVar(value=DEFAULT_OBS_WS_HOST)
        self.obs_port_var = ttk.StringVar(value=str(DEFAULT_OBS_WS_PORT))
        self.obs_password_var = ttk.StringVar(value=DEFAULT_OBS_WS_PASSWORD)
        self.obs_client = None
        self.obs_connected = False
        self.obs_connection_lock = threading.Lock()
        self._connecting = False

        # --- State Variables ---
        self.file_path = ttk.StringVar()
        self.sheet_name = ttk.StringVar()
        self.inputs_data = []
        self.previous_values = {}
        self.running = True
        self.update_thread = None
        self.status_queue = queue.Queue()

        # --- Excel Caching ---
        self.last_excel_mtime = None
        self.cached_df = None
        self.excel_read_lock = threading.Lock() # Lock specifically for reading/caching Excel

        # --- UI Setup ---
        self._setup_ui()

        # --- Start Background Tasks ---
        self.start_update_thread()
        self.root.after(STATUS_QUEUE_CHECK_MS, self.process_status_queue)
        self.root.after(500, self.connect_obs)
        self.root.protocol("WM_DELETE_WINDOW", self.stop)

    # --- UI Setup (_setup_ui - unchanged from previous version) ---
    def _setup_ui(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=BOTH, expand=YES)
        # File Selection Frame
        file_frame = ttk.LabelFrame(main_frame, text="Excel File Setup", padding="10")
        file_frame.pack(fill=X, pady=(0, 10))
        ttk.Label(file_frame, text="Excel File:").grid(row=0, column=0, padx=5, pady=5, sticky=W)
        self.file_entry = ttk.Entry(file_frame, textvariable=self.file_path, width=40)
        self.file_entry.grid(row=0, column=1, padx=5, pady=5, sticky=EW)
        ttk.Button(file_frame, text="Browse", command=self.choose_file, bootstyle=SECONDARY).grid(row=0, column=2, padx=5, pady=5)
        ttk.Label(file_frame, text="Sheet Name:").grid(row=1, column=0, padx=5, pady=5, sticky=W)
        self.sheet_entry = ttk.Entry(file_frame, textvariable=self.sheet_name, width=40)
        self.sheet_entry.grid(row=1, column=1, padx=5, pady=5, sticky=EW)
        self.sheet_entry.bind("<Return>", lambda event: self.update_obs_data(check_changes=False))
        file_frame.columnconfigure(1, weight=1)
        # OBS Connection Frame
        obs_frame = ttk.LabelFrame(main_frame, text="OBS Connection", padding="10")
        obs_frame.pack(fill=X, pady=(0, 10))
        obs_frame.columnconfigure(1, weight=1); obs_frame.columnconfigure(3, weight=1)
        ttk.Label(obs_frame, text="Host:").grid(row=0, column=0, padx=(0,5), pady=5, sticky=W)
        self.obs_host_entry = ttk.Entry(obs_frame, textvariable=self.obs_host_var, width=15)
        self.obs_host_entry.grid(row=0, column=1, padx=(0,10), pady=5, sticky=EW)
        ttk.Label(obs_frame, text="Port:").grid(row=0, column=2, padx=(0,5), pady=5, sticky=W)
        self.obs_port_entry = ttk.Entry(obs_frame, textvariable=self.obs_port_var, width=8)
        self.obs_port_entry.grid(row=0, column=3, padx=(0,10), pady=5, sticky=W)
        ttk.Label(obs_frame, text="Password:").grid(row=1, column=0, padx=(0,5), pady=5, sticky=W)
        self.obs_password_entry = ttk.Entry(obs_frame, textvariable=self.obs_password_var, width=15, show="*")
        self.obs_password_entry.grid(row=1, column=1, columnspan=3, padx=(0,10), pady=5, sticky=EW)
        self.connect_button = ttk.Button(obs_frame, text="Connect / Reconnect", command=self.connect_obs, bootstyle=INFO)
        self.connect_button.grid(row=0, column=4, rowspan=2, padx=5, pady=5, sticky=NS+E)
        self.obs_status_label = ttk.Label(obs_frame, text="OBS Status: Disconnected", anchor=W)
        self.obs_status_label.grid(row=2, column=0, columnspan=5, padx=0, pady=(5,0), sticky=EW)
        # Inputs Frame
        inputs_outer_frame = ttk.LabelFrame(main_frame, text="OBS Source Mapping", padding="10")
        inputs_outer_frame.pack(fill=BOTH, expand=YES, pady=(0, 10))
        header_frame = ttk.Frame(inputs_outer_frame); header_frame.pack(fill=X)
        ttk.Label(header_frame, text="Type", width=12).pack(side=LEFT, padx=5)
        ttk.Label(header_frame, text="OBS Source Name", width=20).pack(side=LEFT, padx=5)
        ttk.Label(header_frame, text="Row", width=6).pack(side=LEFT, padx=5); ttk.Label(header_frame, text="Col", width=8).pack(side=LEFT, padx=5)
        ttk.Label(header_frame, text="Current Value", width=15).pack(side=LEFT, padx=5, expand=YES, fill=X)
        ttk.Label(header_frame, text="Auto?").pack(side=LEFT, padx=5); ttk.Label(header_frame, text="Del", width=4).pack(side=RIGHT, padx=5)
        canvas = ttk.Canvas(inputs_outer_frame); scrollbar = ttk.Scrollbar(inputs_outer_frame, orient=VERTICAL, command=canvas.yview, bootstyle=ROUND)
        self.inputs_frame = ttk.Frame(canvas); self.inputs_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=self.inputs_frame, anchor=NW); canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side=LEFT, fill=BOTH, expand=YES); scrollbar.pack(side=RIGHT, fill=Y)
        # Button Frame
        button_frame = ttk.Frame(main_frame); button_frame.pack(fill=X, pady=(5, 0))
        ttk.Button(button_frame, text="Add Mapping", command=self.add_input_row, bootstyle=SUCCESS).pack(side=LEFT, padx=5)
        ttk.Button(button_frame, text="Update OBS Now", command=lambda: self.update_obs_data(check_changes=False), bootstyle=PRIMARY).pack(side=LEFT, padx=5)
        ttk.Button(button_frame, text="Import Settings", command=self.import_settings, bootstyle=SECONDARY).pack(side=RIGHT, padx=5)
        ttk.Button(button_frame, text="Export Settings", command=self.export_settings, bootstyle=SECONDARY).pack(side=RIGHT, padx=5)
        # Status Bar
        self.status_bar = ttk.Label(self.root, text="Ready.", anchor=W, relief=SUNKEN, padding=(5, 2))
        self.status_bar.pack(side=BOTTOM, fill=X)
        # Add Initial Row
        if not self.inputs_data: self.add_input_row()

    # --- Status Methods (unchanged) ---
    def update_status(self, message, level="info"): # ... (same)
        if self.running:
            try: self.status_queue.put((message, level))
            except Exception as e: logging.error(f"Failed to put message in status queue: {e}")
    def process_status_queue(self): # ... (same)
        if not self.running: return
        try:
            while True:
                message, level = self.status_queue.get_nowait()
                color_map = {"info": DEFAULT, "success": SUCCESS, "warning": WARNING, "error": DANGER}
                self.status_bar.config(text=str(message)[:200], bootstyle=color_map.get(level, DEFAULT))
                if level == "error": logging.error(f"Status Update: {message}")
                elif level == "warning": logging.warning(f"Status Update: {message}")
                self.status_queue.task_done()
        except queue.Empty: pass
        except Exception as e: logging.error(f"Error processing status queue: {e}")
        finally:
            if self.running: self.root.after(STATUS_QUEUE_CHECK_MS, self.process_status_queue)

    # --- File/UI Methods (Modified choose_file) ---
    def choose_file(self):
        try:
            path = filedialog.askopenfilename(title="Select Excel File", filetypes=[("Excel files", "*.xlsx;*.xlsm"), ("All files", "*.*")])
            if path:
                self.file_path.set(path)
                self.update_status(f"Selected file: {os.path.basename(path)}")
                logging.info(f'Selected file: {path}')
                # --- Reset cache on file change ---
                with self.excel_read_lock:
                    self.last_excel_mtime = None
                    self.cached_df = None
                self.previous_values.clear() # Clear previous cell values too
                self.update_all_value_labels() # Update labels
        except Exception as e:
            logging.exception("Error choosing file.")
            self.update_status(f"Error choosing file: {e}", "error")

    def add_input_row(self, mapping_data=None): # ... (same as previous version)
        row_frame = ttk.Frame(self.inputs_frame)
        row_frame.pack(fill=X, pady=2)
        data_type_var = ttk.StringVar(value="Text"); row_var = ttk.StringVar(); col_var = ttk.StringVar(); name_var = ttk.StringVar()
        value_label = ttk.Label(row_frame, text="N/A", width=15, anchor=W, relief=SUNKEN, padding=(3,0), bootstyle=SECONDARY)
        check_var = ttk.IntVar(value=0)
        row_data = {"frame": row_frame, "data_type": data_type_var, "row": row_var, "col": col_var, "name": name_var, "value_label": value_label, "auto_update": check_var}
        self.inputs_data.append(row_data); row_index = len(self.inputs_data) - 1
        # Widgets
        data_type_menu = ttk.OptionMenu(row_frame, data_type_var, "Text", "Text", "Image"); data_type_menu.config(width=6); data_type_menu.pack(side=LEFT, padx=5)
        name_entry = ttk.Entry(row_frame, textvariable=name_var, width=20); name_entry.pack(side=LEFT, padx=5); name_entry.bind("<KeyRelease>", lambda event, r=row_data: self._check_update_needed(r))
        row_entry = ttk.Entry(row_frame, textvariable=row_var, width=5); row_entry.pack(side=LEFT, padx=5); row_entry.bind("<KeyRelease>", lambda event, r=row_data: self.update_value_label(r))
        col_entry = ttk.Entry(row_frame, textvariable=col_var, width=5); col_entry.pack(side=LEFT, padx=5); col_entry.bind("<KeyRelease>", lambda event, r=row_data: self.update_value_label(r))
        value_label.pack(side=LEFT, padx=5, expand=YES, fill=X)
        check_button = ttk.Checkbutton(row_frame, variable=check_var, bootstyle=(PRIMARY, TOOLBUTTON)); check_button.pack(side=LEFT, padx=(5, 10))
        del_button = ttk.Button(row_frame, text="X", command=lambda idx=row_index: self.delete_input_row(idx), bootstyle=(DANGER, OUTLINE), width=3); del_button.pack(side=RIGHT, padx=5)
        # Populate
        if mapping_data:
            data_type_var.set(mapping_data.get("type", "Text")); name_var.set(mapping_data.get("name", "")); row_var.set(str(mapping_data.get("row", "")))
            col_var.set(str(mapping_data.get("col", ""))); check_var.set(int(mapping_data.get("auto_update", 0))); self.update_value_label(row_data)
        else: self.update_status(f"Added new mapping row.")

    def delete_input_row(self, index): # ... (same as previous version)
        if 0 <= index < len(self.inputs_data):
            row_data = self.inputs_data[index]
            try:
                 row_str, col_str = row_data["row"].get(), row_data["col"].get()
                 if row_str.isdigit() and col_str.isdigit(): self.previous_values.pop((int(row_str)-1, int(col_str)-1), None)
            except Exception as e: logging.warning(f"Could not clear previous_value for deleting row {index}: {e}")
            row_data["frame"].destroy(); self.inputs_data.pop(index)
            logging.info(f"Deleted input row at index {index}"); self.update_status(f"Deleted mapping row.")
            self._update_delete_button_commands()

    def _update_delete_button_commands(self): # ... (same)
         for i, row_data in enumerate(self.inputs_data):
             for widget in row_data["frame"].winfo_children():
                 if isinstance(widget, ttk.Button) and widget.cget("text") == "X": widget.configure(command=lambda idx=i: self.delete_input_row(idx)); break
    def _check_update_needed(self, row_data): pass
    def update_value_label(self, row_data): # (Logic unchanged, but uses cached_df now via update_obs_data)
        row_str, col_str = row_data["row"].get().strip(), row_data["col"].get().strip()
        file = self.file_path.get(); sheet = self.sheet_name.get()
        label_widget = row_data["value_label"]; label_widget.config(bootstyle=SECONDARY)
        if not file or not sheet or not row_str.isdigit() or not col_str.isdigit(): label_widget.config(text="N/A"); return
        if not os.path.exists(file): label_widget.config(text="File?", bootstyle=WARNING); return

        # --- Retrieve value using the main update logic which handles caching ---
        # We call a lightweight function to get the current value for this cell
        # This avoids re-reading the file just for the label update.
        # The actual file read/cache management happens in update_obs_data.
        current_value = self._get_cell_value_from_cache(int(row_str) - 1, int(col_str) - 1)

        if current_value is None: # Indicates error during cache read or out of range
             # Rely on update_obs_data to set more specific error text if needed soon
             label_widget.config(text="?", bootstyle=WARNING)
             return

        # Format and display value
        value_str = str(current_value)
        label_widget.config(text=value_str[:50] + ('...' if len(value_str)>50 else ''))

        # Update style based on previous value (if available)
        cell_id = (int(row_str) - 1, int(col_str) - 1)
        if cell_id in self.previous_values and self.previous_values[cell_id] != current_value:
            label_widget.config(bootstyle=INFO)
        else:
            label_widget.config(bootstyle=DEFAULT)


    def update_all_value_labels(self):
        logging.debug("Updating all value labels.")
        # Ensure the cache is potentially populated before updating labels
        # We can trigger a light version of update_obs_data just for reading
        self._ensure_excel_cache()
        for row_data in self.inputs_data:
            self.update_value_label(row_data)

    # --- OBS Connection Methods (unchanged) ---
    def connect_obs(self): # ... (same)
        if self._connecting: return
        self._connecting = True; self.connect_button.config(state=DISABLED)
        self.update_status("Connecting to OBS...", "info"); self.update_obs_status_label("Connecting...", WARNING)
        if self.obs_client:
             try:
                 disconnect_thread = threading.Thread(target=self.obs_client.disconnect, daemon=True, name="OBSDisconnectThread"); disconnect_thread.start()
                 disconnect_thread.join(timeout=DISCONNECT_TIMEOUT_SECONDS)
                 if disconnect_thread.is_alive(): logging.warning("Previous OBS disconnect timed out.")
             except Exception as e: logging.warning(f"Error during explicit OBS disconnect: {e}")
             finally:
                  with self.obs_connection_lock: self.obs_client = None; self.obs_connected = False
        host, password = self.obs_host_var.get().strip(), self.obs_password_var.get()
        port_str = self.obs_port_var.get().strip()
        if not port_str.isdigit():
            self.update_status("Invalid OBS Port: Must be a number.", "error"); logging.error(f"Invalid OBS Port provided: {port_str}")
            self.root.after(0, self._finalize_connection_attempt, False, "Invalid OBS Port"); return
        port = int(port_str)
        logging.info(f"Attempting to connect to OBS at {host}:{port}...")
        connect_thread = threading.Thread(target=self._obs_connect_worker, args=(host, port, password), daemon=True, name="OBSConnectThread"); connect_thread.start()
    def _obs_connect_worker(self, host, port, password): # ... (same)
        new_client, connected_successfully, error_message = None, False, ""
        try:
             new_client = obs.ReqClient(host=host, port=port, password=password if password else None, timeout=CONNECTION_TIMEOUT_SECONDS)
             connected_successfully = True
        except obs.ConnectionFailure as e: error_message = f"OBS Connection Failed: {e}"; logging.error(error_message)
        except Exception as e: error_message = f"OBS Connection Error: {e}"; logging.exception("OBS connection error.")
        finally:
            with self.obs_connection_lock: self.obs_client = new_client if connected_successfully else None; self.obs_connected = connected_successfully
            self.root.after(0, self._finalize_connection_attempt, connected_successfully, error_message)
    def _finalize_connection_attempt(self, success, error_msg): # ... (same)
         if success: logging.info("OBS Connected."); self.update_status("OBS Connected.", "success"); self.update_obs_status_label("Connected", SUCCESS)
         else: self.update_status(error_msg, "error"); self.update_obs_status_label("Disconnected", DANGER)
         self._connecting = False
         try: self.connect_button.config(state=NORMAL)
         except: pass
    def update_obs_status_label(self, text, style): # ... (same)
        def _update():
             if not self.running: return
             try: self.obs_status_label.config(text=f"OBS Status: {text}", bootstyle=style)
             except Exception as e: logging.warning(f"Could not update OBS status label: {e}")
        if hasattr(self.root, 'after'): self.root.after(0, _update)
        else: logging.warning("Cannot schedule OBS status label update.")
    def send_update_to_obs(self, data_type, value, source_name): # ... (same)
        with self.obs_connection_lock:
            if not self.obs_connected or not self.obs_client: return False
            if not source_name: logging.warning("Skipping update: OBS Source Name empty."); return False
            try:
                settings = {}; value_str = str(value)
                if data_type == "Text": settings = {"text": value_str}; logging.info(f"Updating OBS Text '{source_name}'")
                elif data_type == "Image": settings = {"file": value_str.strip()}; logging.info(f"Updating OBS Image '{source_name}'")
                else: logging.warning(f"Unknown data type '{data_type}'"); return False
                resp = self.obs_client.set_input_settings(name=source_name, settings=settings, overlay=True); logging.debug(f"OBS response '{source_name}': {resp}")
                return True
            except obs.ConnectionFailure: logging.error("OBS Connection Lost during update."); self.update_status("OBS Connection Lost.", "error"); self.obs_connected = False; self.root.after(0, self.update_obs_status_label, "Disconnected", DANGER); return False
            except OBSSDKRequestError as e: logging.error(f"Failed OBS update '{source_name}': {e}"); self.update_status(f"Error OBS '{source_name}': {e}", "error"); return False
            except Exception as e: logging.exception(f"Unexpected OBS update error '{source_name}': {e}"); self.update_status(f"Unexpected OBS Error for '{source_name}': {e}", "error"); return False

    # --- Data Handling (MODIFIED update_obs_data, Added helpers) ---

    def _ensure_excel_cache(self, force_read=False):
        """Reads Excel into cache if needed (file changed) or forced."""
        file = self.file_path.get()
        sheet = self.sheet_name.get()

        if not file or not sheet or not os.path.exists(file):
            # Clear cache if file is invalid or gone
            with self.excel_read_lock:
                self.cached_df = None
                self.last_excel_mtime = None
            return False # Indicate failure/no cache

        try:
            current_mtime = os.path.getmtime(file)
        except OSError as e:
             logging.error(f"Cannot get modification time for {file}: {e}")
             with self.excel_read_lock: # Clear cache on error
                 self.cached_df = None
                 self.last_excel_mtime = None
             return False

        # Acquire lock for checking/updating cache
        with self.excel_read_lock:
            if force_read or self.cached_df is None or current_mtime != self.last_excel_mtime:
                logging.debug(f"Reading Excel file '{os.path.basename(file)}' sheet '{sheet}'. Reason: {'Forced' if force_read else 'Cache miss or file changed'}")
                try:
                    start_time = time.time()
                    self.cached_df = pd.read_excel(file, sheet_name=sheet, engine='openpyxl', header=None, index_col=None)
                    read_time = time.time() - start_time
                    self.last_excel_mtime = current_mtime
                    logging.info(f"Excel cache updated in {read_time:.3f}s.")
                    return True # Cache updated
                except Exception as e:
                    # Keep old cache? Clear it? Let's clear it on error.
                    self.cached_df = None
                    self.last_excel_mtime = None
                    log_msg = f"Error reading Excel: {e}"
                    if "No sheet named" in str(e): log_msg = f"Error: Sheet '{sheet}' not found."
                    logging.error(log_msg + f" (File: {file})")
                    # Propagate error visually if it's a manual update?
                    # update_obs_data will handle status bar based on df being None
                    return False # Indicate error during read
            else:
                # logging.debug("Using cached Excel data.")
                return True # Cache is valid

    def _get_cell_value_from_cache(self, row, col):
        """Safely gets a single value from the cached DataFrame."""
        value = None
        error = False
        with self.excel_read_lock: # Lock for accessing cached_df
            if self.cached_df is not None:
                try:
                    if 0 <= row < len(self.cached_df) and 0 <= col < len(self.cached_df.columns):
                        val = self.cached_df.iloc[row, col]
                        if pd.isna(val): value = ""
                        elif isinstance(val, float) and val.is_integer(): value = int(val)
                        else: value = val
                    else:
                        # Cell out of range for the current cache
                        logging.warning(f"_get_cell_value: Cell [{row+1},{col+1}] out of range for cached data.")
                        error = True # Treat as error for label update
                except Exception as e:
                    logging.error(f"Error accessing cached DataFrame cell [{row+1},{col+1}]: {e}")
                    error = True
            else:
                # Cache is None (likely file read error or not yet read)
                error = True

        # Return None signals an error or invalid state to the caller (update_value_label)
        return None if error else value


    def update_obs_data(self, check_changes=False):
        """Reads data from cached Excel (if valid) and updates OBS."""

        # --- Ensure Cache is Up-to-Date ---
        # Force read only on manual update, otherwise rely on mtime check
        cache_valid = self._ensure_excel_cache(force_read=not check_changes)

        # --- Get DataFrame reference safely ---
        current_df = None
        with self.excel_read_lock: # Access cache under lock
            if cache_valid and self.cached_df is not None:
                current_df = self.cached_df
            # else: cache is invalid or df is None after read attempt

        # --- Proceed only if we have a valid DataFrame ---
        if current_df is None:
            if not check_changes: # Only show error on manual update press
                self.update_status("Cannot update OBS: Failed to read or cache Excel file.", "error")
            logging.warning("update_obs_data skipped: No valid Excel data available.")
            # Optionally update all labels to show error state?
            # for row_data in self.inputs_data: row_data['value_label'].config(text="Read?", bootstyle=WARNING)
            return # Cannot proceed

        # --- Process Mappings (using current_df) ---
        updates_sent, updates_attempted, rows_processed = 0, 0, 0
        df_rows, df_cols = current_df.shape # Get dimensions once

        for i, row_data in enumerate(self.inputs_data):
            rows_processed += 1
            row_str, col_str = row_data["row"].get().strip(), row_data["col"].get().strip()
            source_name, data_type = row_data["name"].get().strip(), row_data["data_type"].get()
            is_auto_update, label_widget = row_data["auto_update"].get() == 1, row_data["value_label"]

            if check_changes and not is_auto_update: continue
            if not row_str.isdigit() or not col_str.isdigit():
                if is_auto_update or not check_changes: logging.warning(f"Skipping row {i+1}: Invalid row/col.")
                label_widget.config(text="Num?", bootstyle=WARNING); continue
            row, col = int(row_str) - 1, int(col_str) - 1; cell_id = (row, col)

            # Use dimensions obtained earlier
            if not (0 <= row < df_rows and 0 <= col < df_cols):
                if is_auto_update or not check_changes: logging.warning(f"Skipping row {i+1}: Out of range.")
                label_widget.config(text="Range?", bootstyle=WARNING); continue

            try:
                # --- Get Value from DataFrame ---
                value = current_df.iloc[row, col] # Use the DataFrame directly
                if pd.isna(value): value = ""
                elif isinstance(value, float) and value.is_integer(): value = int(value)

                # Update display label immediately (value logic is now internal)
                # We need to call update_value_label to handle style changes etc.
                # This feels slightly redundant but keeps label logic separate.
                # Alternative: Directly update label here? Let's call the method.
                # self.update_value_label(row_data) # This would call _get_cell_value again, inefficient.
                # Direct label update:
                value_str_display = str(value)
                label_widget.config(text=value_str_display[:50] + ('...' if len(value_str_display)>50 else ''))

                # --- Check vs previous value & update OBS ---
                should_update_obs, previous_value = False, self.previous_values.get(cell_id, None)

                if not source_name: label_widget.config(bootstyle=DEFAULT); pass
                elif check_changes:
                    if previous_value != value: logging.info(f"Change detected '{source_name}'"); should_update_obs = True; label_widget.config(bootstyle=INFO)
                    else: label_widget.config(bootstyle=DEFAULT)
                else: # Manual update
                    should_update_obs = True
                    if previous_value != value: label_widget.config(bootstyle=INFO)
                    else: label_widget.config(bootstyle=DEFAULT)

                if should_update_obs:
                    updates_attempted += 1
                    if self.send_update_to_obs(data_type, value, source_name):
                        updates_sent += 1; self.previous_values[cell_id] = value
                    else: # Failed send
                        if previous_value != value: label_widget.config(bootstyle=WARNING)
                        else: label_widget.config(bootstyle=DANGER)

                elif not check_changes and cell_id not in self.previous_values:
                    self.previous_values[cell_id] = value # Store initial value on manual update

            except Exception as cell_error:
                logging.error(f"Error processing cell [{row+1},{col+1}] for source '{source_name}': {cell_error}")
                label_widget.config(text="Error", bootstyle=DANGER)

        # --- Log summary ---
        if not check_changes:
            status = f"Manual update: Processed {rows_processed}, Attempted {updates_attempted}, Successful {updates_sent}."
            log_level = "success" if updates_sent > 0 else ("warning" if updates_attempted > 0 else "info"); self.update_status(status, log_level)
        elif updates_sent > 0: logging.info(f"Auto-update: Sent {updates_sent} changes.")


    # --- Update Loop (Unchanged) ---
    def start_update_thread(self):
        if self.update_thread is None or not self.update_thread.is_alive():
            self.running = True; self.update_thread = threading.Thread(target=self.periodic_update_loop, daemon=True, name="UpdateThread"); self.update_thread.start(); logging.info("Background update thread started.")
        else: logging.warning("Update thread already running.")
    def periodic_update_loop(self):
        logging.info("Periodic update loop starting.")
        while self.running:
            start_cycle = time.time()
            try:
                with self.obs_connection_lock: is_connected = self.obs_connected
                if is_connected:
                    # Only call update if auto-updates are enabled for at least one row
                    if any(row["auto_update"].get() == 1 for row in self.inputs_data):
                        self.update_obs_data(check_changes=True) # Will use cached excel if possible

                elapsed = time.time() - start_cycle; sleep_time = max(0, UPDATE_INTERVAL_SECONDS - elapsed)
                # Improved sleep loop for faster shutdown response
                sleep_chunk = 0.1
                while self.running and sleep_time > 0:
                     actual_sleep = min(sleep_chunk, sleep_time)
                     time.sleep(actual_sleep)
                     sleep_time -= actual_sleep

            except Exception as e: logging.exception(f"Error in periodic update loop: {e}"); time.sleep(5)
        logging.info("Periodic update loop stopped.")

    # --- Import/Export (Modified import to reset cache) ---
    def export_settings(self): # ... (same logic as before)
        logging.info("Exporting settings...")
        settings_data = {
            "obs_settings": {"host": self.obs_host_var.get(), "port": self.obs_port_var.get(), "password": self.obs_password_var.get()},
            "excel_settings": {"file_path": self.file_path.get(), "sheet_name": self.sheet_name.get()},
            "mappings": [] }
        for row_data in self.inputs_data:
            try:
                 settings_data["mappings"].append({"type": row_data["data_type"].get(), "name": row_data["name"].get(), "row": row_data["row"].get(), "col": row_data["col"].get(), "auto_update": row_data["auto_update"].get()})
            except Exception as e: logging.error(f"Error exporting mapping row: {e}")
        try:
            file_path = filedialog.asksaveasfilename(title="Export Settings As", defaultextension=".json", filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
            if not file_path: logging.info("Export cancelled."); self.update_status("Export cancelled."); return
            with open(file_path, 'w', encoding='utf-8') as f: json.dump(settings_data, f, indent=4, ensure_ascii=False)
            logging.info(f"Settings exported to {file_path}"); self.update_status(f"Settings exported to {os.path.basename(file_path)}", "success")
        except Exception as e: logging.exception("Failed export settings."); self.update_status(f"Error exporting settings: {e}", "error")

    def import_settings(self):
        logging.info("Importing settings...")
        try:
            file_path = filedialog.askopenfilename(title="Import Settings From", filetypes=[("JSON files", "*.json"), ("All files", "*.*")])
            if not file_path: logging.info("Import cancelled."); self.update_status("Import cancelled."); return
            with open(file_path, 'r', encoding='utf-8') as f: settings_data = json.load(f)

            # Apply Settings
            obs_cfg = settings_data.get("obs_settings", {}); excel_cfg = settings_data.get("excel_settings", {})
            self.obs_host_var.set(obs_cfg.get("host", DEFAULT_OBS_WS_HOST)); self.obs_port_var.set(str(obs_cfg.get("port", DEFAULT_OBS_WS_PORT))); self.obs_password_var.set(obs_cfg.get("password", DEFAULT_OBS_WS_PASSWORD))
            self.file_path.set(excel_cfg.get("file_path", "")); self.sheet_name.set(excel_cfg.get("sheet_name", ""))

            # --- Reset Cache on Import ---
            with self.excel_read_lock:
                self.last_excel_mtime = None
                self.cached_df = None
            self.previous_values.clear() # Clear previous cell values state

            # Mappings
            mappings = settings_data.get("mappings", [])
            if not isinstance(mappings, list): raise ValueError("'mappings' must be a list.")
            # Clear existing rows UI
            for widget in self.inputs_frame.winfo_children(): widget.destroy()
            self.inputs_data.clear()
            # Add new rows
            logging.debug(f"Importing {len(mappings)} mappings.")
            for mapping in mappings: self.add_input_row(mapping_data=mapping)

            logging.info(f"Settings imported from {file_path}")
            self.update_status(f"Settings imported from {os.path.basename(file_path)}", "success")
            self.update_all_value_labels() # Update labels (will ensure cache is read)
            self.update_status("Settings imported. Reconnect to OBS if needed.", "info")

        except FileNotFoundError: logging.error(f"Import failed: File not found: {file_path}"); self.update_status("Error importing: File not found.", "error")
        except json.JSONDecodeError as e: logging.error(f"Import failed: Invalid JSON: {e}"); self.update_status(f"Error importing: Invalid JSON file.", "error")
        except Exception as e: logging.exception("Failed import settings."); self.update_status(f"Error importing settings: {e}", "error")

    # --- Stop Method (unchanged) ---
    def stop(self): # ... (same)
        if not self.running: return
        logging.info("Stop requested. Shutting down..."); self.update_status("Exiting...", "info"); self.running = False
        if self.update_thread and self.update_thread.is_alive():
            logging.debug("Waiting for update thread..."); self.update_thread.join(timeout=max(1.0, UPDATE_INTERVAL_SECONDS * 2))
            if self.update_thread.is_alive(): logging.warning("Update thread did not stop gracefully.")
        obs_client_local = None
        with self.obs_connection_lock:
             if self.obs_client and self.obs_connected: obs_client_local = self.obs_client; self.obs_connected = False; self.obs_client = None
        if obs_client_local:
             logging.info("Disconnecting from OBS...");
             try:
                 disconnect_thread = threading.Thread(target=obs_client_local.disconnect, daemon=True, name="OBSFinalDisconnect"); disconnect_thread.start()
                 disconnect_thread.join(timeout=DISCONNECT_TIMEOUT_SECONDS)
                 if disconnect_thread.is_alive(): logging.warning("OBS disconnection timed out.")
             except Exception as e: logging.error(f"Error during final OBS disconnection: {e}")
        logging.info("Destroying root window.")
        try: self.root.destroy()
        except Exception as e: logging.error(f"Error destroying root window: {e}")

# --- Main Execution (unchanged) ---
if __name__ == "__main__":
    root = ttk.Window(themename=DEFAULT_THEME)
    app = ExcelToOBS(root)
    try: root.mainloop()
    except KeyboardInterrupt: logging.info("KeyboardInterrupt received. Stopping..."); app.stop()
    except Exception as e: logging.exception("Unhandled exception in main loop."); app.stop()