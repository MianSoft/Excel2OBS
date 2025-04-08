# -*- coding: utf-8 -*-
import pandas as pd
import openpyxl  # Keep for reading .xlsm if needed, pandas handles .xlsx
# Adjusted imports for obsws-python exceptions
import obsws_python as obs
from obsws_python.error import OBSSDKRequestError
# ConnectionFailure will be accessed via obs.ConnectionFailure in except blocks

import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog
import logging
import os
import threading
import time
import queue # For thread-safe communication with GUI

# --- Configuration ---
OBS_WS_HOST = "localhost"         # OBS WebSocket host
OBS_WS_PORT = 4444                # OBS WebSocket port
OBS_WS_PASSWORD = "MianSoft3216"              # Add password if OBS WebSocket authentication is enabled
UPDATE_INTERVAL_SECONDS = 0.5     # How often to check Excel for changes (if auto-update is on)
DEFAULT_THEME = "litera"          # Example ttkbootstrap theme
CONNECTION_TIMEOUT_SECONDS = 5    # Timeout for OBS connection attempts
DISCONNECT_TIMEOUT_SECONDS = 2    # Timeout for OBS disconnection attempts
STATUS_QUEUE_CHECK_MS = 100       # How often to check the status queue (milliseconds)
LOG_LEVEL = logging.INFO          # Change to DEBUG for more verbose logs

# --- Logging Setup ---
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)

# --- Main Application Class ---
class ExcelToOBS:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel2OBS (Refactored) - B站: 直播说")
        # self.root.geometry("650x500") # Optional: Adjust initial size if needed

        # --- Style ---
        self.style = ttk.Style(theme=DEFAULT_THEME)

        # --- OBS Connection ---
        self.obs_client = None
        self.obs_connected = False
        self.obs_connection_lock = threading.Lock() # Lock for OBS client access and status variable
        self._connecting = False # Flag to prevent multiple concurrent connection attempts

        # --- State Variables ---
        self.file_path = ttk.StringVar()
        self.sheet_name = ttk.StringVar()
        self.inputs_data = []  # List to store data for each input row
        self.previous_values = {} # Stores {(row, col): value}
        self.running = True # Controls the main loop and background thread
        self.update_thread = None
        self.status_queue = queue.Queue() # Queue for status updates from thread

        # --- UI Setup ---
        self._setup_ui()

        # --- Start Background Tasks ---
        self.start_update_thread()
        # Start checking status queue shortly after UI is ready
        self.root.after(STATUS_QUEUE_CHECK_MS, self.process_status_queue)

        # --- Attempt Initial OBS Connection ---
        # Schedule the connection attempt slightly after the main loop starts
        self.root.after(500, self.connect_obs)

        # --- Graceful Shutdown ---
        self.root.protocol("WM_DELETE_WINDOW", self.stop)

    def _setup_ui(self):
        """Creates the user interface elements."""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=BOTH, expand=YES)

        # --- File Selection ---
        file_frame = ttk.LabelFrame(main_frame, text="Excel File Setup", padding="10")
        file_frame.pack(fill=X, pady=(0, 10))

        ttk.Label(file_frame, text="Excel File:").grid(row=0, column=0, padx=5, pady=5, sticky=W)
        self.file_entry = ttk.Entry(file_frame, textvariable=self.file_path, width=40)
        self.file_entry.grid(row=0, column=1, padx=5, pady=5, sticky=EW)
        ttk.Button(file_frame, text="Browse", command=self.choose_file, bootstyle=SECONDARY).grid(row=0, column=2, padx=5, pady=5)

        ttk.Label(file_frame, text="Sheet Name:").grid(row=1, column=0, padx=5, pady=5, sticky=W)
        self.sheet_entry = ttk.Entry(file_frame, textvariable=self.sheet_name, width=40)
        self.sheet_entry.grid(row=1, column=1, padx=5, pady=5, sticky=EW)
        # Allow pressing Enter in sheet name entry to potentially trigger an update/load
        self.sheet_entry.bind("<Return>", lambda event: self.update_obs_data(check_changes=False))

        file_frame.columnconfigure(1, weight=1) # Make entry expand

        # --- OBS Connection ---
        obs_frame = ttk.LabelFrame(main_frame, text="OBS Connection", padding="10")
        obs_frame.pack(fill=X, pady=(0, 10))
        self.obs_status_label = ttk.Label(obs_frame, text="OBS Status: Disconnected", width=25, anchor=W)
        self.obs_status_label.pack(side=LEFT, padx=(0, 10), fill=X, expand=True)
        self.connect_button = ttk.Button(obs_frame, text="Connect/Reconnect OBS", command=self.connect_obs, bootstyle=INFO)
        self.connect_button.pack(side=RIGHT)


        # --- Inputs Frame ---
        inputs_outer_frame = ttk.LabelFrame(main_frame, text="OBS Source Mapping", padding="10")
        inputs_outer_frame.pack(fill=BOTH, expand=YES, pady=(0, 10))

        # Header Row
        header_frame = ttk.Frame(inputs_outer_frame)
        header_frame.pack(fill=X)
        ttk.Label(header_frame, text="Type", width=8).pack(side=LEFT, padx=5)
        ttk.Label(header_frame, text="OBS Source Name", width=20).pack(side=LEFT, padx=5)
        ttk.Label(header_frame, text="Row", width=5).pack(side=LEFT, padx=5)
        ttk.Label(header_frame, text="Col", width=5).pack(side=LEFT, padx=5)
        ttk.Label(header_frame, text="Current Value", width=15).pack(side=LEFT, padx=5, expand=YES, fill=X)
        ttk.Label(header_frame, text="Auto?").pack(side=LEFT, padx=5)
        ttk.Label(header_frame, text="Del", width=4).pack(side=RIGHT, padx=5) # Delete Button placeholder

        # Scrollable Frame for Inputs
        canvas = ttk.Canvas(inputs_outer_frame)
        scrollbar = ttk.Scrollbar(inputs_outer_frame, orient=VERTICAL, command=canvas.yview, bootstyle=ROUND)
        self.inputs_frame = ttk.Frame(canvas) # Frame that holds the actual input rows

        self.inputs_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(
                scrollregion=canvas.bbox("all")
            )
        )

        canvas.create_window((0, 0), window=self.inputs_frame, anchor=NW)
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side=LEFT, fill=BOTH, expand=YES)
        scrollbar.pack(side=RIGHT, fill=Y)

        # --- Buttons ---
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=X, pady=(5, 0))

        ttk.Button(button_frame, text="Add Mapping", command=self.add_input_row, bootstyle=SUCCESS).pack(side=LEFT, padx=5)
        ttk.Button(button_frame, text="Update OBS Now", command=lambda: self.update_obs_data(check_changes=False), bootstyle=PRIMARY).pack(side=LEFT, padx=5)

        # --- Status Bar ---
        self.status_bar = ttk.Label(self.root, text="Ready.", anchor=W, relief=SUNKEN, padding=(5, 2))
        self.status_bar.pack(side=BOTTOM, fill=X)

        # --- Add Initial Row ---
        if not self.inputs_data:
             self.add_input_row()

    def update_status(self, message, level="info"):
        """Safely updates the status bar from any thread."""
        if self.running: # Avoid queuing updates during shutdown
            try:
                self.status_queue.put((message, level))
            except Exception as e:
                logging.error(f"Failed to put message in status queue: {e}")


    def process_status_queue(self):
        """Processes status messages from the queue in the main thread."""
        if not self.running: # Stop processing if app is shutting down
             return
        try:
            while True: # Process all messages currently in the queue
                message, level = self.status_queue.get_nowait()
                color_map = {"info": DEFAULT, "success": SUCCESS, "warning": WARNING, "error": DANGER}
                self.status_bar.config(text=str(message)[:200], bootstyle=color_map.get(level, DEFAULT)) # Limit length
                # Log errors/warnings from status updates as well
                if level == "error":
                    logging.error(f"Status Update: {message}")
                elif level == "warning":
                     logging.warning(f"Status Update: {message}")
                # else: # Avoid logging every info message twice
                    # logging.info(f"Status Update: {message}")
                self.status_queue.task_done() # Mark task as done
        except queue.Empty:
            pass # No more messages for now
        except Exception as e:
             logging.error(f"Error processing status queue: {e}")
        finally:
            # Reschedule after processing
            if self.running:
                 self.root.after(STATUS_QUEUE_CHECK_MS, self.process_status_queue)

    def choose_file(self):
        """Opens a dialog to choose an Excel file."""
        try:
            path = filedialog.askopenfilename(
                title="Select Excel File",
                filetypes=[("Excel files", "*.xlsx;*.xlsm"), ("All files", "*.*")]
            )
            if path:
                self.file_path.set(path)
                self.update_status(f"Selected file: {os.path.basename(path)}")
                logging.info(f'Selected file: {path}')
                # Optionally trigger an immediate value update for all rows
                self.update_all_value_labels() # Update labels immediately
                # Clear previous values as the file context has changed
                self.previous_values.clear()
        except Exception as e:
            logging.exception("Error choosing file.")
            self.update_status(f"Error choosing file: {e}", "error")


    def add_input_row(self):
        """Adds a new row for mapping Excel cell to OBS source."""
        row_frame = ttk.Frame(self.inputs_frame)
        row_frame.pack(fill=X, pady=2)

        data_type_var = ttk.StringVar(value="Text")
        row_var = ttk.StringVar()
        col_var = ttk.StringVar()
        name_var = ttk.StringVar()
        value_label = ttk.Label(row_frame, text="N/A", width=15, anchor=W, relief=SUNKEN, padding=(3,0), bootstyle=SECONDARY) # Give it some width and style
        check_var = ttk.IntVar(value=0) # Default auto-update off

        # Store widgets and variables for later access
        row_data = {
            "frame": row_frame,
            "data_type": data_type_var,
            "row": row_var,
            "col": col_var,
            "name": name_var,
            "value_label": value_label,
            "auto_update": check_var,
        }
        self.inputs_data.append(row_data)
        row_index = len(self.inputs_data) - 1 # Get index for delete command

        # --- Create and place widgets ---
        # Data Type
        data_type_menu = ttk.OptionMenu(row_frame, data_type_var, "Text", "Text", "Image")
        data_type_menu.config(width=5)
        data_type_menu.pack(side=LEFT, padx=5)

        # OBS Source Name
        name_entry = ttk.Entry(row_frame, textvariable=name_var, width=20)
        name_entry.pack(side=LEFT, padx=5)
        name_entry.bind("<KeyRelease>", lambda event, r=row_data: self._check_update_needed(r))

        # Row Entry
        row_entry = ttk.Entry(row_frame, textvariable=row_var, width=5)
        row_entry.pack(side=LEFT, padx=5)
        row_entry.bind("<KeyRelease>", lambda event, r=row_data: self.update_value_label(r)) # Update value label on change

        # Column Entry
        col_entry = ttk.Entry(row_frame, textvariable=col_var, width=5)
        col_entry.pack(side=LEFT, padx=5)
        col_entry.bind("<KeyRelease>", lambda event, r=row_data: self.update_value_label(r)) # Update value label on change

        # Value Label
        value_label.pack(side=LEFT, padx=5, expand=YES, fill=X) # Label expands

        # Auto-Update Checkbox
        check_button = ttk.Checkbutton(row_frame, variable=check_var, bootstyle=(PRIMARY, TOOLBUTTON))
        check_button.pack(side=LEFT, padx=(5, 10)) # Add padding

        # Delete button for this row
        del_button = ttk.Button(row_frame, text="X", command=lambda idx=row_index: self.delete_input_row(idx), bootstyle=(DANGER, OUTLINE), width=3)
        del_button.pack(side=RIGHT, padx=5)

        self.update_status(f"Added new mapping row.")
        # Update the label for the newly added row immediately if possible
        self.update_value_label(row_data)


    def delete_input_row(self, index):
        """Removes an input row from the UI and data list."""
        if 0 <= index < len(self.inputs_data):
            row_data = self.inputs_data.pop(index)
            row_data["frame"].destroy()
            logging.info(f"Deleted input row at index {index}")
            self.update_status(f"Deleted mapping row.")
            # Re-assign delete commands for remaining rows due to index shift
            self._update_delete_button_commands()
            # Clear previous value associated with the deleted row's potential cell
            try:
                 row_str = row_data["row"].get()
                 col_str = row_data["col"].get()
                 if row_str.isdigit() and col_str.isdigit():
                     cell_id = (int(row_str) - 1, int(col_str) - 1)
                     self.previous_values.pop(cell_id, None) # Remove if exists
            except Exception:
                logging.warning("Could not clear previous_value for deleted row.")


    def _update_delete_button_commands(self):
         """Updates the command for all delete buttons after a deletion."""
         for i, row_data in enumerate(self.inputs_data):
             # Find the delete button in the row's frame children
             for widget in row_data["frame"].winfo_children():
                 if isinstance(widget, ttk.Button) and widget.cget("text") == "X":
                     widget.configure(command=lambda idx=i: self.delete_input_row(idx))
                     break

    def _check_update_needed(self, row_data):
         """Placeholder in case logic is needed when source name changes"""
         pass

    def update_value_label(self, row_data):
        """Updates the 'Value' label for a specific row based on Excel data."""
        row_str = row_data["row"].get().strip()
        col_str = row_data["col"].get().strip()
        file = self.file_path.get()
        sheet = self.sheet_name.get()
        label_widget = row_data["value_label"]

        # Reset style initially
        label_widget.config(bootstyle=SECONDARY) # Default style

        if not file or not sheet or not row_str.isdigit() or not col_str.isdigit():
            label_widget.config(text="N/A")
            return # Not enough info or invalid row/col

        if not os.path.exists(file):
            label_widget.config(text="File?", bootstyle=WARNING)
            return

        row = int(row_str) - 1  # 1-based index to 0-based
        col = int(col_str) - 1
        cell_id = (row, col) # For checking previous value

        try:
            # --- Read from Excel ---
            df = pd.read_excel(file, sheet_name=sheet, engine='openpyxl', header=None, index_col=None)

            if 0 <= row < len(df) and 0 <= col < len(df.columns):
                value = df.iloc[row, col]
                if pd.isna(value): value = ""
                elif isinstance(value, float) and value.is_integer(): value = int(value)

                value_str = str(value)
                label_widget.config(text=value_str[:50] + ('...' if len(value_str)>50 else ''))

                if cell_id in self.previous_values and self.previous_values[cell_id] != value:
                    label_widget.config(bootstyle=INFO)
                else:
                     label_widget.config(bootstyle=DEFAULT)

            else:
                label_widget.config(text="Range?", bootstyle=WARNING)
                logging.warning(f"Row {row+1} or Column {col+1} out of range for sheet '{sheet}'.")

        except FileNotFoundError:
             label_widget.config(text="File?", bootstyle=WARNING)
        except Exception as e:
            label_widget.config(text="Error", bootstyle=DANGER)
            if "No sheet named" in str(e):
                 logging.warning(f"Sheet '{sheet}' not found in '{os.path.basename(file)}'.")
            else:
                 self.update_status(f"Error reading Excel: {e}", "error")
                 logging.error(f'Error reading Excel file "{file}", sheet "{sheet}" for cell [{row+1},{col+1}]: {e}')

    def update_all_value_labels(self):
        """Calls update_value_label for all input rows."""
        logging.debug("Updating all value labels.")
        for row_data in self.inputs_data:
            self.update_value_label(row_data)


    # --- OBS Interaction ---

    def connect_obs(self):
        """Connects or reconnects to the OBS WebSocket."""
        if self._connecting:
            logging.warning("OBS connection attempt already in progress.")
            return

        self._connecting = True
        self.connect_button.config(state=DISABLED)
        self.update_status("Connecting to OBS...", "info")
        self.update_obs_status_label("Connecting...", WARNING)

        if self.obs_client:
             try:
                 disconnect_thread = threading.Thread(target=self.obs_client.disconnect, daemon=True, name="OBSDisconnectThread")
                 disconnect_thread.start()
                 disconnect_thread.join(timeout=DISCONNECT_TIMEOUT_SECONDS)
                 if disconnect_thread.is_alive(): logging.warning("Previous OBS disconnect timed out.")
             except Exception as e:
                 logging.warning(f"Error during explicit OBS disconnect: {e}")
             finally:
                  with self.obs_connection_lock:
                       self.obs_client = None
                       self.obs_connected = False

        logging.info(f"Attempting to connect to OBS at {OBS_WS_HOST}:{OBS_WS_PORT}...")
        connect_thread = threading.Thread(target=self._obs_connect_worker, daemon=True, name="OBSConnectThread")
        connect_thread.start()


    def _obs_connect_worker(self):
        """Worker function to handle OBS connection attempt."""
        new_client = None
        connected_successfully = False
        error_message = ""

        try:
             new_client = obs.ReqClient(host=OBS_WS_HOST,
                                        port=OBS_WS_PORT,
                                        password=OBS_WS_PASSWORD if OBS_WS_PASSWORD else None,
                                        timeout=CONNECTION_TIMEOUT_SECONDS)
             connected_successfully = True

        # --- CORRECTED EXCEPTION HANDLING ---
        # Catch ConnectionFailure from the main 'obs' object
        except obs.ConnectionFailure as e:
            error_message = f"OBS Connection Failed: {e}"
            logging.error(error_message)
        # Catch other potential errors
        except Exception as e:
             error_message = f"OBS Connection Error: {e}"
             logging.exception("An unexpected error occurred during OBS connection.")
        # --- END CORRECTION ---

        finally:
            with self.obs_connection_lock:
                self.obs_client = new_client if connected_successfully else None
                self.obs_connected = connected_successfully
            self.root.after(0, self._finalize_connection_attempt, connected_successfully, error_message)


    def _finalize_connection_attempt(self, success, error_msg):
         """Updates UI elements after connection attempt (runs in main thread)."""
         if success:
             logging.info("Successfully connected to OBS.")
             self.update_status("OBS Connected.", "success")
             self.update_obs_status_label("Connected", SUCCESS)
         else:
             self.update_status(error_msg, "error")
             self.update_obs_status_label("Disconnected", DANGER)

         self._connecting = False
         self.connect_button.config(state=NORMAL)

    def update_obs_status_label(self, text, style):
        """Updates the OBS connection status label (must run in main thread or be scheduled)."""
        def _update():
             if not self.running: return
             try:
                 self.obs_status_label.config(text=f"OBS Status: {text}", bootstyle=style)
             except Exception as e:
                 logging.warning(f"Could not update OBS status label: {e}")

        if hasattr(self.root, 'after'):
            self.root.after(0, _update)
        else:
             logging.warning("Cannot schedule OBS status label update: root window not available.")


    def send_update_to_obs(self, data_type, value, source_name):
        """Sends data to the specified OBS source using obsws-python."""
        with self.obs_connection_lock:
            if not self.obs_connected or not self.obs_client:
                return False

            if not source_name:
                logging.warning("Skipping update: OBS Source Name is empty.")
                return False

            try:
                settings = {}
                value_str = str(value)

                if data_type == "Text":
                    settings = {"text": value_str}
                    logging.info(f"Updating OBS Text Source '{source_name}' with text: '{value_str[:50]}...'")
                elif data_type == "Image":
                    clean_path = value_str.strip()
                    settings = {"file": clean_path}
                    logging.info(f"Updating OBS Image Source '{source_name}' with path: '{clean_path}'")
                else:
                    logging.warning(f"Unknown data type '{data_type}' for source '{source_name}'.")
                    return False

                resp = self.obs_client.set_input_settings(name=source_name, settings=settings, overlay=True)
                logging.debug(f"OBS response for '{source_name}': {resp}")
                return True

            # --- CORRECTED EXCEPTION HANDLING ---
            # Catch ConnectionFailure from the main 'obs' object
            except obs.ConnectionFailure:
                logging.error("OBS Connection Lost during update.")
                self.update_status("OBS Connection Lost.", "error")
                self.obs_connected = False
                self.root.after(0, self.update_obs_status_label, "Disconnected", DANGER)
                return False
            # Catch specific SDK errors
            except OBSSDKRequestError as e:
                error_str = f"Failed to update OBS source '{source_name}': {e}"
                logging.error(error_str)
                self.update_status(f"Error updating '{source_name}': {e}", "error")
                if "not found" in str(e).lower():
                     logging.error(f"Verify that an input named '{source_name}' exists in OBS.")
                     self.update_status(f"Error: OBS Source '{source_name}' not found.", "error")
                return False
            # Catch other potential errors
            except Exception as e:
                logging.exception(f"An unexpected error occurred while updating OBS source '{source_name}': {e}")
                self.update_status(f"Unexpected OBS Error for '{source_name}': {e}", "error")
                return False
            # --- END CORRECTION ---
        # End of lock block

    # --- Data Handling and Update Loop ---

    def update_obs_data(self, check_changes=False):
        """Reads data from Excel for all inputs and updates OBS."""
        file = self.file_path.get()
        sheet = self.sheet_name.get()

        if not file or not sheet:
            if not check_changes: self.update_status("Select Excel file and sheet name first.", "warning")
            logging.warning("Update skipped: Excel file or sheet name missing.")
            return

        if not os.path.exists(file):
            if not check_changes: self.update_status(f"Error: File not found: {file}", "error")
            logging.error(f"Update failed: File not found: {file}")
            return

        updates_sent = 0
        updates_attempted = 0
        rows_processed = 0

        try:
            start_time = time.time()
            df = pd.read_excel(file, sheet_name=sheet, engine='openpyxl', header=None, index_col=None)
            read_time = time.time() - start_time
            logging.debug(f"Loaded Excel sheet '{sheet}' from '{os.path.basename(file)}' in {read_time:.3f}s.")

            for i, row_data in enumerate(self.inputs_data):
                rows_processed += 1
                row_str = row_data["row"].get().strip()
                col_str = row_data["col"].get().strip()
                source_name = row_data["name"].get().strip()
                data_type = row_data["data_type"].get()
                is_auto_update = row_data["auto_update"].get() == 1
                label_widget = row_data["value_label"]

                if check_changes and not is_auto_update: continue

                if not row_str.isdigit() or not col_str.isdigit():
                    if is_auto_update or not check_changes: logging.warning(f"Skipping row {i+1}: Invalid row ('{row_str}') or column ('{col_str}'). Must be numbers.")
                    label_widget.config(text="Num?", bootstyle=WARNING)
                    continue

                row = int(row_str) - 1
                col = int(col_str) - 1
                cell_id = (row, col)

                if not (0 <= row < len(df) and 0 <= col < len(df.columns)):
                    if is_auto_update or not check_changes: logging.warning(f"Skipping row {i+1}: Row {row+1} or Column {col+1} is out of range for sheet '{sheet}'.")
                    label_widget.config(text="Range?", bootstyle=WARNING)
                    continue

                try:
                    value = df.iloc[row, col]
                    if pd.isna(value): value = ""
                    elif isinstance(value, float) and value.is_integer(): value = int(value)

                    value_str_display = str(value)
                    label_widget.config(text=value_str_display[:50] + ('...' if len(value_str_display)>50 else ''))

                    should_update_obs = False
                    previous_value = self.previous_values.get(cell_id, None)

                    if not source_name:
                         label_widget.config(bootstyle=DEFAULT)
                         pass
                    elif check_changes:
                        if previous_value != value:
                            logging.info(f"Change detected for '{source_name}' [{row+1},{col+1}]: '{previous_value}' -> '{value}'")
                            should_update_obs = True
                            label_widget.config(bootstyle=INFO)
                        else:
                            label_widget.config(bootstyle=DEFAULT)
                    else:
                         should_update_obs = True
                         if previous_value != value: label_widget.config(bootstyle=INFO)
                         else: label_widget.config(bootstyle=DEFAULT)

                    if should_update_obs:
                        updates_attempted += 1
                        if self.send_update_to_obs(data_type, value, source_name):
                             updates_sent += 1
                             self.previous_values[cell_id] = value
                        else:
                             if previous_value != value: label_widget.config(bootstyle=WARNING)
                             else: label_widget.config(bootstyle=DANGER)

                    elif not check_changes and cell_id not in self.previous_values:
                         self.previous_values[cell_id] = value

                except Exception as cell_error:
                    logging.error(f"Error processing cell [{row+1},{col+1}] for source '{source_name}': {cell_error}")
                    label_widget.config(text="Error", bootstyle=DANGER)

            if not check_changes:
                status = f"Manual update complete. Processed {rows_processed} rows. Attempted {updates_attempted} OBS updates, {updates_sent} successful."
                log_level = "success" if updates_sent > 0 else ("warning" if updates_attempted > 0 else "info")
                self.update_status(status, log_level)
            else:
                 if updates_sent > 0: logging.info(f"Auto-update: Sent {updates_sent} changes to OBS.")

        except FileNotFoundError:
             if not check_changes: self.update_status(f"Error: File not found: {file}", "error")
             logging.error(f"Update failed: File not found: {file}")
        except Exception as e:
            log_msg = f"Error loading/processing Excel: {e}"
            if "No sheet named" in str(e):
                 log_msg = f"Error: Sheet '{sheet}' not found in '{os.path.basename(file)}'."
                 logging.error(log_msg)
                 if not check_changes: self.update_status(log_msg, "error")
            else:
                logging.exception(f'Critical error during update cycle for file "{file}", sheet "{sheet}"')
                if not check_changes: self.update_status(f"Critical Error reading Excel: {e}", "error")


    def start_update_thread(self):
        """Starts the background thread for periodic updates if not already running."""
        if self.update_thread is None or not self.update_thread.is_alive():
            self.running = True
            self.update_thread = threading.Thread(target=self.periodic_update_loop, daemon=True, name="UpdateThread")
            self.update_thread.start()
            logging.info("Background update thread started.")
        else:
            logging.warning("Update thread already running.")

    def periodic_update_loop(self):
        """The loop running in the background thread."""
        logging.info("Periodic update loop starting.")
        while self.running:
            start_cycle = time.time()
            try:
                with self.obs_connection_lock:
                    is_connected = self.obs_connected

                if is_connected:
                    if any(row["auto_update"].get() == 1 for row in self.inputs_data):
                         self.update_obs_data(check_changes=True)

                elapsed = time.time() - start_cycle
                sleep_time = max(0, UPDATE_INTERVAL_SECONDS - elapsed)
                for _ in range(int(sleep_time / 0.1) + 1):
                     if not self.running: break
                     time.sleep(min(0.1, sleep_time))
                     sleep_time -= 0.1
                     if sleep_time <= 0: break

            except Exception as e:
                logging.exception(f"Error in periodic update loop: {e}")
                if self.running: time.sleep(5)

        logging.info("Periodic update loop stopped.")


    def stop(self):
        """Stops the application, background thread, and disconnects OBS."""
        if not self.running: return
        logging.info("Stop requested. Shutting down...")
        self.update_status("Exiting...", "info")
        self.running = False

        if self.update_thread and self.update_thread.is_alive():
            logging.debug("Waiting for update thread to finish...")
            self.update_thread.join(timeout=max(1.0, UPDATE_INTERVAL_SECONDS * 2))
            if self.update_thread.is_alive(): logging.warning("Update thread did not stop gracefully.")

        obs_client_local = None
        with self.obs_connection_lock:
             if self.obs_client and self.obs_connected:
                  obs_client_local = self.obs_client
                  self.obs_connected = False
                  self.obs_client = None

        if obs_client_local:
             logging.info("Disconnecting from OBS...")
             try:
                 disconnect_thread = threading.Thread(target=obs_client_local.disconnect, daemon=True, name="OBSFinalDisconnect")
                 disconnect_thread.start()
                 disconnect_thread.join(timeout=DISCONNECT_TIMEOUT_SECONDS)
                 if disconnect_thread.is_alive(): logging.warning("OBS disconnection timed out during shutdown.")
             except Exception as e:
                 logging.error(f"Error during final OBS disconnection: {e}")

        logging.info("Destroying root window.")
        try:
            # Ensure destroy is called from the main thread if possible
            # If stop() is called from WM_DELETE_WINDOW, it's already in the main thread.
            # If called otherwise, might need self.root.after(0, self.root.destroy)
            self.root.destroy()
        except Exception as e:
             logging.error(f"Error destroying root window: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    root = ttk.Window(themename=DEFAULT_THEME)
    app = ExcelToOBS(root)
    try:
        root.mainloop()
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received. Stopping...")
        app.stop()
    except Exception as e:
         logging.exception("Unhandled exception in main loop.")
         try: app.stop()
         except: pass