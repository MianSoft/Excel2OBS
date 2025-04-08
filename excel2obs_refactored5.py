# -*- coding: utf-8 -*-
import pandas as pd
import openpyxl
import obsws_python as obs
from obsws_python.error import OBSSDKRequestError
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog, TclError
import logging
import os
import threading
import time
import queue
import json
from collections import defaultdict

# --- Configuration (Defaults) ---
DEFAULT_OBS_WS_HOST = "localhost"
DEFAULT_OBS_WS_PORT = 4444
DEFAULT_OBS_WS_PASSWORD = "MianSoft3216"
UPDATE_INTERVAL_SECONDS = 0.5
DEFAULT_THEME = "litera"
CONNECTION_TIMEOUT_SECONDS = 5
DISCONNECT_TIMEOUT_SECONDS = 2
STATUS_QUEUE_CHECK_MS = 100
LOG_LEVEL = logging.INFO
DEFAULT_GROUP_NAME = "Default Group"
EXPAND_SYMBOL = "▼"
COLLAPSE_SYMBOL = "▲"

# --- Logging Setup ---
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'
)

# --- Main Application Class ---
class ExcelToOBS:
    def __init__(self, root):
        self.root = root
        self.root.title("Excel2OBS (Collapsible Groups + More Types) - 原版: B站: 直播说") # Updated Title

        self.style = ttk.Style(theme=DEFAULT_THEME)
        self.obs_host_var = ttk.StringVar(value=DEFAULT_OBS_WS_HOST)
        self.obs_port_var = ttk.StringVar(value=str(DEFAULT_OBS_WS_PORT))
        self.obs_password_var = ttk.StringVar(value=DEFAULT_OBS_WS_PASSWORD)
        self.obs_client = None
        self.obs_connected = False
        self.obs_connection_lock = threading.Lock()
        self._connecting = False
        self.file_path = ttk.StringVar()
        self.sheet_name = ttk.StringVar()
        self.inputs_data = []
        self.previous_values = {}
        self.running = True
        self.update_thread = None
        self.status_queue = queue.Queue()
        self.last_excel_mtime = None
        self.cached_df = None
        self.excel_read_lock = threading.Lock()

        self._setup_ui()
        self.start_update_thread()
        self.root.after(STATUS_QUEUE_CHECK_MS, self.process_status_queue)
        self.root.after(500, self.connect_obs)
        self.root.protocol("WM_DELETE_WINDOW", self.stop)

    def _setup_ui(self):
        # (Setup is largely unchanged, only the OptionMenu choices in add_input_row change)
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=BOTH, expand=YES)

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

        inputs_outer_frame = ttk.LabelFrame(main_frame, text="OBS Source Mapping Groups", padding="10")
        inputs_outer_frame.pack(fill=BOTH, expand=YES, pady=(0, 10))
        canvas = ttk.Canvas(inputs_outer_frame)
        scrollbar = ttk.Scrollbar(inputs_outer_frame, orient=VERTICAL, command=canvas.yview, bootstyle=ROUND)
        self.groups_container_frame = ttk.Frame(canvas)
        self.groups_container_frame.bind( "<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")) )
        canvas.create_window((0, 0), window=self.groups_container_frame, anchor=NW)
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.pack(side=LEFT, fill=BOTH, expand=YES)
        scrollbar.pack(side=RIGHT, fill=Y)

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=X, pady=(5, 0))
        ttk.Button(button_frame, text="Add Group", command=self.add_group, bootstyle=SUCCESS).pack(side=LEFT, padx=5)
        ttk.Button(button_frame, text="Update OBS Now", command=lambda: self.update_obs_data(check_changes=False), bootstyle=PRIMARY).pack(side=LEFT, padx=5)
        ttk.Button(button_frame, text="Import Settings", command=self.import_settings, bootstyle=SECONDARY).pack(side=RIGHT, padx=5)
        ttk.Button(button_frame, text="Export Settings", command=self.export_settings, bootstyle=SECONDARY).pack(side=RIGHT, padx=5)

        self.status_bar = ttk.Label(self.root, text="Ready.", anchor=W, relief=SUNKEN, padding=(5, 2))
        self.status_bar.pack(side=BOTTOM, fill=X)

        self.inputs_data = []
        if not self.inputs_data: self.add_group(group_name=DEFAULT_GROUP_NAME)

    def update_status(self, message, level="info"):
        if self.running:
            try: self.status_queue.put((message, level))
            except Exception as e: logging.error(f"Failed to put message in status queue: {e}")

    def process_status_queue(self):
        if not self.running: return
        try:
            while True:
                message, level = self.status_queue.get_nowait()
                color_map = {"info": DEFAULT, "success": SUCCESS, "warning": WARNING, "error": DANGER}
                style_name = self._get_style_name(color_map.get(level, DEFAULT), "TLabel")
                self.status_bar.config(text=str(message)[:200], style=style_name)
                if level == "error": logging.error(f"Status Update: {message}")
                elif level == "warning": logging.warning(f"Status Update: {message}")
                self.status_queue.task_done()
        except queue.Empty: pass
        except TclError as e: logging.error(f"Error updating status bar style: {e} (Message: {message}, Level: {level})")
        except Exception as e: logging.error(f"Error processing status queue: {e}")
        finally:
            if self.running: self.root.after(STATUS_QUEUE_CHECK_MS, self.process_status_queue)

    def choose_file(self):
        try:
            path = filedialog.askopenfilename(title="Select Excel File", filetypes=[("Excel files", "*.xlsx;*.xlsm"), ("All files", "*.*")])
            if path:
                self.file_path.set(path); self.update_status(f"Selected file: {os.path.basename(path)}")
                logging.info(f'Selected file: {path}')
                with self.excel_read_lock: self.last_excel_mtime = None; self.cached_df = None
                self.previous_values.clear(); self.update_all_value_labels()
        except Exception as e: logging.exception("Error choosing file."); self.update_status(f"Error choosing file: {e}", "error")

    def add_group(self, group_data=None, group_name=None):
        group_index = len(self.inputs_data)
        is_expanded = True
        group_outer_frame = ttk.LabelFrame(self.groups_container_frame, text="", padding=5)
        group_outer_frame.pack(fill=X, pady=(5,0), padx=5)
        header_frame = ttk.Frame(group_outer_frame); header_frame.pack(fill=X, pady=(0, 5))
        toggle_button = ttk.Button(header_frame, text=COLLAPSE_SYMBOL if is_expanded else EXPAND_SYMBOL, command=lambda idx=group_index: self._toggle_group(idx), bootstyle=(SECONDARY, OUTLINE), width=2)
        toggle_button.pack(side=LEFT, padx=(0, 5))
        group_name_var = ttk.StringVar()
        group_name_entry = ttk.Entry(header_frame, textvariable=group_name_var, width=30)
        group_name_entry.pack(side=LEFT, padx=(0, 5), expand=True, fill=X)
        group_name_entry.bind("<FocusOut>", lambda e, idx=group_index: self._update_group_name(idx))
        group_name_entry.bind("<Return>", lambda e, idx=group_index: self._update_group_name(idx))
        add_mapping_button = ttk.Button(header_frame, text="Add Mapping", command=lambda idx=group_index: self.add_input_row(idx), bootstyle=(SUCCESS, OUTLINE), width=12)
        add_mapping_button.pack(side=LEFT, padx=5)
        delete_group_button = ttk.Button(header_frame, text="Delete Group", command=lambda idx=group_index: self.delete_group(idx), bootstyle=(DANGER, OUTLINE), width=12)
        delete_group_button.pack(side=RIGHT, padx=5)
        collapsible_content_frame = ttk.Frame(group_outer_frame)
        if is_expanded: collapsible_content_frame.pack(fill=X, pady=(5, 0))
        separator = ttk.Separator(collapsible_content_frame, orient=HORIZONTAL); separator.pack(fill=X, pady=0)
        mapping_header_frame = ttk.Frame(collapsible_content_frame); mapping_header_frame.pack(fill=X, pady=(5,0))
        ttk.Label(mapping_header_frame, text="Type", width=16).pack(side=LEFT, padx=5)
        ttk.Label(mapping_header_frame, text="OBS Source Name", width=22).pack(side=LEFT, padx=5)
        ttk.Label(mapping_header_frame, text="Row", width=6).pack(side=LEFT, padx=5); ttk.Label(mapping_header_frame, text="Col", width=6).pack(side=LEFT, padx=5)
        ttk.Label(mapping_header_frame, text="Current Value", width=15).pack(side=LEFT, padx=5, expand=YES, fill=X)
        ttk.Label(mapping_header_frame, text="Auto").pack(side=LEFT, padx=5); ttk.Label(mapping_header_frame, text="Del", width=6).pack(side=RIGHT, padx=5)
        mappings_frame = ttk.Frame(collapsible_content_frame); mappings_frame.pack(fill=X, pady=(0, 5))
        new_group = {
            "frame": group_outer_frame, "header_frame": header_frame, "name_var": group_name_var,
            "mappings_frame": mappings_frame, "collapsible_content_frame": collapsible_content_frame,
            "mapping_header_frame": mapping_header_frame, "separator": separator, "mappings": [],
            "delete_button": delete_group_button, "add_mapping_button": add_mapping_button,
            "toggle_button": toggle_button, "is_expanded": is_expanded
        }
        self.inputs_data.append(new_group)
        is_new_group = True; final_group_name = None
        if group_data and isinstance(group_data, dict):
            final_group_name = group_data.get("group_name", f"Group {group_index + 1}"); is_new_group = False
            logging.debug(f"Populating group '{final_group_name}' with {len(group_data.get('mappings',[]))} mappings from data.")
            for mapping_data in group_data.get("mappings", []): self.add_input_row(group_index, mapping_data=mapping_data)
        else: final_group_name = group_name if group_name else f"Group {group_index + 1}"
        group_name_var.set(final_group_name); group_outer_frame.config(text=final_group_name)
        if is_new_group:
            self.add_input_row(group_index); logging.info(f"Added new group '{final_group_name}'.")
            self.update_status(f"Added new group '{final_group_name}'.")
        self._update_dynamic_commands()

    def _update_group_name(self, group_index):
        if 0 <= group_index < len(self.inputs_data):
            group_data = self.inputs_data[group_index]
            new_name = group_data["name_var"].get().strip()
            if not new_name: new_name = f"Group {group_index + 1}"; group_data["name_var"].set(new_name)
            try: group_data["frame"].config(text=new_name); logging.debug(f"Updated group {group_index} name to '{new_name}'")
            except TclError as e: logging.error(f"Error updating group LabelFrame text for index {group_index}: {e}")
            except Exception as e: logging.error(f"Unexpected error updating group name for index {group_index}: {e}")

    def delete_group(self, group_index):
        if not (0 <= group_index < len(self.inputs_data)): logging.warning(f"Attempted to delete invalid group index: {group_index}"); return
        group_to_delete = self.inputs_data[group_index]; group_name = group_to_delete["name_var"].get()
        try:
            group_to_delete["frame"].destroy(); self.inputs_data.pop(group_index)
            logging.info(f"Deleted group '{group_name}' at index {group_index}"); self.update_status(f"Deleted group '{group_name}'.")
            self._update_dynamic_commands()
        except Exception as e: logging.exception(f"Error deleting group {group_index}: {e}"); self.update_status(f"Error deleting group '{group_name}'.", "error")

    def _toggle_group(self, group_index):
        if not (0 <= group_index < len(self.inputs_data)): logging.warning(f"Toggle called for invalid group index: {group_index}"); return
        group_data = self.inputs_data[group_index]
        content_frame = group_data["collapsible_content_frame"]; toggle_button = group_data["toggle_button"]
        group_data["is_expanded"] = not group_data["is_expanded"]; is_now_expanded = group_data["is_expanded"]
        if is_now_expanded: content_frame.pack(fill=X, pady=(5, 0)); toggle_button.config(text=COLLAPSE_SYMBOL); logging.debug(f"Expanded group {group_index}")
        else: content_frame.pack_forget(); toggle_button.config(text=EXPAND_SYMBOL); logging.debug(f"Collapsed group {group_index}")

    # --- MODIFIED add_input_row: Added new types to OptionMenu ---
    def add_input_row(self, group_index, mapping_data=None):
        if not (0 <= group_index < len(self.inputs_data)): logging.error(f"Cannot add mapping row: Invalid group index {group_index}"); return
        group_data = self.inputs_data[group_index]; mappings_list = group_data["mappings"]; mapping_index = len(mappings_list)
        row_frame = ttk.Frame(group_data["mappings_frame"]); row_frame.pack(fill=X, pady=1)
        data_type_var = ttk.StringVar(value="Text") # Default remains Text
        row_var = ttk.StringVar(); col_var = ttk.StringVar(); name_var = ttk.StringVar()
        value_label = ttk.Label(row_frame, text="N/A", width=15, anchor=W, relief=SUNKEN, padding=(3,0), bootstyle=SECONDARY)
        check_var = ttk.IntVar(value=1)
        row_data = {
            "frame": row_frame, "group_index": group_index, "mapping_index": mapping_index,
            "data_type": data_type_var, "row": row_var, "col": col_var, "name": name_var, "value_label": value_label,
            "auto_update": check_var, "row_entry": None, "col_entry": None, "delete_button": None
        }

        # --- Add "Browser URL" and "Media File" to options ---
        supported_types = ["Text", "Image", "Browser", "Media"]
        # Set default value explicitly *before* creating OptionMenu if it's not the first item
        if mapping_data: default_type = mapping_data.get("type", "Text")
        else: default_type = "Text"
        data_type_var.set(default_type if default_type in supported_types else "Text") # Ensure default is valid

        data_type_menu = ttk.OptionMenu(row_frame, data_type_var, supported_types[0], *supported_types)
        # --------------------------------------------------------
        data_type_menu.config(width=10) # Increased width slightly for longer names
        data_type_menu.pack(side=LEFT, padx=5)
        name_entry = ttk.Entry(row_frame, textvariable=name_var, width=20); name_entry.pack(side=LEFT, padx=5)
        row_entry = ttk.Entry(row_frame, textvariable=row_var, width=5); row_entry.pack(side=LEFT, padx=5)
        row_entry.bind("<KeyRelease>", lambda event, r=row_data: self.update_value_label(r)); row_data["row_entry"] = row_entry
        col_entry = ttk.Entry(row_frame, textvariable=col_var, width=5); col_entry.pack(side=LEFT, padx=5)
        col_entry.bind("<KeyRelease>", lambda event, r=row_data: self.update_value_label(r)); row_data["col_entry"] = col_entry
        value_label.pack(side=LEFT, padx=5, expand=YES, fill=X)
        check_button = ttk.Checkbutton(row_frame, variable=check_var, bootstyle=(PRIMARY, TOOLBUTTON)); check_button.pack(side=LEFT, padx=(5, 10))
        del_button = ttk.Button(row_frame, text="X", command=lambda gi=group_index, mi=mapping_index: self.delete_input_row(gi, mi), bootstyle=(DANGER, OUTLINE), width=3); del_button.pack(side=RIGHT, padx=5)
        row_data["delete_button"] = del_button
        mappings_list.append(row_data)

        if mapping_data: # Populate uses the already set data_type_var
            # data_type_var already set above
            name_var.set(mapping_data.get("name", "")); row_var.set(str(mapping_data.get("row", ""))); col_var.set(str(mapping_data.get("col", "")))
            check_var.set(int(mapping_data.get("auto_update", 1))); self.update_value_label(row_data)
        else:
            try: value_label.config(style=self._get_style_name(SECONDARY, "TLabel"))
            except TclError as e: logging.error(f"Error setting initial style for new mapping label ({group_index},{mapping_index}): {e}")
            if len(mappings_list) > 1 or len(self.inputs_data) > 1: self.update_status(f"Added new mapping to group '{group_data['name_var'].get()}'.")
        self._update_mapping_delete_commands(group_index)

    def delete_input_row(self, group_index, mapping_index):
        if not (0 <= group_index < len(self.inputs_data)): logging.warning(f"Attempted to delete mapping from invalid group index: {group_index}"); return
        group_data = self.inputs_data[group_index]; mappings_list = group_data["mappings"]
        if not (0 <= mapping_index < len(mappings_list)): logging.warning(f"Attempted to delete invalid mapping index {mapping_index} from group {group_index}"); return
        row_data_to_delete = mappings_list[mapping_index]
        try:
            row_str, col_str = row_data_to_delete["row"].get(), row_data_to_delete["col"].get()
            if row_str.isdigit() and col_str.isdigit(): cell_id = (int(row_str) - 1, int(col_str) - 1); self.previous_values.pop(cell_id, None); logging.debug(f"Cleared previous value for cell {cell_id} on delete.")
        except Exception as e: logging.warning(f"Could not clear previous_value for deleted row ({group_index},{mapping_index}): {e}")
        try:
            row_data_to_delete["frame"].destroy(); mappings_list.pop(mapping_index)
            logging.info(f"Deleted mapping row {mapping_index} from group '{group_data['name_var'].get()}'."); self.update_status(f"Deleted mapping row from group '{group_data['name_var'].get()}'.")
            self._update_mapping_delete_commands(group_index)
        except Exception as e: logging.exception(f"Error deleting mapping row ({group_index},{mapping_index}): {e}"); self.update_status(f"Error deleting mapping row.", "error")

    def _update_dynamic_commands(self):
        logging.debug("Updating dynamic commands for groups.")
        for i, group_data in enumerate(self.inputs_data):
            try:
                if group_data.get("delete_button"): group_data["delete_button"].configure(command=lambda idx=i: self.delete_group(idx))
                if group_data.get("add_mapping_button"): group_data["add_mapping_button"].configure(command=lambda idx=i: self.add_input_row(idx))
                if group_data.get("toggle_button"): group_data["toggle_button"].configure(command=lambda idx=i: self._toggle_group(idx))
                name_entry = group_data["header_frame"].winfo_children()[1]
                if isinstance(name_entry, ttk.Entry):
                    name_entry.bind("<FocusOut>", lambda e, idx=i: self._update_group_name(idx))
                    name_entry.bind("<Return>", lambda e, idx=i: self._update_group_name(idx))
                else: logging.warning(f"Could not find group name entry at expected index 1 for group {i} during command update.")
                self._update_mapping_delete_commands(i)
            except IndexError: logging.error(f"Error finding widget for group {i} during command update (check header_frame children order).")
            except Exception as e: logging.error(f"Error updating dynamic commands for group {i}: {e}")

    def _update_mapping_delete_commands(self, group_index):
        if not (0 <= group_index < len(self.inputs_data)): return
        group_data = self.inputs_data[group_index]
        logging.debug(f"Updating mapping delete commands for group {group_index} ('{group_data['name_var'].get()}')")
        for j, mapping_data in enumerate(group_data["mappings"]):
            try:
                 if mapping_data.get("delete_button"): mapping_data["delete_button"].configure(command=lambda gi=group_index, mi=j: self.delete_input_row(gi, mi))
                 mapping_data["mapping_index"] = j; mapping_data["group_index"] = group_index
                 if mapping_data.get("row_entry"): mapping_data["row_entry"].bind("<KeyRelease>", lambda event, r=mapping_data: self.update_value_label(r))
                 if mapping_data.get("col_entry"): mapping_data["col_entry"].bind("<KeyRelease>", lambda event, r=mapping_data: self.update_value_label(r))
            except Exception as e: logging.error(f"Error updating commands/bindings for mapping ({group_index},{j}): {e}")

    def _check_update_needed(self, row_data): pass

    def _get_style_name(self, bootstyle_constant, base_widget_type="TLabel"):
        style_prefix_map = {PRIMARY: "primary", INFO: "info", SUCCESS: "success", WARNING: "warning", DANGER: "danger", LIGHT: "light", DARK: "dark", SECONDARY: "secondary", DEFAULT: ""}
        prefix = style_prefix_map.get(bootstyle_constant, "")
        return f"{prefix}.{base_widget_type}" if prefix else base_widget_type

    def update_value_label(self, mapping_data):
        row_str, col_str = mapping_data["row"].get().strip(), mapping_data["col"].get().strip()
        file = self.file_path.get(); sheet = self.sheet_name.get()
        label_widget = mapping_data.get("value_label")
        if not label_widget: return
        current_style = SECONDARY; display_text = "N/A"
        if not file or not sheet or not row_str.isdigit() or not col_str.isdigit(): display_text = "N/A"
        elif not os.path.exists(file): display_text = "File?"; current_style = WARNING
        else:
            self._ensure_excel_cache()
            try:
                row_idx, col_idx = int(row_str) - 1, int(col_str) - 1
                current_value = self._get_cell_value_from_cache(row_idx, col_idx)
                if current_value is None: display_text = "?"; current_style = WARNING
                else:
                    value_str = str(current_value)
                    display_text = value_str[:50] + ('...' if len(value_str)>50 else '')
                    cell_id = (row_idx, col_idx)
                    if cell_id in self.previous_values and self.previous_values[cell_id] != current_value: current_style = INFO
                    else: current_style = DEFAULT
            except ValueError: display_text = "Num?"; current_style = WARNING
            except Exception as e: logging.error(f"Error getting/checking value for label update ({mapping_data.get('group_index')},{mapping_data.get('mapping_index')}): {e}"); display_text = "Err"; current_style = DANGER
        try:
            style_name = self._get_style_name(current_style, "TLabel")
            label_widget.config(text=display_text, style=style_name)
        except TclError as e:
            logging.error(f"TclError configuring label ({mapping_data.get('group_index')},{mapping_data.get('mapping_index')}): {e}. Text: '{display_text}', Style: '{style_name}'")
            try: label_widget.config(text=display_text + " (StyleErr!)")
            except Exception as fallback_e: logging.error(f"Error during fallback label text config: {fallback_e}")
        except Exception as e: logging.error(f"Unexpected error configuring label ({mapping_data.get('group_index')},{mapping_data.get('mapping_index')}): {e}")

    def update_all_value_labels(self):
        logging.debug("Updating all value labels across all groups.")
        self._ensure_excel_cache()
        for group_index, group_data in enumerate(self.inputs_data):
            for mapping_index, mapping_data in enumerate(group_data["mappings"]):
                 try: self.update_value_label(mapping_data)
                 except Exception as e:
                     logging.error(f"Error updating label for mapping ({group_index},{mapping_index}): {e}")
                     if mapping_data.get("value_label"):
                         try: err_style = self._get_style_name(DANGER, "TLabel"); mapping_data["value_label"].config(text="Err!", style=err_style)
                         except Exception as fallback_e: logging.error(f"Error setting error label state: {fallback_e}")

    def connect_obs(self):
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

    def _obs_connect_worker(self, host, port, password):
        new_client, connected_successfully, error_message = None, False, ""
        try:
             new_client = obs.ReqClient(host=host, port=port, password=password if password else None, timeout=CONNECTION_TIMEOUT_SECONDS)
             connected_successfully = True
        except obs.ConnectionFailure as e: error_message = f"OBS Connection Failed: {e}"; logging.error(error_message)
        except Exception as e: error_message = f"OBS Connection Error: {e}"; logging.exception("OBS connection error.")
        finally:
            with self.obs_connection_lock: self.obs_client = new_client if connected_successfully else None; self.obs_connected = connected_successfully
            self.root.after(0, self._finalize_connection_attempt, connected_successfully, error_message)

    def _finalize_connection_attempt(self, success, error_msg):
         if success: logging.info("OBS Connected."); self.update_status("OBS Connected.", "success"); self.update_obs_status_label("Connected", SUCCESS)
         else: self.update_status(error_msg, "error"); self.update_obs_status_label("Disconnected", DANGER)
         self._connecting = False
         try: self.connect_button.config(state=NORMAL)
         except: pass

    def update_obs_status_label(self, text, style_constant):
        def _update():
             if not self.running: return
             try: style_name = self._get_style_name(style_constant, "TLabel"); self.obs_status_label.config(text=f"OBS Status: {text}", style=style_name)
             except TclError as e: logging.warning(f"Could not update OBS status label style: {e} (Text: {text}, Style: {style_name})")
             except Exception as e: logging.warning(f"Could not update OBS status label: {e}")
        if hasattr(self.root, 'after') and self.root.winfo_exists(): self.root.after(0, _update)
        else: logging.warning("Cannot schedule OBS status label update (root destroyed or no 'after').")

    # --- MODIFIED send_update_to_obs: Handle new types ---
    def send_update_to_obs(self, data_type, value, source_name):
        """Sends the appropriate setting update to the specified OBS source."""
        with self.obs_connection_lock:
            if not self.obs_connected or not self.obs_client: return False
            if not source_name: logging.warning("Skipping update: OBS Source Name empty."); return False

            try:
                settings = {}
                value_str = str(value).strip() # Convert to string and strip whitespace

                # Determine settings based on type
                if data_type == "Text":
                    settings = {"text": value_str} # Use stripped string for text too
                    logging.info(f"Updating OBS Text '{source_name}' to '{value_str[:50]}{'...' if len(value_str)>50 else ''}'")
                elif data_type == "Image":
                    if not value_str: logging.warning(f"Skipping OBS Image '{source_name}': Empty file path."); return False
                    abs_path = os.path.abspath(value_str)
                    settings = {"file": abs_path}
                    logging.info(f"Updating OBS Image '{source_name}' to '{abs_path}'")
                # --- NEW TYPES ---
                elif data_type == "Browser URL":
                    # Basic check: allow empty URL to potentially clear/reset it in OBS? Or skip? Let's allow it.
                    # if not value_str: logging.warning(f"Skipping OBS Browser URL '{source_name}': Empty URL."); return False
                    settings = {"url": value_str}
                    logging.info(f"Updating OBS Browser URL '{source_name}' to '{value_str}'")
                elif data_type == "Media File":
                    if not value_str: logging.warning(f"Skipping OBS Media File '{source_name}': Empty file path."); return False
                    abs_path = os.path.abspath(value_str)
                    settings = {"local_file": abs_path} # Key is 'local_file' for media sources
                    logging.info(f"Updating OBS Media File '{source_name}' to '{abs_path}'")
                # --- END NEW TYPES ---
                else:
                    logging.warning(f"Unknown data type '{data_type}' for source '{source_name}'"); return False

                # Send the request
                resp = self.obs_client.set_input_settings(name=source_name, settings=settings, overlay=True)
                logging.debug(f"OBS response for '{source_name}' ({data_type}): {resp}")
                return True

            except obs.ConnectionFailure:
                logging.error("OBS Connection Lost during update."); self.update_status("OBS Connection Lost.", "error"); self.obs_connected = False
                self.root.after(0, self.update_obs_status_label, "Disconnected", DANGER); return False
            except OBSSDKRequestError as e:
                # More specific error logging
                err_details = str(e)
                if "NotFound" in err_details:
                     log_msg = f"OBS Error: Source '{source_name}' not found."
                     status_level = "warning" # Treat not found as warning? Or error?
                elif "InvalidData" in err_details: # Example, might be different
                     log_msg = f"OBS Error: Invalid data/setting for '{source_name}' (Type: {data_type}). Check input."
                     status_level = "error"
                else:
                     log_msg = f"Failed OBS update '{source_name}': {e}"
                     status_level = "error"
                logging.error(log_msg)
                self.update_status(log_msg, status_level)
                return False
            except FileNotFoundError: # Catch local file not found before sending absolute path
                 # This applies to Image and Media File after abspath
                 logging.error(f"{data_type} file not found for OBS update '{source_name}': {value_str}");
                 self.update_status(f"Error: {data_type} file not found for '{source_name}'", "error");
                 return False
            except Exception as e:
                logging.exception(f"Unexpected OBS update error '{source_name}' ({data_type}): {e}")
                self.update_status(f"Unexpected OBS Error for '{source_name}': {e}", "error");
                return False
    # --- Data Handling (Excel Cache - Unchanged) ---
    def _ensure_excel_cache(self, force_read=False):
        file = self.file_path.get(); sheet = self.sheet_name.get()
        if not file or not sheet:
            if self.cached_df is not None: logging.info("Clearing Excel cache due to missing file/sheet path."); self.cached_df = None; self.last_excel_mtime = None
            return False
        if not os.path.exists(file):
            if self.cached_df is not None: logging.warning(f"Excel file not found: {file}. Clearing cache."); self.cached_df = None; self.last_excel_mtime = None
            return False
        try: current_mtime = os.path.getmtime(file)
        except OSError as e: logging.error(f"Cannot get modification time for {file}: {e}"); self.cached_df = None; self.last_excel_mtime = None; return False
        with self.excel_read_lock:
            if force_read or self.cached_df is None or current_mtime != self.last_excel_mtime:
                logging.debug(f"Reading Excel file '{os.path.basename(file)}' sheet '{sheet}'. Reason: {'Forced' if force_read else 'Cache miss or file changed'}")
                try:
                    start_time = time.time(); self.cached_df = pd.read_excel(file, sheet_name=sheet, engine='openpyxl', header=None, index_col=None)
                    read_time = time.time() - start_time; self.last_excel_mtime = current_mtime
                    logging.info(f"Excel cache updated in {read_time:.3f}s. Shape: {self.cached_df.shape}")
                    return True
                except Exception as e:
                    self.cached_df = None; self.last_excel_mtime = None
                    log_msg = f"Error reading Excel: {e}";
                    if "No sheet named" in str(e): log_msg = f"Error: Sheet '{sheet}' not found."
                    logging.error(log_msg + f" (File: {file})")
                    return False
            else: return True

    def _get_cell_value_from_cache(self, row, col):
        value = None; error = False
        with self.excel_read_lock:
            if self.cached_df is not None:
                try:
                    if 0 <= row < self.cached_df.shape[0] and 0 <= col < self.cached_df.shape[1]:
                        val = self.cached_df.iloc[row, col]
                        if pd.isna(val): value = ""
                        elif isinstance(val, (float, int)) and float(val).is_integer(): value = int(val)
                        else: value = val
                    else: logging.debug(f"_get_cell_value: Cell [{row+1},{col+1}] out of range ({self.cached_df.shape})."); error = True
                except Exception as e: logging.error(f"Error accessing cached DataFrame cell [{row+1},{col+1}]: {e}"); error = True
            else: error = True
        return None if error else value

    def update_obs_data(self, check_changes=False):
        # (Unchanged - calls updated send_update_to_obs)
        cache_valid = self._ensure_excel_cache(force_read=not check_changes)
        current_df = None
        with self.excel_read_lock:
            if cache_valid and self.cached_df is not None: current_df = self.cached_df
        if current_df is None:
            if not check_changes: self.update_status("Cannot update OBS: Failed to read or cache Excel file.", "error")
            logging.warning("update_obs_data skipped: No valid Excel data available.")
            for group_data in self.inputs_data:
                for mapping_data in group_data["mappings"]:
                    if mapping_data.get("value_label"):
                        try: warn_style = self._get_style_name(WARNING); mapping_data["value_label"].config(text="Read?", style=warn_style)
                        except Exception as e: logging.error(f"Error setting 'Read?' label: {e}")
            return
        updates_sent, updates_attempted, mappings_processed = 0, 0, 0
        df_rows, df_cols = current_df.shape
        for group_index, group_data in enumerate(self.inputs_data):
            group_name = group_data["name_var"].get()
            if not group_data.get("is_expanded", True): mappings_processed += len(group_data["mappings"]); continue # Skip collapsed
            for mapping_index, mapping_data in enumerate(group_data["mappings"]):
                mappings_processed += 1
                row_str, col_str = mapping_data["row"].get().strip(), mapping_data["col"].get().strip()
                source_name = mapping_data["name"].get().strip(); data_type = mapping_data["data_type"].get()
                is_auto_update = mapping_data["auto_update"].get() == 1
                label_widget = mapping_data.get("value_label")
                if not label_widget: continue
                label_text = "N/A"; label_style_constant = SECONDARY
                if check_changes and not is_auto_update: continue
                if not row_str.isdigit() or not col_str.isdigit():
                    if is_auto_update or not check_changes: logging.warning(f"Skipping Group '{group_name}' Mapping {mapping_index+1}: Invalid row/col '{row_str}'/'{col_str}'.")
                    label_text, label_style_constant = "Num?", WARNING
                    try: label_widget.config(text=label_text, style=self._get_style_name(label_style_constant))
                    except TclError as e: logging.error(f"TclError configuring label for Num? ({group_index},{mapping_index}): {e}")
                    continue
                row, col = int(row_str) - 1, int(col_str) - 1; cell_id = (row, col)
                if not (0 <= row < df_rows and 0 <= col < df_cols):
                    if is_auto_update or not check_changes: logging.warning(f"Skipping Group '{group_name}' Mapping {mapping_index+1}: Cell [{row+1},{col+1}] out of range {current_df.shape}.")
                    label_text, label_style_constant = "Range?", WARNING
                    try: label_widget.config(text=label_text, style=self._get_style_name(label_style_constant))
                    except TclError as e: logging.error(f"TclError configuring label for Range? ({group_index},{mapping_index}): {e}")
                    continue
                try:
                    value = self._get_cell_value_from_cache(row, col)
                    if value is None:
                        label_text, label_style_constant = "Read?", WARNING
                        try: label_widget.config(text=label_text, style=self._get_style_name(label_style_constant))
                        except TclError as e: logging.error(f"TclError configuring label for Read? ({group_index},{mapping_index}): {e}")
                        continue
                    value_str_display = str(value); label_text = value_str_display[:50] + ('...' if len(value_str_display) > 50 else '')
                    should_update_obs = False; _sentinel = object(); previous_value = self.previous_values.get(cell_id, _sentinel)
                    changed = (previous_value is not _sentinel and previous_value != value)
                    label_style_constant = INFO if changed else DEFAULT
                    try: label_widget.config(text=label_text, style=self._get_style_name(label_style_constant))
                    except TclError as e:
                        logging.error(f"TclError configuring label before OBS update ({group_index},{mapping_index}): {e}")
                        try: label_widget.config(text=label_text + " (StyleErr!)")
                        except: pass
                    if not source_name:
                        if changed: logging.debug(f"Change detected (no source): Group '{group_name}' Cell [{row+1},{col+1}]")
                    elif check_changes:
                        if previous_value is _sentinel or changed:
                            if changed: logging.info(f"Change detected: Group '{group_name}' Source '{source_name}' Cell [{row+1},{col+1}]")
                            should_update_obs = True
                    else: should_update_obs = True
                    if should_update_obs and source_name:
                        updates_attempted += 1
                        if self.send_update_to_obs(data_type, value, source_name):
                            updates_sent += 1; self.previous_values[cell_id] = value
                        else:
                            fail_style = WARNING if changed else DANGER
                            try: label_widget.config(style=self._get_style_name(fail_style))
                            except TclError as e:
                                logging.error(f"TclError configuring label on OBS fail ({group_index},{mapping_index}): {e}")
                                try: label_widget.config(text=label_text + " (SendFail!)")
                                except: pass
                    if previous_value is _sentinel: self.previous_values[cell_id] = value
                except Exception as cell_error:
                    logging.error(f"Error processing Group '{group_name}' Mapping {mapping_index+1} Cell [{row+1},{col+1}] Source '{source_name}': {cell_error}")
                    try: label_widget.config(text="Error", style=self._get_style_name(DANGER))
                    except TclError as e:
                         logging.error(f"TclError configuring label on cell processing error ({group_index},{mapping_index}): {e}")
                         try: label_widget.config(text="Error (StyleErr!)")
                         except: pass
        if not check_changes: status = f"Manual update: Processed {mappings_processed}, Attempted {updates_attempted}, Successful {updates_sent}."; log_level = "success" if updates_sent > 0 else ("warning" if updates_attempted > 0 else "info"); self.update_status(status, log_level)
        elif updates_sent > 0: logging.info(f"Auto-update: Sent {updates_sent} changes.")

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
                    auto_update_enabled = False
                    try:
                        current_groups = list(self.inputs_data)
                        for group_data in current_groups:
                            if group_data.get("is_expanded", True) and any(m["auto_update"].get() == 1 for m in group_data["mappings"]):
                                auto_update_enabled = True; break
                    except Exception as e: logging.error(f"Error checking auto-update status in loop: {e}"); auto_update_enabled = False
                    if auto_update_enabled: self.update_obs_data(check_changes=True)
                elapsed = time.time() - start_cycle; sleep_time = max(0, UPDATE_INTERVAL_SECONDS - elapsed)
                sleep_chunk = 0.1
                while self.running and sleep_time > 0: actual_sleep = min(sleep_chunk, sleep_time); time.sleep(actual_sleep); sleep_time -= actual_sleep
            except Exception as e: logging.exception(f"Error in periodic update loop: {e}"); time.sleep(5)
        logging.info("Periodic update loop stopped.")

    def export_settings(self):
        logging.info("Exporting settings...")
        settings_data = {"obs_settings": {"host": self.obs_host_var.get(), "port": self.obs_port_var.get(), "password": self.obs_password_var.get()},"excel_settings": {"file_path": self.file_path.get(), "sheet_name": self.sheet_name.get()},"mapping_groups": [] }
        for group_index, group_data in enumerate(self.inputs_data):
            try:
                group_export = {"group_name": group_data["name_var"].get(), "mappings": []}
                for mapping_index, mapping_data in enumerate(group_data["mappings"]):
                     try: group_export["mappings"].append({"type": mapping_data["data_type"].get(), "name": mapping_data["name"].get(), "row": mapping_data["row"].get(), "col": mapping_data["col"].get(), "auto_update": mapping_data["auto_update"].get()})
                     except Exception as map_e: logging.error(f"Error exporting mapping ({group_index},{mapping_index}): {map_e}")
                settings_data["mapping_groups"].append(group_export)
            except Exception as group_e: logging.error(f"Error exporting group {group_index}: {group_e}")
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
            obs_cfg = settings_data.get("obs_settings", {}); excel_cfg = settings_data.get("excel_settings", {})
            self.obs_host_var.set(obs_cfg.get("host", DEFAULT_OBS_WS_HOST)); self.obs_port_var.set(str(obs_cfg.get("port", DEFAULT_OBS_WS_PORT))); self.obs_password_var.set(obs_cfg.get("password", DEFAULT_OBS_WS_PASSWORD))
            self.file_path.set(excel_cfg.get("file_path", "")); self.sheet_name.set(excel_cfg.get("sheet_name", ""))
            with self.excel_read_lock: self.last_excel_mtime = None; self.cached_df = None
            self.previous_values.clear()
            logging.debug("Clearing existing groups UI and data...")
            widgets_to_destroy = list(self.groups_container_frame.winfo_children())
            for widget in widgets_to_destroy:
                try: widget.destroy()
                except Exception as destroy_e: logging.warning(f"Error destroying group widget during import clear: {destroy_e}")
            self.inputs_data.clear(); logging.debug("Existing groups cleared.")
            if "mappings" in settings_data and "mapping_groups" not in settings_data:
                 logging.warning("Importing legacy settings format. Creating a single default group.")
                 imported_groups = [{"group_name": DEFAULT_GROUP_NAME, "mappings": settings_data.get("mappings", [])}]
            else: imported_groups = settings_data.get("mapping_groups", [])
            if not isinstance(imported_groups, list): raise ValueError("'mapping_groups' must be a list.")
            logging.debug(f"Importing {len(imported_groups)} groups.")
            if not imported_groups: self.add_group(group_name=DEFAULT_GROUP_NAME)
            else:
                for group_import_data in imported_groups: self.add_group(group_data=group_import_data)
            logging.info(f"Settings imported from {file_path}"); self.update_status(f"Settings imported from {os.path.basename(file_path)}", "success")
            self.update_all_value_labels(); self.update_status("Settings imported. Reconnect to OBS if needed.", "info")
        except FileNotFoundError: logging.error(f"Import failed: File not found: {file_path}"); self.update_status("Error importing: File not found.", "error")
        except json.JSONDecodeError as e: logging.error(f"Import failed: Invalid JSON: {e}"); self.update_status(f"Error importing: Invalid JSON file.", "error")
        except Exception as e: logging.exception("Failed import settings."); self.update_status(f"Error importing settings: {e}", "error")

    def stop(self):
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
        try:
             if self.root and self.root.winfo_exists(): self.root.destroy()
             else: logging.info("Root window already destroyed or doesn't exist.")
        except Exception as e: logging.error(f"Error destroying root window: {e}")

if __name__ == "__main__":
    root = ttk.Window(themename=DEFAULT_THEME, minsize=(710, 550))
    app = ExcelToOBS(root)
    try: root.mainloop()
    except KeyboardInterrupt: logging.info("KeyboardInterrupt received. Stopping..."); app.stop()
    except TclError as e: logging.exception(f"TclError in main loop: {e}"); app.stop()
    except Exception as e: logging.exception("Unhandled exception in main loop."); app.stop()