#!/usr/bin/env python3
"""
Consolidated GRN Scheduler - Excel Version
Workflow: Gmail → Drive → Excel Processing → Google Sheets
- Downloads Consolidated GRN Excel attachments from Gmail to Google Drive
- Processes the consolidated Excel file (one file, multiple POs)
- GRN number comes from the po_number column inside the file
- Pushes data to Google Sheets
"""

import os
import io
import base64
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import pandas as pd

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('grn_scheduler.log'),
        logging.StreamHandler()
    ]
)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  ← Update these values before running
# ─────────────────────────────────────────────────────────────────────────────
CONFIG = {
    'mail': {
        'gdrive_folder_id': '1vj4fxIyuEmfPp7uDPJ1ChRYtPGjEt6DZ',   # Drive folder to store downloaded GRN files
        'sender': '',                                        # e.g. 'grn@handsontrades.com'  (leave '' for any sender)
        'search_term': 'Consolidated GRN',                  # Subject/body keyword to match
        'attachment_filter': 'Consolidated-GRN-Report',     # Only download files matching this name pattern
        'days_back': 25,
        'max_results': 500
    },
    'sheet': {
        'drive_folder_id': '1vj4fxIyuEmfPp7uDPJ1ChRYtPGjEt6DZ',    # Same as above (where files land)
        'spreadsheet_id': '1urhhzYZy_-0l_KAW0_TkPBj7UThoFyQfmiKd4F3cRZk',       # Target Google Sheet
        'sheet_range': 'GRN',                               # Tab name inside the sheet
        'days_back': 25,
        'max_files': 500
    },
    # ── Excel column mapping ─────────────────────────────────────────────────
    # Key  = internal name used in this script
    # Value = actual column header in the Consolidated GRN Excel file
    'excel_mapping': {
        'item_code':          'Item Code',
        'po_number':          'po_number',          # PO/GRN reference column
        'product_upc':        'Product UPC',
        'product_description':'Product Description',
        'mrp':                'MRP',
        'tax_amount':         'Tax Amount',
        'landing_rate_po':    'Landing Rate - PO',
        'landing_rate_grn':   'Landing Rate - GRN',
        'quantity_po':        'Quantity - PO',
        'quantity_grn':       'Quantity - GRN',
        'fill_rate':          'Fill rate (%)',
        'total_grn_amount':   'Total GRN Amount',
        'gmv_loss':           'GMV Loss',
    },
    # ── Output column order in Google Sheets ────────────────────────────────
    'output_columns': {
        'grn_number':          'GRN Number',
        'received_date':       'Received Date',
        'item_code':           'Item Code',
        'product_upc':         'Product UPC',
        'product_description': 'Product Description',
        'mrp':                 'MRP',
        'tax_amount':          'Tax Amount',
        'landing_rate_po':     'Landing Rate - PO',
        'landing_rate_grn':    'Landing Rate - GRN',
        'quantity_po':         'Qty - PO',
        'quantity_grn':        'Qty - GRN',
        'fill_rate':           'Fill Rate (%)',
        'total_grn_amount':    'Total GRN Amount',
        'gmv_loss':            'GMV Loss',
        'source_file':         'source_file',
    },
    'workflow_log': {
        'spreadsheet_id': '1zebjRyYd2R1d2f9iOeLJFWvinVn7_rJ5OwF1wVicZKM',
        'sheet_range': 'GRN_workflow_log'
    },
    'credentials_path': 'credentials.json',
    'token_path': 'token.json'
}


class GRNAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None

        self.gmail_scopes  = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes  = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']

    # ── Utilities ─────────────────────────────────────────────────────────────

    def log(self, message: str, level: str = "INFO"):
        if level.upper() == "ERROR":
            logging.error(message)
        elif level.upper() == "WARNING":
            logging.warning(message)
        else:
            logging.info(message)

    # ── Authentication ────────────────────────────────────────────────────────

    def authenticate(self) -> bool:
        try:
            self.log("Starting authentication process...")
            creds = None
            combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))

            if os.path.exists(CONFIG['token_path']):
                creds = Credentials.from_authorized_user_file(CONFIG['token_path'], combined_scopes)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    self.log("Refreshing expired token...")
                    creds.refresh(Request())
                else:
                    if not os.path.exists(CONFIG['credentials_path']):
                        self.log(f"Credentials file not found: {CONFIG['credentials_path']}", "ERROR")
                        return False
                    self.log("Starting new OAuth flow...")
                    flow = InstalledAppFlow.from_client_secrets_file(
                        CONFIG['credentials_path'], combined_scopes)
                    creds = flow.run_local_server(port=0)

                with open(CONFIG['token_path'], 'w') as token:
                    token.write(creds.to_json())
                self.log("Token saved successfully")

            self.gmail_service  = build('gmail',  'v1', credentials=creds)
            self.drive_service  = build('drive',  'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)

            self.log("Authentication successful!")
            return True

        except Exception as e:
            self.log(f"Authentication failed: {str(e)}", "ERROR")
            return False

    # ── Gmail helpers ─────────────────────────────────────────────────────────

    def search_emails(self, sender: str = "", search_term: str = "",
                      days_back: int = 7, max_results: int = 50) -> List[Dict]:
        try:
            query_parts = ["has:attachment"]
            if sender:
                query_parts.append(f'from:"{sender}"')
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    kw_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if kw_query:
                        query_parts.append(f"({kw_query})")
                else:
                    query_parts.append(f'"{search_term}"')

            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            query = " ".join(query_parts)
            self.log(f"[GMAIL] Searching with query: {query}")

            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()

            messages = result.get('messages', [])
            self.log(f"[GMAIL] Found {len(messages)} emails matching criteria")
            return messages

        except Exception as e:
            self.log(f"[ERROR] Email search failed: {str(e)}", "ERROR")
            return []

    def get_email_details(self, message_id: str) -> Dict:
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            headers = message['payload'].get('headers', [])
            return {
                'id': message_id,
                'sender':  next((h['value'] for h in headers if h['name'] == "From"),    "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date':    next((h['value'] for h in headers if h['name'] == "Date"),    "")
            }
        except Exception as e:
            self.log(f"[ERROR] Failed to get email details: {str(e)}", "ERROR")
            return {}

    def get_attachments(self, message_id: str, attachment_filter: str = "") -> List[Dict]:
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='full'
            ).execute()
            attachments = []

            def process_parts(parts):
                for part in parts:
                    if part.get('filename'):
                        filename = part['filename']
                        if attachment_filter and attachment_filter.lower() not in filename.lower():
                            continue
                        attachment_id = part['body'].get('attachmentId')
                        if attachment_id:
                            attachments.append({
                                'id': attachment_id,
                                'filename': filename,
                                'mimeType': part.get('mimeType', 'application/octet-stream')
                            })
                    if 'parts' in part:
                        process_parts(part['parts'])

            if 'parts' in message['payload']:
                process_parts(message['payload']['parts'])
            return attachments

        except Exception as e:
            self.log(f"[ERROR] Failed to get attachments: {str(e)}", "ERROR")
            return []

    def download_attachment(self, message_id: str, attachment_id: str) -> Optional[bytes]:
        try:
            attachment = self.gmail_service.users().messages().attachments().get(
                userId='me', messageId=message_id, id=attachment_id
            ).execute()
            data = attachment['data']
            return base64.urlsafe_b64decode(data.encode('UTF-8'))
        except Exception as e:
            self.log(f"[ERROR] Failed to download attachment: {str(e)}", "ERROR")
            return None

    # ── Drive helpers ─────────────────────────────────────────────────────────

    def upload_to_drive(self, file_data: bytes, filename: str,
                        folder_id: str, mime_type: str) -> Optional[str]:
        try:
            file_metadata = {'name': filename, 'parents': [folder_id]}
            media = MediaIoBaseUpload(
                io.BytesIO(file_data), mimetype=mime_type, resumable=True)
            file = self.drive_service.files().create(
                body=file_metadata, media_body=media,
                fields='id, name, webViewLink'
            ).execute()
            return file.get('id')
        except Exception as e:
            self.log(f"[ERROR] Failed to upload to Drive: {str(e)}", "ERROR")
            return None

    def file_exists_in_drive(self, filename: str, folder_id: str) -> bool:
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(
                q=query, spaces='drive', fields='files(id, name)'
            ).execute()
            return len(results.get('files', [])) > 0
        except Exception as e:
            self.log(f"[ERROR] Failed to check file existence: {str(e)}", "ERROR")
            return False

    def list_excel_files(self, folder_id: str, days_back: int = 7) -> List[Dict]:
        try:
            start_date = datetime.now() - timedelta(days=days_back)
            start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
            mime_types = [
                "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
                "mimeType='application/vnd.ms-excel'"
            ]
            mime_query = " or ".join(mime_types)
            query = (f"'{folder_id}' in parents and ({mime_query}) "
                     f"and trashed=false and modifiedTime >= '{start_date_str}'")
            results = self.drive_service.files().list(
                q=query, spaces='drive',
                fields='files(id, name, createdTime, modifiedTime, mimeType)',
                orderBy='createdTime desc'
            ).execute()
            files = results.get('files', [])
            self.log(f"[DRIVE] Found {len(files)} Excel files in Drive folder")
            return files
        except Exception as e:
            self.log(f"[ERROR] Failed to list Drive files: {str(e)}", "ERROR")
            return []

    def download_excel_file(self, file_id: str) -> Optional[bytes]:
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_data = io.BytesIO()
            downloader = MediaIoBaseDownload(file_data, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            return file_data.getvalue()
        except Exception as e:
            self.log(f"[ERROR] Failed to download file: {str(e)}", "ERROR")
            return None

    # ── Sheets helpers ────────────────────────────────────────────────────────

    def get_existing_source_files(self, spreadsheet_id: str, sheet_range: str) -> set:
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            if not values:
                return set()
            headers = values[0]
            if "source_file" not in headers:
                self.log("No 'source_file' column found", "WARNING")
                return set()
            idx = headers.index("source_file")
            existing = {row[idx] for row in values[1:] if len(row) > idx and row[idx]}
            self.log(f"[SHEET] Found {len(existing)} already processed files")
            return existing
        except Exception as e:
            self.log(f"[ERROR] Failed to get existing files: {str(e)}", "ERROR")
            return set()

    def append_to_sheet(self, spreadsheet_id: str, sheet_range: str,
                        values: List[List]) -> bool:
        try:
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body={'values': values}
            ).execute()
            return True
        except Exception as e:
            self.log(f"[ERROR] Failed to append to sheet: {str(e)}", "ERROR")
            return False

    def setup_headers(self, spreadsheet_id: str, sheet_range: str,
                      headers: List[str]) -> bool:
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_range}!A1:Z1"
            ).execute()
            existing_headers = result.get('values', [[]])[0] if result.get('values') else []

            if not existing_headers:
                self.log("[SHEET] Creating headers...")
                return self.append_to_sheet(spreadsheet_id, sheet_range, [headers])
            elif existing_headers != headers:
                self.log("[SHEET] Updating headers...")
                sheet_name = sheet_range.split('!')[0]
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption='USER_ENTERED',
                    body={'values': [headers]}
                ).execute()
                return True
            else:
                self.log("[SHEET] Headers already exist and match")
                return True
        except Exception as e:
            self.log(f"[ERROR] Failed to setup headers: {str(e)}", "ERROR")
            return False

    # ── Core GRN Excel processing ─────────────────────────────────────────────

    def extract_date_from_filename(self, filename: str) -> str:
        """
        Extract received date from the timestamped filename we create during upload.
        Format: Consolidated-GRN-Report_YYYYMMDD_HHMMSS.xlsx
        Falls back to scanning for any YYYYMMDD pattern, then today's date.
        """
        name_no_ext = re.sub(r'\.(xlsx?|xls)$', '', filename, flags=re.IGNORECASE)

        # Primary: match our own _YYYYMMDD_HHMMSS suffix
        m = re.search(r'_(\d{4})(\d{2})(\d{2})_\d{6}$', name_no_ext)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        # Fallback: any YYYYMMDD anywhere in name
        m = re.search(r'(\d{4})(\d{2})(\d{2})', name_no_ext)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

        received_date = datetime.now().strftime('%Y-%m-%d')
        self.log(f"[WARNING] Could not extract date from '{filename}', using today: {received_date}")
        return received_date

    def process_grn_excel_file(self, file_data: bytes, file_info: Dict,
                                excel_mapping: Dict) -> List[Dict]:
        """
        Process the Consolidated GRN Excel file.

        Key differences from PO processing:
        - The file may contain rows for MULTIPLE GRN/PO numbers (po_number column).
        - Totals (total items, total qty, total amount) are calculated PER GRN number.
        - The received date is inferred from the filename; per-GRN dates are used if available.
        """
        rows = []
        try:
            received_date = self.extract_date_from_filename(file_info['name'])

            # ── Read file ───────────────────────────────────────────────────
            # dtype=str forces ALL columns to plain Python strings at read time.
            # This is why the PO script never hit int64 issues — its values came
            # from the filename as strings, never raw from pandas cells.
            # This permanently prevents "Object of type int64 is not JSON serializable".
            df = pd.read_excel(io.BytesIO(file_data), engine='openpyxl', dtype=str)
            df = df.dropna(how='all')
            df = df.replace({'nan': '', 'NaN': '', 'None': ''})

            self.log(f"[EXCEL] Read {len(df)} rows from {file_info['name']}")
            self.log(f"[EXCEL] Columns found: {list(df.columns)}")

            # ── Validate required columns ────────────────────────────────────
            required_fields = ['item_code', 'product_description', 'po_number']
            missing = [excel_mapping[f] for f in required_fields
                       if excel_mapping.get(f) and excel_mapping[f] not in df.columns]
            if missing:
                self.log(f"[ERROR] Missing required columns: {missing}", "ERROR")
                return rows

            # ── Drop rows without an item code ───────────────────────────────
            item_col = excel_mapping['item_code']
            df = df[df[item_col].notna()]
            df = df[df[item_col].astype(str).str.strip().isin(['', 'nan']) == False]

            if df.empty:
                self.log(f"[WARNING] No valid line items found in {file_info['name']}")
                return rows

            # ── Calculate per-GRN totals ──────────────────────────────────────
            grn_col = excel_mapping['po_number']

            self.log(f"[GRN] Found {len(df[grn_col].unique())} unique GRN numbers")

            # ── Build output rows ─────────────────────────────────────────────
            # All df values are already plain Python strings (dtype=str at read time).
            # Just use .get() directly — no type conversion needed.
            for _, row in df.iterrows():
                grn_number = str(row.get(grn_col, '')).strip()

                row_dict = {
                    'grn_number':          grn_number,
                    'received_date':       received_date,
                    'item_code':           str(row.get(excel_mapping.get('item_code', ''),           '')).strip(),
                    'product_upc':         str(row.get(excel_mapping.get('product_upc', ''),         '')).strip(),
                    'product_description': str(row.get(excel_mapping.get('product_description', ''), '')).strip(),
                    'mrp':                 str(row.get(excel_mapping.get('mrp', ''),                 '')).strip(),
                    'tax_amount':          str(row.get(excel_mapping.get('tax_amount', ''),          '')).strip(),
                    'landing_rate_po':     str(row.get(excel_mapping.get('landing_rate_po', ''),     '')).strip(),
                    'landing_rate_grn':    str(row.get(excel_mapping.get('landing_rate_grn', ''),    '')).strip(),
                    'quantity_po':         str(row.get(excel_mapping.get('quantity_po', ''),         '')).strip(),
                    'quantity_grn':        str(row.get(excel_mapping.get('quantity_grn', ''),        '')).strip(),
                    'fill_rate':           str(row.get(excel_mapping.get('fill_rate', ''),           '')).strip(),
                    'total_grn_amount':    str(row.get(excel_mapping.get('total_grn_amount', ''),    '')).strip(),
                    'gmv_loss':            str(row.get(excel_mapping.get('gmv_loss', ''),            '')).strip(),

                    'source_file': file_info.get('name', '')
                }
                rows.append(row_dict)

            self.log(f"[SUCCESS] Extracted {len(rows)} rows from {file_info['name']}")
            return rows

        except Exception as e:
            self.log(f"[ERROR] Failed to process GRN Excel file: {str(e)}", "ERROR")
            import traceback
            traceback.print_exc()
            return rows

    # ── Workflow 1: Mail → Drive ───────────────────────────────────────────────

    def process_mail_to_drive_workflow(self, config: dict) -> dict:
        stats = {'processed': 0, 'failed': 0, 'skipped': 0, 'total_attachments': 0}
        try:
            self.log("=" * 80)
            self.log("Starting Mail to Drive workflow (GRN Excel files)")
            self.log("=" * 80)

            messages = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )

            if not messages:
                self.log("No emails found matching criteria", "WARNING")
                return stats

            for message in messages:
                try:
                    message_id = message['id']
                    email_details = self.get_email_details(message_id)
                    self.log(f"\n[EMAIL] Processing: {email_details.get('subject', 'Unknown')}")

                    attachments = self.get_attachments(message_id, config['attachment_filter'])
                    if not attachments:
                        self.log("[SKIP] No Excel attachments found in email")
                        stats['skipped'] += 1
                        continue

                    # Parse email date once for all attachments in this email
                    email_date_str = email_details.get('date', '')
                    try:
                        from email.utils import parsedate_to_datetime
                        email_dt = parsedate_to_datetime(email_date_str)
                        timestamp = email_dt.strftime('%Y%m%d_%H%M%S')
                    except Exception:
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

                    for attachment in attachments:
                        try:
                            original_filename = attachment['filename']

                            # Rename file: insert timestamp before extension
                            # e.g. Consolidated-GRN-Report.xlsx → Consolidated-GRN-Report_20260210_143022.xlsx
                            name_no_ext, ext = os.path.splitext(original_filename)
                            filename = f"{name_no_ext}_{timestamp}{ext}"

                            if self.file_exists_in_drive(filename, config['gdrive_folder_id']):
                                self.log(f"[SKIP] File already exists: {filename}")
                                stats['skipped'] += 1
                                continue

                            file_data = self.download_attachment(message_id, attachment['id'])
                            if not file_data:
                                self.log(f"[ERROR] Failed to download: {filename}")
                                stats['failed'] += 1
                                continue

                            file_id = self.upload_to_drive(
                                file_data, filename,
                                config['gdrive_folder_id'], attachment['mimeType']
                            )
                            if file_id:
                                self.log(f"[SUCCESS] Uploaded to Drive as: {filename} (original: {original_filename})")
                                stats['processed'] += 1
                                stats['total_attachments'] += 1
                            else:
                                self.log(f"[ERROR] Failed to upload: {filename}")
                                stats['failed'] += 1

                        except Exception as e:
                            self.log(f"[ERROR] Processing attachment {attachment.get('filename')}: {str(e)}", "ERROR")
                            stats['failed'] += 1

                except Exception as e:
                    self.log(f"[ERROR] Processing email: {str(e)}", "ERROR")
                    stats['failed'] += 1

            self.log("\n" + "=" * 80)
            self.log(f"Mail to Drive complete. Processed: {stats['processed']}, "
                     f"Failed: {stats['failed']}, Skipped: {stats['skipped']}")
            self.log("=" * 80)
            return stats

        except Exception as e:
            self.log(f"[ERROR] Mail to Drive workflow failed: {str(e)}", "ERROR")
            return stats

    # ── Workflow 2: Drive → Sheets ─────────────────────────────────────────────

    def process_drive_to_sheet_workflow(self, config: dict, skip_existing: bool = True) -> dict:
        stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'rows_added': 0
        }
        try:
            self.log("=" * 80)
            self.log("Starting Drive to Sheets workflow (GRN)")
            self.log("=" * 80)

            existing_files = set()
            if skip_existing:
                existing_files = self.get_existing_source_files(
                    config['spreadsheet_id'], config['sheet_range'])

            excel_files = self.list_excel_files(
                config['drive_folder_id'], config.get('days_back', 7))

            # Only process Consolidated GRN files — ignore zip, debit notes, etc.
            excel_files = [f for f in excel_files if 'Consolidated-GRN-Report' in f['name']]
            self.log(f"[FILTER] {len(excel_files)} Consolidated-GRN-Report file(s) after filtering")

            stats['total_files'] = len(excel_files)

            if skip_existing:
                original_count = len(excel_files)
                excel_files = [f for f in excel_files if f['name'] not in existing_files]
                stats['skipped_files'] = original_count - len(excel_files)
                self.log(f"[SKIP] Skipped {stats['skipped_files']} already processed files")

            max_files = config.get('max_files')
            if max_files and len(excel_files) > max_files:
                excel_files = excel_files[:max_files]
                self.log(f"[LIMIT] Limited to {max_files} files")

            if not excel_files:
                self.log("[INFO] No new GRN Excel files to process")
                return stats

            # Setup headers
            output_columns  = list(CONFIG['output_columns'].keys())
            display_headers = [CONFIG['output_columns'][col] for col in output_columns]

            if not self.setup_headers(config['spreadsheet_id'], config['sheet_range'], display_headers):
                self.log("[ERROR] Failed to setup headers", "ERROR")
                return stats

            # Process each file
            for excel_file in excel_files:
                try:
                    self.log(f"\n[PROCESSING] {excel_file['name']}")

                    file_data = self.download_excel_file(excel_file['id'])
                    if not file_data:
                        self.log(f"[ERROR] Failed to download {excel_file['name']}")
                        stats['failed_files'] += 1
                        continue

                    rows_data = self.process_grn_excel_file(
                        file_data, excel_file, CONFIG['excel_mapping'])

                    if not rows_data:
                        self.log(f"[SKIP] No data found in {excel_file['name']}")
                        stats['failed_files'] += 1
                        continue

                    sheet_rows = [
                        [row_dict.get(col, "") for col in output_columns]
                        for row_dict in rows_data
                    ]

                    self.log(f"[APPEND] Appending {len(sheet_rows)} rows to sheet")
                    if self.append_to_sheet(config['spreadsheet_id'], config['sheet_range'], sheet_rows):
                        stats['rows_added']      += len(sheet_rows)
                        stats['processed_files'] += 1
                        self.log(f"[SUCCESS] Processed {excel_file['name']}: {len(sheet_rows)} rows added")
                    else:
                        stats['failed_files'] += 1
                        self.log("[ERROR] Failed to append data")

                except Exception as e:
                    self.log(f"[ERROR] Failed to process {excel_file.get('name', 'unknown')}: {str(e)}", "ERROR")
                    import traceback
                    traceback.print_exc()
                    stats['failed_files'] += 1

            self.log("\n" + "=" * 80)
            self.log("Drive to Sheets workflow complete!")
            self.log(f"Files processed : {stats['processed_files']}/{stats['total_files']}")
            self.log(f"Files skipped   : {stats['skipped_files']}")
            self.log(f"Files failed    : {stats['failed_files']}")
            self.log(f"Total rows added: {stats['rows_added']}")
            self.log("=" * 80)
            return stats

        except Exception as e:
            self.log(f"[ERROR] Workflow failed: {str(e)}", "ERROR")
            import traceback
            traceback.print_exc()
            return stats

    # ── Workflow logging ───────────────────────────────────────────────────────

    def log_workflow_to_sheet(self, workflow_name: str, start_time: datetime,
                               end_time: datetime, stats: dict):
        try:
            duration = (end_time - start_time).total_seconds()
            if duration >= 60:
                minutes = int(duration // 60)
                seconds = int(duration % 60)
                duration_str = f"{minutes}m {seconds}s"
            else:
                duration_str = f"{duration:.2f}s"

            log_row = [
                start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time.strftime("%Y-%m-%d %H:%M:%S"),
                duration_str,
                workflow_name,
                stats.get('processed', stats.get('processed_files', 0)),
                stats.get('total_attachments', stats.get('rows_added', 0)),
                stats.get('failed', stats.get('failed_files', 0)),
                stats.get('skipped', stats.get('skipped_files', 0)),
                "Success" if stats.get('processed', stats.get('processed_files', 0)) > 0 else "No New Files"
            ]

            log_config = CONFIG['workflow_log']

            try:
                result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=log_config['spreadsheet_id'],
                    range=f"{log_config['sheet_range']}!A1:Z1"
                ).execute()
                if not result.get('values'):
                    self.append_to_sheet(
                        log_config['spreadsheet_id'], log_config['sheet_range'],
                        [["Start Time", "End Time", "Duration", "Workflow",
                          "Processed", "Total Items", "Failed", "Skipped", "Status"]]
                    )
            except Exception:
                pass

            self.append_to_sheet(log_config['spreadsheet_id'], log_config['sheet_range'], [log_row])
            self.log("[LOG] Workflow logged successfully")

        except Exception as e:
            self.log(f"[ERROR] Failed to log workflow: {str(e)}", "ERROR")

    # ── Orchestration ──────────────────────────────────────────────────────────

    def run_scheduled_workflow(self):
        try:
            self.log("\n" + "=" * 80)
            self.log("STARTING GRN SCHEDULED WORKFLOW RUN")
            self.log("=" * 80)

            overall_start = datetime.now(timezone.utc)

            # Workflow 1 — Mail → Drive
            self.log("\n[WORKFLOW 1/2] Starting Mail to Drive workflow...")
            mail_start = datetime.now(timezone.utc)
            mail_stats = self.process_mail_to_drive_workflow(CONFIG['mail'])
            mail_end   = datetime.now(timezone.utc)
            self.log_workflow_to_sheet("GRN Mail to Drive", mail_start, mail_end, mail_stats)

            import time
            time.sleep(5)

            # Workflow 2 — Drive → Sheets
            self.log("\n[WORKFLOW 2/2] Starting Drive to Sheet workflow...")
            sheet_start = datetime.now(timezone.utc)
            sheet_stats = self.process_drive_to_sheet_workflow(CONFIG['sheet'], skip_existing=True)
            sheet_end   = datetime.now(timezone.utc)
            self.log_workflow_to_sheet("GRN Drive to Sheet", sheet_start, sheet_end, sheet_stats)

            total_duration = (datetime.now(timezone.utc) - overall_start).total_seconds()
            self.log("\n" + "=" * 80)
            self.log("GRN SCHEDULED WORKFLOW RUN COMPLETED")
            self.log(f"Total Duration   : {total_duration:.2f} seconds")
            self.log(f"Mail to Drive    : {mail_stats['processed']} emails, "
                     f"{mail_stats['total_attachments']} attachments")
            self.log(f"Drive to Sheet   : {sheet_stats['processed_files']} files processed, "
                     f"{sheet_stats['rows_added']} rows added")
            self.log("=" * 80 + "\n")

        except Exception as e:
            self.log(f"[ERROR] Scheduled workflow failed: {str(e)}", "ERROR")
            import traceback
            traceback.print_exc()


# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("Consolidated GRN Automation")
    print("=" * 80)

    automation = GRNAutomation()

    print("\nAuthenticating...")
    if not automation.authenticate():
        print("ERROR: Authentication failed.")
        return

    print("Authentication successful!")
    print("\nRunning GRN workflow...")
    automation.run_scheduled_workflow()
    print("\nWorkflow completed. Exiting.")


if __name__ == "__main__":
    main()
