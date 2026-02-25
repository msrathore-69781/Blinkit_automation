#!/usr/bin/env python3
"""
Instamart GRN Scheduler - Excel Version
Workflow: Gmail → Drive → Excel Processing → Google Sheets
- Downloads Excel attachments from Gmail to Google Drive
- Processes Excel files from Drive
- Extracts PO number and date from filename (format: PONUMBER_YYYYMMDD_HHMMSS.xlsx)
- Pushes data to Google Sheets
"""

import os
import io
import base64
import logging
import schedule
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
        logging.FileHandler('instamart_scheduler.log'),
        logging.StreamHandler()
    ]
)

# Configuration
CONFIG = {
    'mail': {
        'gdrive_folder_id': '1nuvMdNYmciFDLo6mfAfyc_cXdbFWWLPp',
        'sender': 'purchaseorder@handsontrades.com',  # Leave empty to search all senders
        'search_term': 'PO_ZHPL',  # Search for emails with "PO" in subject/body
        'attachment_filter': '.xlsx',  # Only download Excel files
        'days_back': 7,
        'max_results': 500
    },
    'sheet': {
        'drive_folder_id': '1nuvMdNYmciFDLo6mfAfyc_cXdbFWWLPp',
        'spreadsheet_id': '1urhhzYZy_-0l_KAW0_TkPBj7UThoFyQfmiKd4F3cRZk',
        'sheet_range': 'blinkit',
        'days_back': 7,
        'max_files': 500
    },
    # Excel column mapping - Update these to match your actual Excel columns
    'excel_mapping': {
        'product_number': 'Item Code',
        'product_name': 'Product Description',
        'quantity_ordered': 'Quantity',
        "item_count":"Total Items",
        "PO Total Quantity":"Total Quantity",
        "PO Total Amount" : "Total Amount",
        'price_per_unit': 'Basic Cost Price',
        'mrp': 'MRP',
        'base_price': 'Landing Rate',
        'amount_per_line_amount': 'Total Amount',
        'total_items_in_po': 'Total Items in PO',
        'total_quantity_in_po': 'Total Quantity in PO',
        'total_amount_of_po': 'Total Amount of PO',

    },
    # Output columns for Google Sheets
    'output_columns': {
        'po_number': 'PO Number',
        'po_date': 'PO Date',
        'product_number': 'Product Number',
        'product_name': 'Product Name',
        'quantity_ordered': 'Qty',
        'price_per_unit': 'price_per_unit',
        'mrp': 'MRP',
        'base_price': 'Base Price',
        'amount_per_line_amount': 'Total Line Amount',
        'total_items_in_po': 'Total Items in PO',
        'total_quantity_in_po': 'Total Quantity in PO',
        'total_amount_of_po': 'Total Amount of PO',
        'source_file': '    '
    },
    'workflow_log': {
        'spreadsheet_id': '1zebjRyYd2R1d2f9iOeLJFWvinVn7_rJ5OwF1wVicZKM',
        'sheet_range': 'workflow_logs'
    },
    'credentials_path': 'credentials.json',
    'token_path': 'token.json'
}


class InstamartAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        # API scopes
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
    
    def log(self, message: str, level: str = "INFO"):
        """Log message with appropriate level"""
        if level.upper() == "ERROR":
            logging.error(message)
        elif level.upper() == "WARNING":
            logging.warning(message)
        else:
            logging.info(message)
    
    def authenticate(self):
        """Authenticate using local credentials file"""
        try:
            self.log("Starting authentication process...")
            
            creds = None
            combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
            
            # Load token if exists
            if os.path.exists(CONFIG['token_path']):
                creds = Credentials.from_authorized_user_file(CONFIG['token_path'], combined_scopes)
            
            # Refresh or get new credentials
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
                
                # Save credentials
                with open(CONFIG['token_path'], 'w') as token:
                    token.write(creds.to_json())
                self.log("Token saved successfully")
            
            # Build services
            self.gmail_service = build('gmail', 'v1', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            
            self.log("Authentication successful!")
            return True
            
        except Exception as e:
            self.log(f"Authentication failed: {str(e)}", "ERROR")
            return False
    
    def search_emails(self, sender: str = "", search_term: str = "", 
                     days_back: int = 7, max_results: int = 50) -> List[Dict]:
        """Search for emails with attachments"""
        try:
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
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
            self.log(f"[ERROR] Email search failed: {str(e)}")
            return []
    
    def get_email_details(self, message_id: str) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            self.log(f"[ERROR] Failed to get email details: {str(e)}")
            return {}
    
    def get_attachments(self, message_id: str, attachment_filter: str = "") -> List[Dict]:
        """Get all attachments from an email"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='full'
            ).execute()
            
            attachments = []
            
            def process_parts(parts):
                for part in parts:
                    if part.get('filename'):
                        filename = part['filename']
                        
                        # Apply filter if specified
                        if attachment_filter and attachment_filter.lower() not in filename.lower():
                            continue
                        
                        attachment_id = part['body'].get('attachmentId')
                        if attachment_id:
                            attachments.append({
                                'id': attachment_id,
                                'filename': filename,
                                'mimeType': part.get('mimeType', 'application/octet-stream')
                            })
                    
                    # Check nested parts
                    if 'parts' in part:
                        process_parts(part['parts'])
            
            if 'parts' in message['payload']:
                process_parts(message['payload']['parts'])
            
            return attachments
            
        except Exception as e:
            self.log(f"[ERROR] Failed to get attachments: {str(e)}")
            return []
    
    def download_attachment(self, message_id: str, attachment_id: str) -> Optional[bytes]:
        """Download attachment data"""
        try:
            attachment = self.gmail_service.users().messages().attachments().get(
                userId='me', messageId=message_id, id=attachment_id
            ).execute()
            
            data = attachment['data']
            file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
            return file_data
            
        except Exception as e:
            self.log(f"[ERROR] Failed to download attachment: {str(e)}")
            return None
    
    def upload_to_drive(self, file_data: bytes, filename: str, folder_id: str, mime_type: str) -> Optional[str]:
        """Upload file to Google Drive"""
        try:
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            
            media = MediaIoBaseUpload(
                io.BytesIO(file_data),
                mimetype=mime_type,
                resumable=True
            )
            
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, webViewLink'
            ).execute()
            
            return file.get('id')
            
        except Exception as e:
            self.log(f"[ERROR] Failed to upload to Drive: {str(e)}")
            return None
    
    def file_exists_in_drive(self, filename: str, folder_id: str) -> bool:
        """Check if file already exists in Drive folder"""
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            files = results.get('files', [])
            return len(files) > 0
            
        except Exception as e:
            self.log(f"[ERROR] Failed to check file existence: {str(e)}")
            return False
    
    def process_mail_to_drive_workflow(self, config: dict):
        """Process Mail to Drive workflow (Excel files only)"""
        stats = {
            'processed': 0,
            'failed': 0,
            'skipped': 0,
            'total_attachments': 0
        }
        
        try:
            self.log("=" * 80)
            self.log("Starting Mail to Drive workflow (Excel files)")
            self.log("=" * 80)
            
            # Search for emails
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
                    
                    # Get email details
                    email_details = self.get_email_details(message_id)
                    self.log(f"\n[EMAIL] Processing: {email_details.get('subject', 'Unknown')}")
                    
                    # Get attachments
                    attachments = self.get_attachments(message_id, config['attachment_filter'])
                    
                    if not attachments:
                        self.log("[SKIP] No Excel attachments found in email")
                        stats['skipped'] += 1
                        continue
                    
                    for attachment in attachments:
                        try:
                            filename = attachment['filename']
                            
                            # Check if file already exists
                            if self.file_exists_in_drive(filename, config['gdrive_folder_id']):
                                self.log(f"[SKIP] File already exists: {filename}")
                                stats['skipped'] += 1
                                continue
                            
                            # Download attachment
                            file_data = self.download_attachment(message_id, attachment['id'])
                            
                            if not file_data:
                                self.log(f"[ERROR] Failed to download: {filename}")
                                stats['failed'] += 1
                                continue
                            
                            # Upload to Drive
                            file_id = self.upload_to_drive(
                                file_data,
                                filename,
                                config['gdrive_folder_id'],
                                attachment['mimeType']
                            )
                            
                            if file_id:
                                self.log(f"[SUCCESS] Uploaded to Drive: {filename}")
                                stats['processed'] += 1
                                stats['total_attachments'] += 1
                            else:
                                self.log(f"[ERROR] Failed to upload: {filename}")
                                stats['failed'] += 1
                                
                        except Exception as e:
                            self.log(f"[ERROR] Processing attachment {attachment.get('filename')}: {str(e)}")
                            stats['failed'] += 1
                    
                except Exception as e:
                    self.log(f"[ERROR] Processing email: {str(e)}")
                    stats['failed'] += 1
            
            self.log("\n" + "=" * 80)
            self.log(f"Mail to Drive complete. Processed: {stats['processed']}, Failed: {stats['failed']}, Skipped: {stats['skipped']}")
            self.log("=" * 80)
            return stats
            
        except Exception as e:
            self.log(f"[ERROR] Mail to Drive workflow failed: {str(e)}")
            return stats
    
    def extract_po_from_filename(self, filename: str) -> Dict[str, str]:
        """
        Extract PO number and date from filename
        Format: PONUMBER_YYYYMMDD_HHMMSS.xlsx
        Example: 5630310004451_20260211_030533.xlsx
        Returns: {'po_number': '5630310004451', 'po_date': '2026-02-11'}
        """
        try:
            # Remove extension
            name_without_ext = filename.replace('.xlsx', '').replace('.xls', '')
            
            # Split by underscore
            parts = name_without_ext.split('_')
            
            if len(parts) >= 2:
                po_number = parts[0]
                po_date_raw = parts[1]  # YYYYMMDD format
                
                # Convert YYYYMMDD to YYYY-MM-DD
                if len(po_date_raw) == 8:
                    year = po_date_raw[0:4]
                    month = po_date_raw[4:6]
                    day = po_date_raw[6:8]
                    po_date = f"{year}-{month}-{day}"
                    
                    self.log(f"[FILENAME] Extracted PO: {po_number}, Date: {po_date}")
                    return {'po_number': po_number, 'po_date': po_date}
            
            self.log(f"[WARNING] Could not extract PO info from filename: {filename}")
            return {'po_number': '', 'po_date': ''}
            
        except Exception as e:
            self.log(f"[ERROR] Failed to extract PO from filename {filename}: {str(e)}")
            return {'po_number': '', 'po_date': ''}
    
    def list_excel_files(self, folder_id: str, days_back: int = 7) -> List[Dict]:
        """List Excel files in Drive folder"""
        try:
            start_date = datetime.now() - timedelta(days=days_back)
            start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
            
            # Search for Excel files
            mime_types = [
                "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",  # .xlsx
                "mimeType='application/vnd.ms-excel'"  # .xls
            ]
            
            mime_query = " or ".join(mime_types)
            query = f"'{folder_id}' in parents and ({mime_query}) and trashed=false and modifiedTime >= '{start_date_str}'"
            
            results = self.drive_service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name, createdTime, modifiedTime, mimeType)',
                orderBy='createdTime desc'
            ).execute()
            
            files = results.get('files', [])
            self.log(f"[DRIVE] Found {len(files)} Excel files in Drive folder")
            return files
            
        except Exception as e:
            self.log(f"[ERROR] Failed to list Drive files: {str(e)}")
            return []
    
    def download_excel_file(self, file_id: str) -> bytes:
        """Download Excel file from Google Drive"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_data = io.BytesIO()
            
            downloader = MediaIoBaseDownload(file_data, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            return file_data.getvalue()
            
        except Exception as e:
            self.log(f"[ERROR] Failed to download file: {str(e)}")
            return None
    
    def process_excel_file(self, file_data: bytes, file_info: Dict, excel_mapping: Dict) -> List[Dict]:
        """
        Process Excel file and extract data
        PO number and date come from filename
        Calculates:
        - Total Items in PO
        - Total Quantity in PO
        - Total Amount of PO
        """

        rows = []

        try:
            # Extract PO info
            po_info = self.extract_po_from_filename(file_info['name'])

            # Read Excel
            df = pd.read_excel(io.BytesIO(file_data), engine='openpyxl')

            # Remove completely empty rows
            df = df.dropna(how="all")

            self.log(f"[EXCEL] Read {len(df)} rows from {file_info['name']}")
            self.log(f"[EXCEL] Columns found: {list(df.columns)}")

            # Validate required columns
            required_fields = ['product_number', 'product_name']
            missing_columns = []

            for field in required_fields:
                excel_col = excel_mapping.get(field)
                if excel_col and excel_col not in df.columns:
                    missing_columns.append(excel_col)

            if missing_columns:
                self.log(f"[ERROR] Missing required columns: {missing_columns}", "ERROR")
                return rows

            # ------------------------------------------------------
            # STEP 1: Identify valid line item rows
            # ------------------------------------------------------
            valid_rows = []

            for _, row in df.iterrows():
                product_num = row.get(excel_mapping.get('product_number', ""), None)

                if pd.isna(product_num) or str(product_num).strip() in ['', 'nan', 'NaN', 'NAN']:
                    continue

                valid_rows.append(row)

            if not valid_rows:
                self.log(f"[WARNING] No valid line items found in {file_info['name']}")
                return rows

            # ------------------------------------------------------
            # STEP 2: Calculate Totals
            # ------------------------------------------------------
            total_items = len(valid_rows)
            total_quantity = 0
            total_amount = 0

            for row in valid_rows:

                qty_val = pd.to_numeric(
                    row.get(excel_mapping.get('quantity_ordered', ''), 0),
                    errors='coerce'
                )

                amt_val = pd.to_numeric(
                    row.get(excel_mapping.get('amount_per_line_amount', ''), 0),
                    errors='coerce'
                )

                if not pd.isna(qty_val):
                    total_quantity += qty_val

                if not pd.isna(amt_val):
                    total_amount += amt_val

            self.log(f"[TOTALS] Items: {total_items}, Qty: {total_quantity}, Amount: {total_amount}")

            # ------------------------------------------------------
            # STEP 3: Build output rows
            # ------------------------------------------------------
            for row in valid_rows:

                row_dict = {
                    # PO info
                    "po_number": po_info['po_number'],
                    "po_date": po_info['po_date'],

                    # Line item data
                    "product_number": str(row.get(excel_mapping.get('product_number', ''), '')).strip(),
                    "product_name": str(row.get(excel_mapping.get('product_name', ''), '')).strip(),
                    "quantity_ordered": str(row.get(excel_mapping.get('quantity_ordered', ''), '')).strip(),
                    "price_per_unit": str(row.get(excel_mapping.get('price_per_unit', ''), '')).strip(),
                    "mrp": str(row.get(excel_mapping.get('mrp', ''), '')).strip(),
                    "base_price": str(row.get(excel_mapping.get('base_price', ''), '')).strip(),
                    "amount_per_line_amount": str(row.get(excel_mapping.get('amount_per_line_amount', ''), '')).strip(),

                    # PO totals
                    "total_items_in_po": total_items,
                    "total_quantity_in_po": total_quantity,
                    "total_amount_of_po": total_amount,

                    # Tracking
                    "source_file": file_info.get('name', '')
                }

                rows.append(row_dict)

            self.log(f"[SUCCESS] Extracted {len(rows)} rows from {file_info['name']}")
            return rows

        except Exception as e:
            self.log(f"[ERROR] Failed to process Excel file: {str(e)}", "ERROR")
            import traceback
            traceback.print_exc()
            return rows

    
    def get_existing_source_files(self, spreadsheet_id: str, sheet_range: str) -> set:
        """Get set of already processed files from Google Sheet"""
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
            
            name_index = headers.index("source_file")
            existing_names = {row[name_index] for row in values[1:] if len(row) > name_index and row[name_index]}
            
            self.log(f"[SHEET] Found {len(existing_names)} already processed files")
            return existing_names
            
        except Exception as e:
            self.log(f"[ERROR] Failed to get existing files: {str(e)}")
            return set()
    
    def append_to_sheet(self, spreadsheet_id: str, sheet_range: str, values: List[List]) -> bool:
        """Append rows to Google Sheet"""
        try:
            body = {'values': values}
            
            self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            return True
            
        except Exception as e:
            self.log(f"[ERROR] Failed to append to sheet: {str(e)}")
            return False
    
    def setup_headers(self, spreadsheet_id: str, sheet_range: str, headers: List[str]) -> bool:
        """Setup or update sheet headers"""
        try:
            # Check existing headers
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_range}!A1:Z1"
            ).execute()
            
            existing_headers = result.get('values', [[]])[0] if result.get('values') else []
            
            if not existing_headers:
                # No headers exist, create them
                self.log("[SHEET] Creating headers...")
                return self.append_to_sheet(spreadsheet_id, sheet_range, [headers])
            elif existing_headers != headers:
                # Headers exist but don't match, update them
                self.log("[SHEET] Updating headers...")
                sheet_name = sheet_range.split('!')[0]
                body = {'values': [headers]}
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A1",
                    valueInputOption='USER_ENTERED',
                    body=body
                ).execute()
                return True
            else:
                self.log("[SHEET] Headers already exist and match")
                return True
                
        except Exception as e:
            self.log(f"[ERROR] Failed to setup headers: {str(e)}")
            return False
    
    def process_drive_to_sheet_workflow(self, config: dict, skip_existing: bool = True):
        """Main workflow: Process Excel files from Drive to Google Sheets"""
        
        stats = {
            'total_files': 0,
            'processed_files': 0,
            'failed_files': 0,
            'skipped_files': 0,
            'rows_added': 0
        }
        
        try:
            self.log("=" * 80)
            self.log("Starting Drive to Sheets workflow (Excel)")
            self.log("=" * 80)
            
            # Get list of already processed files
            existing_files = set()
            if skip_existing:
                existing_files = self.get_existing_source_files(
                    config['spreadsheet_id'],
                    config['sheet_range']
                )
            
            # List Excel files in Drive folder
            excel_files = self.list_excel_files(
                config['drive_folder_id'],
                config.get('days_back', 7)
            )
            
            stats['total_files'] = len(excel_files)
            
            # Filter out already processed files
            if skip_existing:
                original_count = len(excel_files)
                excel_files = [f for f in excel_files if f['name'] not in existing_files]
                stats['skipped_files'] = original_count - len(excel_files)
                self.log(f"[SKIP] Skipped {stats['skipped_files']} already processed files")
            
            # Limit number of files
            max_files = config.get('max_files')
            if max_files and len(excel_files) > max_files:
                excel_files = excel_files[:max_files]
                self.log(f"[LIMIT] Limited to {max_files} files")
            
            if not excel_files:
                self.log("[INFO] No new Excel files to process")
                return stats
            
            # Setup headers
            output_columns = list(CONFIG['output_columns'].keys())
            display_headers = [CONFIG['output_columns'][col] for col in output_columns]
            
            if not self.setup_headers(config['spreadsheet_id'], config['sheet_range'], display_headers):
                self.log("[ERROR] Failed to setup headers", "ERROR")
                return stats
            
            # Process each Excel file
            for excel_file in excel_files:
                try:
                    self.log(f"\n[PROCESSING] {excel_file['name']}")
                    
                    # Download file
                    file_data = self.download_excel_file(excel_file['id'])
                    if not file_data:
                        self.log(f"[ERROR] Failed to download {excel_file['name']}")
                        stats['failed_files'] += 1
                        continue
                    
                    # Process Excel data
                    rows_data = self.process_excel_file(
                        file_data,
                        excel_file,
                        CONFIG['excel_mapping']
                    )
                    
                    if not rows_data:
                        self.log(f"[SKIP] No data found in {excel_file['name']}")
                        stats['failed_files'] += 1
                        continue
                    
                    # Convert to sheet rows (only selected columns)
                    sheet_rows = []
                    for row_dict in rows_data:
                        row_values = [row_dict.get(col, "") for col in output_columns]
                        sheet_rows.append(row_values)
                    
                    # Append to Google Sheet
                    self.log(f"[APPEND] Appending {len(sheet_rows)} rows to sheet")
                    
                    if self.append_to_sheet(config['spreadsheet_id'], config['sheet_range'], sheet_rows):
                        stats['rows_added'] += len(sheet_rows)
                        stats['processed_files'] += 1
                        self.log(f"[SUCCESS] Processed {excel_file['name']}: {len(sheet_rows)} rows added")
                    else:
                        stats['failed_files'] += 1
                        self.log(f"[ERROR] Failed to append data")
                
                except Exception as e:
                    self.log(f"[ERROR] Failed to process {excel_file.get('name', 'unknown')}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    stats['failed_files'] += 1
            
            # Summary
            self.log("\n" + "=" * 80)
            self.log("Drive to Sheets workflow complete!")
            self.log(f"Files processed: {stats['processed_files']}/{stats['total_files']}")
            self.log(f"Files skipped: {stats['skipped_files']}")
            self.log(f"Files failed: {stats['failed_files']}")
            self.log(f"Total rows added: {stats['rows_added']}")
            self.log("=" * 80)
            
            return stats
            
        except Exception as e:
            self.log(f"[ERROR] Workflow failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return stats
    
    def log_workflow_to_sheet(self, workflow_name: str, start_time: datetime, 
                             end_time: datetime, stats: dict):
        """Log workflow execution to workflow_logs sheet"""
        try:
            duration = (end_time - start_time).total_seconds()
            duration_str = f"{duration:.2f}s"
            
            if duration >= 60:
                minutes = int(duration // 60)
                seconds = int(duration % 60)
                duration_str = f"{minutes}m {seconds}s"
            
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
            
            # Check if headers exist
            try:
                result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=log_config['spreadsheet_id'],
                    range=f"{log_config['sheet_range']}!A1:Z1"
                ).execute()
                
                if not result.get('values'):
                    # Create headers
                    header_row = [
                        "Start Time", "End Time", "Duration", "Workflow",
                        "Processed", "Total Items", "Failed", "Skipped", "Status"
                    ]
                    self.append_to_sheet(
                        log_config['spreadsheet_id'],
                        log_config['sheet_range'],
                        [header_row]
                    )
            except:
                pass
            
            # Append log row
            self.append_to_sheet(
                log_config['spreadsheet_id'],
                log_config['sheet_range'],
                [log_row]
            )
            
            self.log("[LOG] Workflow logged successfully")
            
        except Exception as e:
            self.log(f"[ERROR] Failed to log workflow: {str(e)}")
    
    def run_scheduled_workflow(self):
        """Run both workflows in sequence and log results"""
        try:
            self.log("\n" + "=" * 80)
            self.log("STARTING SCHEDULED WORKFLOW RUN")
            self.log("=" * 80)
            
            overall_start = datetime.now(timezone.utc)
            
            # Workflow 1: Mail to Drive
            self.log("\n[WORKFLOW 1/2] Starting Mail to Drive workflow...")
            mail_start = datetime.now(timezone.utc)
            mail_stats = self.process_mail_to_drive_workflow(CONFIG['mail'])
            mail_end = datetime.now(timezone.utc)
            self.log_workflow_to_sheet("Mail to Drive", mail_start, mail_end, mail_stats)
            
            # Small delay between workflows
            import time
            time.sleep(5)
            
            # Workflow 2: Drive to Sheet
            self.log("\n[WORKFLOW 2/2] Starting Drive to Sheet workflow...")
            sheet_start = datetime.now(timezone.utc)
            sheet_stats = self.process_drive_to_sheet_workflow(CONFIG['sheet'], skip_existing=True)
            sheet_end = datetime.now(timezone.utc)
            
            sheet_stats_for_log = {
                'processed_files': sheet_stats['processed_files'],
                'rows_added': sheet_stats['rows_added'],
                'failed_files': sheet_stats['failed_files'],
                'skipped_files': sheet_stats['skipped_files'],
                'success': sheet_stats['processed_files'] > 0
            }
            self.log_workflow_to_sheet("Drive to Sheet", sheet_start, sheet_end, sheet_stats_for_log)
            
            overall_end = datetime.now(timezone.utc)
            total_duration = (overall_end - overall_start).total_seconds()
            
            self.log("\n" + "=" * 80)
            self.log("SCHEDULED WORKFLOW RUN COMPLETED")
            self.log(f"Total Duration: {total_duration:.2f} seconds")
            self.log(f"Mail to Drive: {mail_stats['processed']} emails, {mail_stats['total_attachments']} attachments")
            self.log(f"Drive to Sheet: {sheet_stats['processed_files']} files processed, {sheet_stats['rows_added']} rows added")
            self.log("=" * 80 + "\n")
            
        except Exception as e:
            self.log(f"[ERROR] Scheduled workflow failed: {str(e)}")
            import traceback
            traceback.print_exc()
def main():
    """Main function - run once and exit"""

    print("=" * 80)
    print("Instamart GRN Automation")
    print("=" * 80)

    automation = InstamartAutomation()

    print("\nAuthenticating...")
    if not automation.authenticate():
        print("ERROR: Authentication failed.")
        return

    print("Authentication successful!")

    print("\nRunning workflow...")
    automation.run_scheduled_workflow()

    print("\nWorkflow completed. Exiting.")

if __name__ == "__main__":
    main()