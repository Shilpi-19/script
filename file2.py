


import os
import json
import tempfile
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SCOPES = ['https://www.googleapis.com/auth/drive']
TRACK_FILE = 'uploaded_files.json'  # File to track uploaded files
FOLDER_NAME = "ResumeUploads"  # Folder to store Excel sheets on Google Drive

def authenticate_google_drive():
    """Authenticate using OAuth 2.0 and return the Google Drive service."""
    creds = None
    token_path = 'token.json'

    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        with open(token_path, 'w') as token_file:
            token_file.write(creds.to_json())

    return build('drive', 'v3', credentials=creds)

def create_or_get_folder(service, folder_name):
    """Create a folder on Google Drive or return its ID if it already exists."""
    query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    folders = results.get('files', [])
    
    if folders:
        folder_id = folders[0]['id']
        print(f"Folder '{folder_name}' found with ID: {folder_id}")
    else:
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        folder_id = folder.get('id')
        print(f"Folder '{folder_name}' created with ID: {folder_id}")
    
    return folder_id

def load_uploaded_files():
    """Load the list of previously uploaded files from the tracking file."""
    if os.path.exists(TRACK_FILE):
        with open(TRACK_FILE, 'r') as file:
            return json.load(file)
    return []

def save_uploaded_files(files):
    """Save the list of uploaded files to the tracking file."""
    with open(TRACK_FILE, 'w') as file:
        json.dump(files, file)

def get_files_from_downloads(folder_path):
    """Fetches details of pdf, doc, and docx files from the specified folder."""
    extensions = ['.pdf', '.doc', '.docx']
    files_data = []

    for root, _, files in os.walk(folder_path):
        for file in files:
            if any(file.lower().endswith(ext) for ext in extensions):
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                files_data.append({
                    "File Name": file,
                    "File Path": file_path,
                    "File Size (KB)": round(file_size / 1024, 2),
                })

    return files_data

def upload_to_google_drive(service, file_path, folder_id, mime_type='application/pdf'):
    """Uploads a file to Google Drive inside a specific folder and returns the shareable URL."""
    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [folder_id]  # Specify the parent folder
    }
    media = MediaFileUpload(file_path, mimetype=mime_type)
    uploaded_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    
    file_id = uploaded_file.get('id')
    file_url = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
    print(f"File uploaded to Google Drive with ID: {file_id}")
    return file_url

def upload_excel_to_drive(service, data, output_file_name, folder_id):
    """Saves the file data (including Google Drive links) to an Excel sheet and uploads it directly to Google Drive."""
    df = pd.DataFrame(data)

    # Use a temporary file for the Excel sheet
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_file:
        temp_excel_path = temp_file.name
        df.to_excel(temp_excel_path, index=False)

    # Upload the Excel file to the specified folder
    excel_url = upload_to_google_drive(service, temp_excel_path, folder_id, mime_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    print(f"Excel sheet uploaded to Google Drive: {excel_url}")

    os.remove(temp_excel_path)
    return excel_url

def main():
    # Authenticate Google Drive
    drive_service = authenticate_google_drive()

    # Create or get the folder for storing Excel sheets
    folder_id = create_or_get_folder(drive_service, FOLDER_NAME)

    # Load the list of already uploaded files
    uploaded_files = load_uploaded_files()

    # Specify the Downloads folder
    downloads_folder = os.path.expanduser("~/Downloads")

    # Get file details from Downloads
    files_data = get_files_from_downloads(downloads_folder)

    # Filter out already uploaded files
    new_files = [file for file in files_data if file["File Name"] not in uploaded_files]

    if new_files:
        # Upload each new file to Google Drive and get the URL
        for file in new_files:
            file_path = file["File Path"]
            file_url = upload_to_google_drive(drive_service, file_path, folder_id)
            file["Download Link"] = file_url

        # Save the new file names to the tracking file
        uploaded_files.extend(file["File Name"] for file in new_files)
        save_uploaded_files(uploaded_files)

        # Save details (including links) to Excel and upload it directly to Google Drive
        upload_excel_to_drive(drive_service, new_files, "File_Details_with_Links.xlsx", folder_id)
    else:
        print("No new files to upload.")

if __name__ == "__main__":
    main()
