
import os
import json
from docx import Document
from PyPDF2 import PdfReader
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from langchain_google_genai import ChatGoogleGenerativeAI
import pandas as pd
import io
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MONGO_URI = os.getenv("MONGO_URI")
EXCEL_FILE_PATH = "parsed_resumes.xlsx"

# Directory to store processed files
PROCESSED_FILES_DIR = "ProcessedFiles"

# Ensure the directory exists
os.makedirs(PROCESSED_FILES_DIR, exist_ok=True)

# Check if API key and Mongo URI are set
if not GEMINI_API_KEY:
    raise Exception("Gemini API Key not found or is empty in .env file!")
if not MONGO_URI:
    raise Exception("MongoDB URI not found or is empty in .env file!")

# Google Drive Authentication
SCOPES = ['https://www.googleapis.com/auth/drive']
PROCESSED_FILES_TRACKER = os.path.join(PROCESSED_FILES_DIR, 'processed_files.json')

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

def load_processed_files():
    """Load the list of previously processed files from the tracker."""
    if os.path.exists(PROCESSED_FILES_TRACKER):
        with open(PROCESSED_FILES_TRACKER, 'r') as file:
            return set(json.load(file))
    return set()

def save_processed_files(processed_files):
    """Save the list of processed files to the tracker."""
    with open(PROCESSED_FILES_TRACKER, 'w') as file:
        json.dump(list(processed_files), file)

def download_file_from_drive(service, file_id, file_name):
    """Download a file from Google Drive."""
    file_path = os.path.join(PROCESSED_FILES_DIR, file_name)  # Save in ProcessedFiles directory
    request = service.files().get_media(fileId=file_id)
    with io.BytesIO() as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download progress: {int(status.progress() * 100)}%")
        with open(file_path, 'wb') as f:
            f.write(fh.getvalue())
    print(f"File downloaded: {file_path}")
    return file_path

def extract_text(file_path):
    """Extracts text from PDF or DOCX files."""
    if file_path.endswith(".pdf"):
        try:
            reader = PdfReader(file_path)
            return "".join(page.extract_text() or "" for page in reader.pages).strip()
        except Exception as e:
            print(f"Error reading PDF: {str(e)}")
    elif file_path.endswith(".docx"):
        try:
            doc = Document(file_path)
            return "\n".join(paragraph.text for paragraph in doc.paragraphs).strip()
        except Exception as e:
            print(f"Error reading DOCX: {str(e)}")
    else:
        print(f"Unsupported file type for {file_path}. Skipping.")
    return None

def parse_resume_with_langchain(file_path):
    """Parses the resume using LangChain with GooglePalm API."""
    try:
        llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", api_key=GEMINI_API_KEY)
        text = extract_text(file_path)
        if not text:
            raise ValueError("Failed to extract text from the resume.")

        messages = [
            {"role": "system", "content": "Analyze the following resume and extract the information in JSON format with fields: "
                                           "Name, Contact Information (email, phone), Skills, Education, and Work Experience."},
            {"role": "user", "content": text}
        ]

        response = llm.invoke(input=messages)

        if hasattr(response, "content"):
            response_content = response.content
        elif isinstance(response, str):
            response_content = response
        else:
            response_content = None

        if not response_content or not response_content.strip():
            raise ValueError("Empty or invalid response from LangChain API.")

        try:
            json_start = response_content.find("{")
            json_end = response_content.rfind("}")
            cleaned_response = response_content[json_start:json_end + 1]
            parsed_response = json.loads(cleaned_response)
            return parsed_response
        except Exception as e:
            raise ValueError(f"JSON parsing failed: {str(e)}. Raw response: {response_content}")

    except Exception as e:
        print(f"Error processing resume with LangChain: {str(e)}")
        return {"error": f"LangChain processing failed: {str(e)}"}

def save_to_mongo(parsed_data, mongo_uri, db_name, collection_name):
    """Saves parsed resume data to MongoDB."""
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    
    if collection.find_one({"file_name": parsed_data["file_name"]}):
        print(f"Document with file_name {parsed_data['file_name']} already exists in MongoDB. Skipping insert.")
    else:
        collection.insert_one(parsed_data)
    client.close()

def save_to_excel(parsed_data, file_path=EXCEL_FILE_PATH):
    """Appends parsed data to the Excel file."""
    if os.path.exists(file_path):
        df_existing = pd.read_excel(file_path)
    else:
        df_existing = pd.DataFrame()

    df_parsed = pd.DataFrame([parsed_data])

    df_combined = pd.concat([df_existing, df_parsed]).drop_duplicates(subset=["file_name"])

    df_combined.to_excel(file_path, index=False)
    print(f"Data successfully saved to Excel: {file_path}")

# def save_to_excel(parsed_data, file_path=EXCEL_FILE_PATH):
#     """Appends parsed data to the Excel file, excluding 'file_name' and 'file_size' columns."""
#     print(f"Saving data to Excel: {file_path}")  # Debug log

#     # Load existing data (if any) into DataFrame
#     if os.path.exists(file_path):
#         print(f"Existing file found: {file_path}")
#         df_existing = pd.read_excel(file_path)
#     else:
#         print(f"No existing file found. A new file will be created at: {file_path}")
#         df_existing = pd.DataFrame()

#     # Convert parsed data to DataFrame
#     if parsed_data:
#         df_parsed = pd.DataFrame([parsed_data])
#     else:
#         print("Parsed data is empty. No data will be written to Excel.")
#         return

#     # Drop 'file_name' and 'file_size' columns if they exist
#     columns_to_drop = ['file_name', 'file_size']
#     df_parsed = df_parsed.drop(columns=columns_to_drop, errors='ignore')

#     # Concatenate with existing data, ensuring no duplicate rows
#     df_combined = pd.concat([df_existing, df_parsed]).drop_duplicates()

#     # Save combined data back to Excel
#     df_combined.to_excel(file_path, index=False)
#     print(f"Data successfully saved to {file_path}")

def get_latest_excel_from_folder(service, folder_id):
    """Retrieve the latest Excel file from the specified Google Drive folder."""
    query = f"'{folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
    results = service.files().list(q=query, orderBy="createdTime desc", fields="files(id, name)").execute()
    files = results.get('files', [])
    if not files:
        raise Exception("No Excel files found in the folder!")
    return files[0]['id'], files[0]['name']

def main():
    drive_service = authenticate_google_drive()
    processed_files = load_processed_files()

    folder_id = "15UjBIQpUcvm0eszAfyQ5J72qEHseY7gV"

    try:
        excel_file_id, excel_file_name = get_latest_excel_from_folder(drive_service, folder_id)
        excel_file_path = download_file_from_drive(drive_service, excel_file_id, excel_file_name)
    except Exception as e:
        print(f"Error retrieving the latest Excel file: {str(e)}")
        return

    df = pd.read_excel(excel_file_path)

    for _, row in df.iterrows():
        file_name = row["File Name"]
        if file_name in processed_files:
            print(f"Skipping already processed file: {file_name}")
            continue

        file_url = row["Download Link"]
        file_id = file_url.split("/d/")[1].split("/view")[0]

        try:
            downloaded_file_path = download_file_from_drive(drive_service, file_id, file_name)
            parsed_data = parse_resume_with_langchain(downloaded_file_path)
            if "error" in parsed_data:
                print(f"Error parsing {file_name}: {parsed_data['error']}")
            else:
                parsed_data["file_name"] = file_name
                parsed_data["file_size"] = row["File Size (KB)"]
                save_to_mongo(parsed_data, MONGO_URI, "resume_db", "parsed_resumes")
                save_to_excel(parsed_data)
                print(f"Parsed and saved {file_name} successfully.")
                processed_files.add(file_name)
        except Exception as e:
            print(f"Error processing {file_name}: {str(e)}")

    save_processed_files(processed_files)

if __name__ == "__main__":
    main()




