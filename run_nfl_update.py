import pandas as pd
import gspread
from gspread_dataframe import get_as_dataframe
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# --- SETTINGS ---
# 1. Your Google Sheet Name
GOOGLE_SHEET_NAME = "My_NFL_Betting_Data1"

# 2. Your Worksheet (tab) Name
WORKSHEET_NAME = "live_picks_sheets"

# 3. Your Firestore Collection Name (from your screenshot)
FIRESTORE_COLLECTION = "live_picks"

# 4. !! IMPORTANT !!
# The name of the column in your sheet that has the unique ID for each row.
# Firestore will use this as the Document ID. (e.g., "pick_id", "game_id")
KEY_COLUMN = "pick_id" 

# 5. Standard credential file name
CREDENTIALS_FILE = "credentials.json"
# --- END SETTINGS ---

def authenticate_gspread():
    """Connects to Google Sheets API using the service account."""
    print("Authenticating with Google Sheets...")
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        gc = gspread.service_account(filename=CREDENTIALS_FILE, scopes=scope)
        print("Authentication successful.")
        return gc
    except Exception as e:
        print(f"Authentication failed: {e}")
        return None

def initialize_firebase():
    """Initializes the Firebase Admin SDK using the same service account."""
    print("Initializing Firebase (Cloud Firestore)...")
    try:
        cred = credentials.Certificate(CREDENTIALS_FILE)
        # You don't need a databaseURL for Firestore
        firebase_admin.initialize_app(cred)
        print("Firebase initialization successful.")
        return True
    except Exception as e:
        if 'already exists' in str(e):
            print("Firebase app already initialized.")
            return True
        print(f"Firebase initialization failed: {e}")
        return False

def fetch_sheet_data(gc):
    """Fetches all data from the Google Sheet."""
    print(f"Opening Google Sheet: '{GOOGLE_SHEET_NAME}'")
    try:
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.worksheet(WORKSHEET_NAME)
        
        print(f"Reading all data from worksheet: '{WORKSHEET_NAME}'...")
        # Use gspread-dataframe to read the sheet into a pandas DataFrame
        # 'header=0' means it will use the first row as column names
        data_df = get_as_dataframe(worksheet, header=0)
        
        # Drop any rows where all values are empty
        data_df.dropna(how='all', inplace=True)
        
        print(f"Fetched {len(data_df)} rows from sheet.")
        return data_df
    except Exception as e:
        print(f"Failed to fetch sheet data: {e}")
        return None

def update_firebase(data_df):
    """Updates Firestore with the data from the sheet."""
    if data_df is None or data_df.empty:
        print("No data from sheet. Skipping Firebase update.")
        return

    if KEY_COLUMN not in data_df.columns:
        print(f"ERROR: Your KEY_COLUMN '{KEY_COLUMN}' was not found in the sheet.")
        print(f"Available columns are: {list(data_df.columns)}")
        return

    print(f"Connecting to Firestore collection: '{FIRESTORE_COLLECTION}'")
    try:
        db = firestore.client()
        
        # Use a "batch" to send all updates at once (much faster)
        batch = db.batch()
        
        # Convert DataFrame to a dictionary
        data_df_cleaned = data_df.fillna(value="") # Replace NaN with empty string
        
        # 'to_dict('records')' makes a list of dicts, one for each row
        records = data_df_cleaned.to_dict('records')
        
        print(f"Preparing batch write for {len(records)} documents...")
        
        for record in records:
            # Get the unique ID from the row and convert it to a string
            doc_id = str(record[KEY_COLUMN])
            
            # Create a reference to the document in Firestore
            # This will create a new doc or overwrite an existing one
            doc_ref = db.collection(FIRESTORE_COLLECTION).document(doc_id)
            
            # Add the 'set' operation to the batch
            batch.set(doc_ref, record)
            
        # Commit all the changes at once
        batch.commit()
        
        print("Firebase update complete.")
    except Exception as e:
        print(f"Failed to update Firebase: {e}")

# --- Main execution ---
if __name__ == "__main__":
    gc = authenticate_gspread()
    firebase_ready = initialize_firebase()
    
    if gc and firebase_ready:
        sheet_data = fetch_sheet_data(gc)
        update_firebase(sheet_data)
    else:
        print("Failed to authenticate with Google Sheets or Firebase. Exiting.")
