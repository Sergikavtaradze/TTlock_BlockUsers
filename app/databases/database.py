import pandas as pd
import sqlite3
import json
import re
import os

# --- FILE PATHS (Update these if your filenames differ) ---
JSON_FILE = 'building_access_master.json'
TRANS_CSV = 'transactions_history_FULL.csv'
OWNERS_CSV = '2025 გადასახადების მოსაკრებელი.csv'

def clean_apt_id(val):
    """
    Standardizes apartment IDs for joining tables.
    Removes decimal zeros (e.g., '10.0' -> '10') and whitespace.
    """
    if pd.isna(val): return None
    s = str(val).strip()
    # Remove .0 if it exists (common in Excel imports)
    if s.endswith('.0'):
        s = s[:-2]
    return s

def parse_label(label):
    """
    Splits a JSON label into the base apartment number.
    Example: "02 HL" -> Base: "2", Suffix: "HL"
    Example: "60/64" -> Base: "60/64", Suffix: None
    """
    if not label: return "Unknown", None
    s = str(label).strip()
            
    # Extract the base number
    # regex matches start of string, digits, optional slashes/dots
    def extract_id(s):
        
        for suffix in ["LMD", "CMG", "HL", "cmg", "თელასი", "M", "m", "Mars", "მარსი", "ოფისი"]:
            if suffix in s:
                # s = s.replace(suffix, "").strip()
                return suffix # Want to make this the name of the apartment (use for offices)
        for suffix in ["ლემონდო", "ლემონდუ", "ლემონდუუ"]:
            if suffix in s:
                return "LMD"

        # First, try to match a range: "14 - 69", "100-200", etc.
        # range_match = re.search(r'^(\d+\s*[-–]\s*\d+)', s)
        # if range_match:
        #     return range_match.group(1)  # Keep as-is; e.g., "14 - 69"

        # Otherwise, try a single number (with possible /, like before)
        single_match = re.search(r'^(\d+[\/\d]*)', s)
        if single_match:
            base = single_match.group(1)
            # Only normalize if it's a plain number (no slashes)
            if '/' not in base and base.isdigit():
                return str(int(base))  # Remove leading zeros
            else:
                return base  # e.g., "01/02"

        return None  # No match

    s = extract_id(s)
    if s is None:
        print(f"Term did not match any pattern: {label}")
    return s

def create_databases():
    print("Loading files...")
    
    # 1. LOAD OWNERS DATA
    df_owners_raw = pd.read_csv(OWNERS_CSV, skiprows=1)
    
    # Select and rename specified columns
    cols_map = {
        'მესაკუთრეები:': 'owner_name',
        'ბინის #': 'apt_id',
        'მოსაკრებელი თვეში': 'monthly_fee',
        'ყოველთვიური მოსაკრებლის დავალიანება': 'debt'
    }
    
    # Filter only columns that exist
    available_cols = [c for c in cols_map.keys() if c in df_owners_raw.columns]
    df_owners = df_owners_raw[available_cols].copy()
    df_owners.rename(columns=cols_map, inplace=True)
    
    # Clean apt_id for joining
    df_owners['apt_id'] = df_owners['apt_id'].apply(clean_apt_id)
    df_owners = df_owners.dropna(subset=['apt_id'])

    # 2. LOAD TRANSACTIONS (To find Payment Partners)
    df_trans = pd.read_csv(TRANS_CSV, skiprows=1)
    
    # Logic: Find "Apartment X" in description -> Map to "Partner's Name"
    apt_partner_map = {}
    
    for _, row in df_trans.iterrows():
        desc = str(row.get('Description', ''))
        partner = row.get("Partner's Name")
        
        if pd.isna(partner): continue
        
        # Regex to find "bina X", "apt X", etc.
        match = re.search(r'(?:ბინა|apt|apartment)\s*(\d+)', desc, re.IGNORECASE)
        if match:
            apt_num = str(int(match.group(1))) # Normalize '02' -> '2'
            # Store/Update the partner for this apartment
            apt_partner_map[apt_num] = partner

    # Map payment partners to the owners dataframe
    df_owners['payment_partner'] = df_owners['apt_id'].map(apt_partner_map)

    # 3. CREATE OWNER DATABASE (Lite SQL DB 1)
    print("Creating 'financial_data.db'...")
    conn_fin = sqlite3.connect('financial_data.db')
    df_owners.to_sql('owners_financial_status', conn_fin, index=False, if_exists='replace')
    conn_fin.close()

    # 4. PROCESS JSON & JOIN (Lite SQL DB 2)
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

    access_rows = []
    
    # Iterate through ekeys and cards
    for category in ['ekeys', 'cards']:
        data_dict = json_data.get(category, {})
        for label, items in data_dict.items():
            # Parse label to get apt ID
            base_apt  = parse_label(label)
            
            for item in items:
                if category == 'ekeys':
                    access_rows.append({
                        'apt_id': base_apt,
                        'original_label': label,
                        'type': 'ekey',
                        'username': item.get('username'),
                        'key_id': item.get('keyId'),
                        'status': item.get('status')
                    })
                elif category == "cards":
                    access_rows.append({
                        'apt_id': base_apt,
                        'original_label': label,
                        'type': 'card',
                        'lockId': item.get("lockId"),
                        'cardNumber': item.get('cardNumber'),
                        'startDate': item.get("startDate"),
                        'endDate': item.get("endDate"),
                        'createDate': item.get("createDate")
                    })
                    

    df_access = pd.DataFrame(access_rows)

    # Merge Access Data with Owner Data
    # matching 'apt_id' from Access list to 'apt_id' from Owners list
    df_final = df_access.merge(df_owners, on='apt_id', how='left')

    print("Creating 'building_access_full.db'...")
    conn_acc = sqlite3.connect('building_access_full.db')
    
    # Save the main joined table
    df_final.to_sql('access_with_owners', conn_acc, index=False, if_exists='replace')
    
    conn_acc.close()
    
    print("\nProcess Complete!")
    print(f"- 'financial_data.db' created with {len(df_owners)} owner records.")
    print(f"- 'building_access_full.db' created with {len(df_final)} access records joined with owner info.")

if __name__ == "__main__":
    create_databases()