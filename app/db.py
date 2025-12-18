import sqlite3
import os

DB_PATH = 'access_control.db'

def initialize_db():
    """Create the Tenants table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS Tenants (
            flat_number INTEGER PRIMARY KEY,
            ttlock_lock_id INTEGER NOT NULL,
            tenant_email TEXT NOT NULL,
            monthly_fee REAL NOT NULL,
            current_credit_balance REAL DEFAULT 0.0,
            is_access_active BOOLEAN DEFAULT 0
        )
    """)
    # Note: We skip the Transactions table for the MVP speed.
    conn.commit()
    conn.close()

def import_initial_data(data_file_path='initial_tenant_data.csv'):
    """Import the required data from the static CSV file into the DB."""
    import pandas as pd
    
    # Run initialize_db() first
    initialize_db()
    
    try:
        df = pd.read_csv(data_file_path)
        conn = sqlite3.connect(DB_PATH)
        
        # Write the data frame to the Tenants table
        # If a record with the same flat_number exists, it will replace it (if_exists='replace')
        df.to_sql('Tenants', conn, if_exists='replace', index=False)
        conn.close()
        print(f"Successfully loaded {len(df)} tenants into the database.")
    except FileNotFoundError:
        print("Initial data file not found. Database table created but empty.")

def get_tenant_info(flat_number: int):
    """Retrieve all data for a single flat."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT * FROM Tenants WHERE flat_number=?", (flat_number,))
    # In a real app, you would parse the result row into a Pydantic Model
    result = c.fetchone()
    conn.close()
    return result

def update_tenant_credit(flat_number: int, new_balance: float, access_status: bool):
    """Update the tenant's credit and access status."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        UPDATE Tenants 
        SET current_credit_balance = ?, is_access_active = ? 
        WHERE flat_number = ?
    """, (new_balance, access_status, flat_number))
    conn.commit()
    conn.close()