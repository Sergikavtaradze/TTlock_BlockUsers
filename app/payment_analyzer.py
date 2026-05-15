import pandas as pd
import sqlite3
import re
import math

class AccessDatabase:
    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row  # Access columns by name

    def clean_currency(self, value_str):
        """Converts DB strings like '30 ₾' to float 30.0"""
        if not value_str or not isinstance(value_str, str):
            return 0.0
        # Remove non-numeric characters except decimal point and negative sign
        clean = re.sub(r'[^\d.-]', '', value_str)
        try:
            return float(clean)
        except ValueError:
            return 0.0

    def find_user(self, tax_code, partner_name, description):
        cursor = self.conn.cursor()
        
        # --- PRIORITY 1: Payment Partner (Existing Link) ---
        if tax_code:
            cursor.execute("SELECT * FROM access_with_owners WHERE payment_partner LIKE ?", (f'%{tax_code}%',))
            user = cursor.fetchone()
            if user: return dict(user), "Payment Partner (ID Match)"

        if partner_name:
            cursor.execute("SELECT * FROM access_with_owners WHERE payment_partner LIKE ?", (f'%{partner_name}%',))
            user = cursor.fetchone()
            if user: return dict(user), "Payment Partner (Name Match)"

        # --- PRIORITY 2: Owner Name ---
        if partner_name:
            cursor.execute("SELECT * FROM access_with_owners WHERE owner_name LIKE ?", (f'%{partner_name}%',))
            user = cursor.fetchone()
            if user: return dict(user), "Owner Name"

        # --- PRIORITY 3: Apartment ID from Description + DB UPDATE ---
        apt_matches = re.findall(r'(?:apt|flat|bina|ბინა|^|\s)(\d{1,3})(?:[\s/,&]|$)', description or "", re.IGNORECASE)
        
        for apt in apt_matches:
            # Look for apartment number inside apt_id or original_label
            query = "SELECT * FROM access_with_owners WHERE apt_id LIKE ? OR original_label LIKE ?"
            cursor.execute(query, (f'%{apt}%', f'%{apt}%'))
            user = cursor.fetchone()
            
            if user:
                user_dict = dict(user)
                
                # --- LEARN THE PAYER ---
                # We found the apartment via description, so let's link this payer to the unit in the DB
                if partner_name or tax_code:
                    new_partner_str = f"{partner_name}, {tax_code}".strip(", ")
                    
                    # Update the database permanently
                    update_sql = "UPDATE access_with_owners SET payment_partner = ? WHERE key_id = ?"
                    cursor.execute(update_sql, (new_partner_str, user_dict['key_id']))
                    self.conn.commit()
                    
                    return user_dict, f"Apt Desc ({apt}) - DB UPDATED with new partner: {new_partner_str}"
                
                return user_dict, f"Apartment Description ({apt})"

        # --- NO MATCH: RETURN DETAILED REASONS ---
        # If we reach here, nothing worked. return details.
        failure_reason = (
            f"FAILED MATCH | "
            f"Tax ID Tried: '{tax_code}' (Not in DB) | "
            f"Name Tried: '{partner_name}' (Not in DB) | "
            f"Desc Parsed: {apt_matches} (No Apt found in DB)"
        )
        return None, failure_reason

    def update_balance(self, key_id, amount_paid):
        """
        Updates the debt in the database.
        Logic: New_Debt = Old_Debt - Payment
        """
        cursor = self.conn.cursor()
        
        # Get current debt
        cursor.execute("SELECT debt FROM access_with_owners WHERE key_id = ?", (key_id,))
        row = cursor.fetchone()
        if not row: return None
        
        current_debt = self.clean_currency(row['debt'])
        new_debt = current_debt - amount_paid
        
        # Update DB (Formatted back to string with symbol if needed, or just number)
        # Assuming we store back as plain number for now for easier math later
        cursor.execute("UPDATE access_with_owners SET debt = ? WHERE key_id = ?", (str(new_debt), key_id))
        self.conn.commit()
        return new_debt

class StatementAnalyzer:
    def __init__(self, db_handler):
        self.db = db_handler

    def analyze_payment(self, user, amount_paid):
        monthly_fee = self.db.clean_currency(user['monthly_fee'])
        
        if monthly_fee == 0:
            return "Fee Unknown", "Check DB configuration"

        # Check for multiples
        # We use a small epsilon (0.01) for floating point comparison
        months_covered = amount_paid / monthly_fee
        remainder = amount_paid % monthly_fee
        
        is_exact_multiple = (remainder < 0.1) or (abs(remainder - monthly_fee) < 0.1)

        if is_exact_multiple:
            count = int(round(months_covered))
            if count == 1:
                return "Standard Rent", "1 Month"
            elif count > 1:
                return "Prepayment / Arrears", f"{count} Months"
            else:
                return "Zero Payment", "0 Months"
        else:
            return "FLAG: Other Expense", f"Paid {amount_paid}, Fee {monthly_fee} (Not a multiple)"

    def process_csv(self, csv_path):
        # Skip row 0 (Georgian) use row 1 (English) headers
        df = pd.read_csv(csv_path, header=1)
        incomes = df[df['Transaction Type'] == 'Income'].copy()
        
        results = []

        print(f"Processing {len(incomes)} transactions...")

        for _, row in incomes.iterrows():
            # Extract CSV fields
            tax_code = row.get("Partner's Tax Code")
            partner_name = row.get("Partner's Name")
            desc = row.get("Description")
            amount = row.get("Amount", 0.0)

            # 1. Match User
            user, match_type = self.db.find_user(tax_code, partner_name, desc)

            if user:
                # 2. Analyze Fee Structure
                payment_type, notes = self.analyze_payment(user, amount)
                
                # 3. Update Debt (Simulated logic here)
                # new_debt = self.db.update_balance(user['key_id'], amount)
                current_debt = self.db.clean_currency(user['debt'])
                new_debt = current_debt - amount # logical calculation
                
                # 4. Determine Access Action
                # If debt is cleared (<= 0), unfreeze. Otherwise, freeze.
                action = "UNFREEZE" if new_debt <= 0 else "FREEZE (Still in Debt)"

                results.append({
                    "Payer": partner_name,
                    "Matched User": user['owner_name'],
                    "Match Method": match_type,
                    "Amount": amount,
                    "Fee": user['monthly_fee'],
                    "Payment Type": payment_type,
                    "Notes": notes,
                    "Old Debt": current_debt,
                    "New Debt": new_debt,
                    "Action": action,
                    "TTLock ID": user['key_id']
                })
            else:
                results.append({
                    "Payer": partner_name,
                    "Matched User": "UNKNOWN",
                    "Match Method": "FAILED",
                    "Amount": amount,
                    "Action": "MANUAL REVIEW",
                    "Notes": f"Desc: {desc}"
                })

        return pd.DataFrame(results)

# --- EXECUTION ---
if __name__ == "__main__":
    # Initialize DB connection
    db = AccessDatabase('databases/building_access_full.db')
    analyzer = StatementAnalyzer(db)
    
    # Process the statement
    # Ensure 'bank_statement.csv' exists or update path
    report = analyzer.process_csv('databases/transactions_history_December.csv')
    
    # Display Report
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(report[['Matched User', 'Match Method', 'Amount', 'Payment Type', 'Action']])
    
    # Save report for manual review
    report.to_csv('payment_analysis_report.csv', index=False)
    print("\nReport saved to 'payment_analysis_report.csv'")