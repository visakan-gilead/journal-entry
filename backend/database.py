import sqlite3
import bcrypt
import uuid
import json
from datetime import datetime
from typing import Optional

class SnowflakeDB:
    def __init__(self):
        self.db_path = "sap_database.db"
        self.create_users_table()
   
    def create_users_table(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
            """)
            conn.commit()
   
    def create_user(self, username: str, email: str, password: str) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if user exists
                cursor = conn.execute("SELECT id FROM users WHERE email = ? OR username = ?", (email, username))
                if cursor.fetchone():
                    return False
                
                password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                user_id = str(uuid.uuid4())
                
                conn.execute("""
                    INSERT INTO users (id, username, email, password_hash, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (user_id, username, email, password_hash, datetime.utcnow().isoformat()))
                conn.commit()
                
                # Log user account to JSON file
                user_data = {
                    "id": user_id,
                    "username": username,
                    "email": email,
                    "created_at": datetime.utcnow().isoformat()
                }
                with open("user_accounts.json", "a") as f:
                    f.write(json.dumps(user_data) + "\n")
                
                return True
        except Exception as e:
            print(f"Error creating user: {e}")
            return False
   
    def authenticate_user(self, email: str, password: str) -> Optional[dict]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT id, username, email, password_hash FROM users WHERE email = ?",
                    (email,)
                )
                user_data = cursor.fetchone()
                
                if user_data and bcrypt.checkpw(password.encode('utf-8'), user_data[3].encode('utf-8')):
                    return {
                        "id": user_data[0],
                        "username": user_data[1],
                        "email": user_data[2]
                    }
                
                return None
        except Exception as e:
            print(f"Error authenticating user: {e}")
            return None
    
    def get_user_by_username(self, username: str) -> Optional[dict]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    "SELECT id, username, email FROM users WHERE username = ?",
                    (username,)
                )
                user_data = cursor.fetchone()
                
                if user_data:
                    return {
                        "id": user_data[0],
                        "username": user_data[1],
                        "email": user_data[2]
                    }
                return None
        except Exception as e:
            print(f"Error getting user by username: {e}")
            return None
        
    def export_user_accounts(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT id, username, email, created_at FROM users")
                accounts = []
                
                for row in cursor.fetchall():
                    accounts.append({
                        "id": row[0],
                        "username": row[1],
                        "email": row[2],
                        "created_at": row[3]
                    })
                
                with open("user_accounts.json", "w") as f:
                    json.dump(accounts, f, indent=2)
                
                return accounts
        except Exception as e:
            print(f"Error exporting accounts: {e}")
            return []