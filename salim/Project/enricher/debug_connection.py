import os
import psycopg2
from dotenv import load_dotenv
from urllib.parse import quote_plus

def debug_connection():
    """Debug the database connection issue"""
    
    # Load environment variables
    try:
        load_dotenv('../.env')
    except UnicodeDecodeError as e:
        print(f"Error reading .env file: {e}")
        print("The .env file may be corrupted or saved in wrong encoding")
        print("Please ensure the .env file is saved as UTF-8")
        return
    except FileNotFoundError:
        print("No .env file found")
        print("Please create a .env file with your DATABASE_URL")
        return
    
    # Get database URL
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("DATABASE_URL not found in .env file")
        return
    
    print("Debugging connection...")
    print(f"Original URL: {database_url}")
    
    # Try to parse the URL to see what's wrong
    try:
        # Extract parts manually
        if database_url.startswith('postgresql://'):
            parts = database_url.replace('postgresql://', '').split('@')
            if len(parts) == 2:
                credentials = parts[0]
                host_part = parts[1]
                
                if ':' in credentials:
                    username, password = credentials.split(':', 1)
                    print(f"Username: {username}")
                    print(f"Password: {'*' * len(password)}")
                    
                    # Check if password needs URL encoding
                    if any(char in password for char in ['/', ':', '@', '?', '#', '[', ']', '%']):
                        print("Password contains special characters that need URL encoding")
                        encoded_password = quote_plus(password)
                        print(f"Encoded password: {encoded_password}")
                        
                        # Create new URL with encoded password
                        new_url = f"postgresql://{username}:{encoded_password}@{host_part}"
                        print(f"New URL: {new_url}")
                        
                        # Test the new URL
                        print("Testing encoded URL...")
                        try:
                            conn = psycopg2.connect(new_url)
                            print("Connection successful with encoded password!")
                            conn.close()
                            return
                        except Exception as e:
                            print(f"Encoded URL failed: {e}")
                    
                    print(f"Host part: {host_part}")
                else:
                    print("Invalid credentials format")
            else:
                print("Invalid URL format")
        else:
            print("URL doesn't start with postgresql://")
            
    except Exception as e:
        print(f"Error parsing URL: {e}")
    
    # Try original URL
    print("Testing original URL...")
    try:
        conn = psycopg2.connect(database_url)
        print("Connection successful!")
        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}")
        print("Try URL-encoding your password if it contains special characters")

if __name__ == "__main__":
    debug_connection()
