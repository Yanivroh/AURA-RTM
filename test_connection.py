import os
import psycopg2
from dotenv import load_dotenv

def test_connection():
    """Test the Redshift connection"""
    load_dotenv()
    
    conn_params = {
        'host': os.getenv("REDSHIFT_HOST"),
        'database': os.getenv("REDSHIFT_DB"),
        'user': os.getenv("REDSHIFT_USER"),
        'password': os.getenv("REDSHIFT_PASS"),
        'port': int(os.getenv("REDSHIFT_PORT")),
        'sslmode': 'require',
        'connect_timeout': 10
    }
    
    print("Attempting to connect with parameters:")
    print(f"Host: {conn_params['host']}")
    print(f"Port: {conn_params['port']}")
    print(f"Database: {conn_params['database']}")
    print(f"User: {conn_params['user']}")
    
    try:
        print("\nTrying to establish connection...")
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        print("✓ Successfully connected to Redshift!")
        
        # Test a simple query
        cursor.execute("SELECT current_user, current_database(), version()")
        result = cursor.fetchone()
        print("\nConnection details:")
        print(f"User: {result[0]}")
        print(f"Database: {result[1]}")
        print(f"Redshift version: {result[2].split(',')[0]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print("\n✗ Connection failed with error:")
        print(f"Error: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Verify your Redshift cluster is running")
        print("2. Check if your IP is whitelisted in the Redshift security group")
        print("3. Verify the port number (common ports: 5439 for Redshift, 5432 for PostgreSQL)")
        print("4. Check your credentials and permissions")
        print("5. If using Redshift Serverless, ensure the workgroup is configured correctly")

if __name__ == "__main__":
    test_connection()
