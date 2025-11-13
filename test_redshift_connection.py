import os
import psycopg2
from dotenv import load_dotenv

def test_connection():
    """Test connection to Redshift with detailed error reporting"""
    # Load environment variables
    load_dotenv()
    
    # Get connection parameters
    host = os.getenv("REDSHIFT_HOST")
    dbname = os.getenv("REDSHIFT_DB")
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASS")
    port = os.getenv("REDSHIFT_PORT", "5439")  # Default to 5439 if not set
    
    print("🔍 Testing Redshift connection with these parameters:")
    print(f"   Host: {host}")
    print(f"   Port: {port}")
    print(f"   Database: {dbname}")
    print(f"   User: {user}")
    print(f"   Password: {'*' * 8 if password else 'Not set'}")
    
    # Try different ports if needed
    ports_to_try = [int(port), 5439, 8200, 8201, 8202, 8203, 8204, 8205, 8206, 8207, 8208, 8209, 8210]
    
    for test_port in ports_to_try:
        print(f"\n🔄 Trying to connect on port {test_port}...")
        try:
            conn = psycopg2.connect(
                host=host,
                database=dbname,
                user=user,
                password=password,
                port=test_port,
                connect_timeout=5,
                sslmode='require'
            )
            
            # Test the connection
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(), current_user, version()")
                result = cur.fetchone()
                print("✅ Connection successful!")
                print(f"   Database: {result[0]}")
                print(f"   User: {result[1]}")
                print(f"   Version: {result[2]}")
                return True
                
        except psycopg2.OperationalError as e:
            print(f"❌ Connection failed on port {test_port}: {str(e)}")
            continue
        except Exception as e:
            print(f"❌ Unexpected error on port {test_port}: {str(e)}")
            continue
        finally:
            if 'conn' in locals():
                conn.close()
    
    print("\n❌ Could not establish a connection with any of the tested ports.")
    print("Please verify:")
    print("1. The Redshift cluster is running and accessible")
    print("2. Your IP is whitelisted in the security group")
    print("3. The correct port is being used (check AWS Redshift Console)")
    print("4. The security group allows inbound traffic on the correct port")
    return False

if __name__ == "__main__":
    test_connection()
