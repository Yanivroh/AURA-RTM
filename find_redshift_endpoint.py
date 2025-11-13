import boto3
import json
from botocore.exceptions import ClientError

def get_redshift_serverless_endpoints():
    """Find all Redshift Serverless workgroups and their endpoints"""
    try:
        # Initialize boto3 client
        client = boto3.client('redshift-serverless')
        
        print("🔍 Searching for Redshift Serverless workgroups...")
        
        # List all workgroups
        workgroups = client.list_workgroups()['workgroups']
        
        if not workgroups:
            print("No Redshift Serverless workgroups found in this region.")
            return
            
        print(f"\nFound {len(workgroups)} workgroup(s):")
        print("-" * 80)
        
        for i, workgroup in enumerate(workgroups, 1):
            print(f"\n{i}. Workgroup: {workgroup['workgroupName']}")
            print(f"   Status: {workgroup.get('status', 'N/A')}")
            
            # Get the endpoint details
            if 'endpoint' in workgroup and workgroup['endpoint']:
                endpoint = workgroup['endpoint']
                print(f"   Endpoint: {endpoint.get('address', 'N/A')}")
                print(f"   Port: {endpoint.get('port', 'N/A')}")
                print(f"   VPC: {endpoint.get('vpcEndpoints', [{}])[0].get('vpcEndpointId', 'N/A')}")
            else:
                print("   No active endpoint found for this workgroup")
            
            # Get the namespace details
            namespace_id = workgroup.get('namespaceName')
            if namespace_id:
                try:
                    namespace = client.get_namespace(namespaceName=namespace_id)
                    print(f"   Namespace: {namespace.get('namespaceName', 'N/A')}")
                    print(f"   Database: {namespace.get('defaultDatabaseName', 'N/A')}")
                except ClientError as e:
                    print(f"   Could not retrieve namespace details: {e}")
            
            print("-" * 80)
            
    except ClientError as e:
        print(f"Error: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure you have AWS credentials configured (run 'aws configure' if needed)")
        print("2. Verify your IAM user has permissions to access Redshift Serverless")
        print("3. Check if you're in the correct AWS region")
        print("4. Ensure you have the AWS CLI installed (pip install boto3)")

if __name__ == "__main__":
    print("🔎 Redshift Serverless Endpoint Finder")
    print("=" * 80)
    get_redshift_serverless_endpoints()
    
    print("\nTo use with your dashboard:")
    print("1. Update your .env file with the endpoint and port shown above")
    print("2. Make sure your IP is whitelisted in the security group")
    print("3. Ensure your IAM user has the correct permissions")
