"""
Shared utilities for AWS Health Events Analyzer microservices
"""
import boto3
import json
import os
import time
import traceback
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from botocore.exceptions import ClientError


class IncompleteWorkflowException(Exception):
    """Custom exception for incomplete workflows"""
    pass


def generate_workflow_id():
    """Generate a unique workflow ID"""
    return f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}-{int(time.time())}"


def get_organization_account_email_mapping():
    """
    Get account-to-email mapping from AWS Organizations by looking up account contact information.
    
    Returns:
        tuple: (account_to_email_mapping, email_to_accounts_mapping)
    """
    account_to_email = {}
    email_to_accounts = defaultdict(list)
    
    use_org_mapping = os.environ.get('USE_ORGANIZATION_ACCOUNT_EMAIL_MAPPING', 'false').lower() == 'true'
    if not use_org_mapping:
        print("Organization account email mapping is disabled")
        return account_to_email, dict(email_to_accounts)
    
    try:
        print("Fetching account email mappings from AWS Organizations...")
        
        # Create Organizations client
        org_client = boto3.client('organizations')
        
        # List all accounts in the organization
        paginator = org_client.get_paginator('list_accounts')
        
        for page in paginator.paginate():
            for account in page['Accounts']:
                account_id = account['Id']
                account_name = account.get('Name', 'Unknown')
                account_email = account.get('Email')
                
                if account_email:
                    account_to_email[account_id] = account_email
                    email_to_accounts[account_email].append(account_id)
                    print(f"Account mapping: {account_id} ({account_name}) -> {account_email}")
                else:
                    print(f"Warning: No email found for account {account_id} ({account_name})")
        
        print(f"Retrieved {len(account_to_email)} account email mappings from Organizations")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'AccessDeniedException':
            print("Error: Access denied when trying to access AWS Organizations. Ensure the Lambda has organizations:ListAccounts permissions.")
        elif error_code == 'AWSOrganizationsNotInUseException':
            print("Error: AWS Organizations is not enabled for this account.")
        else:
            print(f"Error accessing AWS Organizations: {error_code} - {e.response['Error']['Message']}")
    except Exception as e:
        print(f"Error fetching account mappings from Organizations: {str(e)}")
        traceback.print_exc()
    
    return account_to_email, dict(email_to_accounts)


def get_custom_account_email_mapping_from_dynamodb():
    """
    Fetch custom account-email mapping from DynamoDB table
    
    Returns:
        tuple: (account_to_email_mapping, email_to_accounts_mapping)
    """
    use_custom_mapping = os.environ.get('USE_CUSTOM_ACCOUNT_EMAIL_MAPPING', 'false').lower() == 'true'
    if not use_custom_mapping:
        print("Custom account email mapping is disabled")
        return {}, {}
    
    table_name = os.environ.get('ACCOUNT_EMAIL_MAPPING_TABLE', '')
    if not table_name:
        print("Custom mapping is enabled but no DynamoDB table name configured")
        return {}, {}
    
    try:
        print(f"Fetching custom account email mapping from DynamoDB table: {table_name}")
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        
        # Scan the table to get all mappings
        response = table.scan()
        items = response['Items']
        
        # Handle pagination if there are many items
        while 'LastEvaluatedKey' in response:
            print("Fetching additional items from DynamoDB (pagination)...")
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        print(f"Successfully loaded {len(items)} account mappings from DynamoDB")
        
        # Convert to both formats for compatibility with existing code
        account_to_email = {}
        email_to_accounts = defaultdict(list)
        
        for item in items:
            account_id = item.get('AccountId', '').strip()
            email = item.get('Email', '').strip()
            
            if not account_id or not email:
                print(f"Warning: Skipping invalid mapping - AccountId: '{account_id}', Email: '{email}'")
                continue
            
            account_to_email[account_id] = email
            email_to_accounts[email].append(account_id)
            print(f"Custom mapping: {account_id} -> {email}")
        
        return account_to_email, dict(email_to_accounts)
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFoundException':
            print(f"DynamoDB table {table_name} not found")
        elif error_code == 'AccessDeniedException':
            print(f"Access denied when trying to read DynamoDB table {table_name}")
        else:
            print(f"Error accessing DynamoDB table: {error_code} - {e.response['Error']['Message']}")
        return {}, {}
    except Exception as e:
        print(f"Error fetching custom mapping from DynamoDB: {str(e)}")
        traceback.print_exc()
        return {}, {}


def get_combined_account_email_mapping():
    """
    Get account-email mapping using hybrid approach:
    1. Start with AWS Organizations mapping (if enabled)
    2. Override with custom DynamoDB mapping (if enabled)
    3. DynamoDB takes precedence for accounts that exist in both sources
    
    Returns:
        tuple: (account_to_email_mapping, email_to_accounts_mapping, mapping_sources)
    """
    use_custom = os.environ.get('USE_CUSTOM_ACCOUNT_EMAIL_MAPPING', 'false').lower() == 'true'
    use_org = os.environ.get('USE_ORGANIZATION_ACCOUNT_EMAIL_MAPPING', 'false').lower() == 'true'
    
    print(f"Account email mapping configuration:")
    print(f"  - Custom DynamoDB mapping: {'ENABLED' if use_custom else 'DISABLED'}")
    print(f"  - Organizations mapping: {'ENABLED' if use_org else 'DISABLED'}")
    
    # Initialize combined mappings
    combined_account_to_email = {}
    combined_email_to_accounts = defaultdict(list)
    mapping_sources = {}  # Track source for each account
    
    # Step 1: Get Organizations mapping as base (if enabled)
    if use_org:
        print("Fetching AWS Organizations mapping as base...")
        org_account_to_email, org_email_to_accounts = get_organization_account_email_mapping()
        
        if org_account_to_email:
            combined_account_to_email.update(org_account_to_email)
            for email, accounts in org_email_to_accounts.items():
                combined_email_to_accounts[email].extend(accounts)
            # Mark all as organizations source
            for account_id in org_account_to_email:
                mapping_sources[account_id] = 'organizations'
            print(f"Added {len(org_account_to_email)} accounts from Organizations mapping")
    
    # Step 2: Override with DynamoDB mapping (if enabled)
    if use_custom:
        print("Fetching custom DynamoDB mapping for overrides...")
        custom_account_to_email, custom_email_to_accounts = get_custom_account_email_mapping_from_dynamodb()
        
        if custom_account_to_email:
            # Track overrides for logging
            overridden_accounts = []
            new_accounts = []
            
            for account_id, new_email in custom_account_to_email.items():
                old_email = combined_account_to_email.get(account_id)
                
                if old_email and old_email != new_email:
                    # This account exists in Organizations but with different email
                    overridden_accounts.append(f"{account_id}: {old_email} -> {new_email}")
                    mapping_sources[account_id] = 'combined'  # Organizations overridden by custom
                    # Remove from old email list
                    if old_email in combined_email_to_accounts:
                        combined_email_to_accounts[old_email] = [
                            acc for acc in combined_email_to_accounts[old_email] if acc != account_id
                        ]
                        # Clean up empty email lists
                        if not combined_email_to_accounts[old_email]:
                            del combined_email_to_accounts[old_email]
                elif not old_email:
                    # This is a new account not in Organizations
                    new_accounts.append(f"{account_id} -> {new_email}")
                    mapping_sources[account_id] = 'custom'
                else:
                    # Same email in both sources
                    mapping_sources[account_id] = 'organizations'  # Keep as organizations since it's the same
                
                # Apply the DynamoDB mapping
                combined_account_to_email[account_id] = new_email
                if account_id not in combined_email_to_accounts[new_email]:
                    combined_email_to_accounts[new_email].append(account_id)
            
            print(f"Added {len(custom_account_to_email)} accounts from DynamoDB mapping")
            
            if overridden_accounts:
                print(f"DynamoDB overrode {len(overridden_accounts)} Organizations mappings")
            
            if new_accounts:
                print(f"DynamoDB added {len(new_accounts)} new accounts not in Organizations")
    
    # Convert defaultdict back to regular dict
    combined_email_to_accounts = dict(combined_email_to_accounts)
    
    # Summary logging
    if combined_account_to_email:
        print(f"Final combined mapping: {len(combined_account_to_email)} accounts across {len(combined_email_to_accounts)} email addresses")
        return combined_account_to_email, combined_email_to_accounts, mapping_sources
    else:
        print("No account-email mapping configured - all events will go to default recipients")
        return {}, {}, {}


def expand_events_by_account(events):
    """
    Expands events that affect multiple accounts into separate event records for each account.
    Fetches affected accounts if not already specified.
    
    Args:
        events (list): List of event dictionaries
        
    Returns:
        list: Expanded list of event dictionaries
    """
    expanded_events = []
    
    # Health API should go to us-east-1
    health_client = boto3.client('health', region_name='us-east-1')
    
    for event in events:
        # Get the account ID string - check multiple possible sources
        account_id_str = event.get('accountId', '')
        
        # If no accountId, check affectedEntities (for CSV format)
        if not account_id_str:
            affected_entities = event.get('affectedEntities', [])
            if affected_entities and isinstance(affected_entities, list):
                # Extract account IDs from affected entities
                account_ids = []
                for entity in affected_entities:
                    if isinstance(entity, dict) and 'entityValue' in entity:
                        account_ids.append(entity['entityValue'])
                if account_ids:
                    account_id_str = ', '.join(account_ids)
        
        event_arn = event.get('arn', '')
        
        # If no account ID or it's N/A, try to fetch affected accounts
        if not account_id_str or account_id_str == 'N/A':
            try:
                print(f"Fetching affected accounts for event: {event.get('eventTypeCode', 'unknown')}")
                response = health_client.describe_affected_accounts_for_organization(
                    eventArn=event_arn
                )
                affected_accounts = response.get('affectedAccounts', [])
                
                if affected_accounts:
                    # If multiple accounts are affected, join them with commas
                    account_id_str = ', '.join(affected_accounts)
                    event['accountId'] = account_id_str  # Update the event with the account IDs
                    print(f"Found affected accounts: {account_id_str}")
                else:
                    print("No affected accounts found")
                    expanded_events.append(event)  # Keep the event as is
                    continue
            except Exception as e:
                print(f"Error fetching affected accounts: {str(e)}")
                expanded_events.append(event)  # Keep the event as is
                continue
        
        # If no comma in the string, it's a single account or none
        if ',' not in account_id_str:
            expanded_events.append(event)
            continue
        
        # Split the account IDs and create a separate event for each
        account_ids = [aid.strip() for aid in account_id_str.split(',')]
        print(f"Expanding event {event_arn} for {len(account_ids)} accounts: {account_ids}")
        
        for account_id in account_ids:
            # Create a copy of the event for this specific account
            account_event = event.copy()
            account_event['accountId'] = account_id
            expanded_events.append(account_event)
    
    print(f"Expanded {len(events)} events to {len(expanded_events)} account-specific events")
    return expanded_events


def group_events_by_account(events):
    """
    Group events by account ID
    
    Args:
        events (list): List of event dictionaries
        
    Returns:
        dict: Dictionary mapping account IDs to lists of events
    """
    events_by_account = defaultdict(list)
    
    for event in events:
        account_id = event.get('accountId', 'unknown')
        events_by_account[account_id].append(event)
    
    return dict(events_by_account)


def store_workflow_result(workflow_id, account_id, result_data, ttl_days=7):
    """
    Store workflow result in DynamoDB with TTL
    
    Args:
        workflow_id (str): Workflow identifier
        account_id (str): Account identifier
        result_data (dict): Result data to store
        ttl_days (int): TTL in days (default 7)
    """
    table_name = os.environ.get('WORKFLOW_RESULTS_TABLE')
    if not table_name:
        raise ValueError("WORKFLOW_RESULTS_TABLE environment variable not set")
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    # Calculate TTL timestamp
    ttl_timestamp = int(time.time()) + (ttl_days * 24 * 60 * 60)
    
    item = {
        'workflow_id': workflow_id,
        'account_id': account_id,
        'result_data': result_data,
        'status': 'COMPLETED',
        'timestamp': datetime.now().isoformat(),
        'ttl': ttl_timestamp
    }
    
    table.put_item(Item=item)
    print(f"Stored result for workflow {workflow_id}, account {account_id}")


def get_workflow_results(workflow_id):
    """
    Get all results for a workflow
    
    Args:
        workflow_id (str): Workflow identifier
        
    Returns:
        list: List of result items
    """
    table_name = os.environ.get('WORKFLOW_RESULTS_TABLE')
    if not table_name:
        raise ValueError("WORKFLOW_RESULTS_TABLE environment variable not set")
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    response = table.query(
        KeyConditionExpression='workflow_id = :wid',
        ExpressionAttributeValues={':wid': workflow_id}
    )
    
    return response['Items']


def count_completed_results(workflow_id):
    """
    Count completed results for a workflow
    
    Args:
        workflow_id (str): Workflow identifier
        
    Returns:
        int: Number of completed results
    """
    results = get_workflow_results(workflow_id)
    return len([r for r in results if r.get('status') == 'COMPLETED'])


def cleanup_workflow_results(workflow_id):
    """
    Clean up workflow results from DynamoDB
    
    Args:
        workflow_id (str): Workflow identifier
    """
    table_name = os.environ.get('WORKFLOW_RESULTS_TABLE')
    if not table_name:
        raise ValueError("WORKFLOW_RESULTS_TABLE environment variable not set")
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    # Get all items for this workflow
    response = table.query(
        KeyConditionExpression='workflow_id = :wid',
        ExpressionAttributeValues={':wid': workflow_id}
    )
    
    # Batch delete items
    with table.batch_writer() as batch:
        for item in response['Items']:
            batch.delete_item(
                Key={
                    'workflow_id': item['workflow_id'],
                    'account_id': item['account_id']
                }
            )
    
    print(f"Cleaned up {len(response['Items'])} items for workflow {workflow_id}")