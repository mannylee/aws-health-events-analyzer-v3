"""
Workflow Initializer Lambda Function
Fetches organization mappings and health events, then prepares account processing tasks
"""
import boto3
import json
import os
import traceback
import csv
import io
from datetime import datetime, timedelta, timezone
from shared_utils import (
    generate_workflow_id,
    get_combined_account_email_mapping,
    expand_events_by_account,
    group_events_by_account
)


def serialize_datetime_objects(obj):
    """
    Recursively convert datetime objects to ISO format strings
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: serialize_datetime_objects(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_datetime_objects(item) for item in obj]
    else:
        return obj


def lambda_handler(event, context):
    """
    Initialize the health events analysis workflow
    """
    try:
        print("Starting workflow initialization...")
        
        # Generate unique workflow ID
        workflow_id = generate_workflow_id()
        print(f"Generated workflow ID: {workflow_id}")
        
        # Get account email mappings (both org and custom)
        print("Fetching account email mappings...")
        account_to_email, email_to_accounts, mapping_sources = get_combined_account_email_mapping()
        
        # Get health events
        print("Fetching health events...")
        health_events = get_health_events()
        
        if not health_events:
            print("No health events found")
            result = {
                'workflow_id': workflow_id,
                'accounts': [],
                'total_accounts': 0,
                'email_mappings': account_to_email,
                'message': 'No health events to process'
            }
            return serialize_datetime_objects(result)
        
        # Expand events by account and group them
        print("Processing and grouping events by account...")
        expanded_events = expand_events_by_account(health_events)
        events_by_account = group_events_by_account(expanded_events)
        
        # Prepare account processing tasks
        accounts_to_process = []
        for account_id, events in events_by_account.items():
            if account_id == 'unknown':
                print(f"Skipping {len(events)} events with unknown account ID")
                continue
                
            accounts_to_process.append({
                'workflow_id': workflow_id,
                'account_id': account_id,
                'events': events,
                'email_mapping': account_to_email.get(account_id),
                'mapping_source': mapping_sources.get(account_id, 'none'),
                'event_count': len(events)
            })
        
        print(f"Prepared {len(accounts_to_process)} accounts for processing")
        print(f"Total events: {len(expanded_events)}")
        
        # Serialize datetime objects before returning
        result = {
            'workflow_id': workflow_id,
            'accounts': accounts_to_process,
            'total_accounts': len(accounts_to_process),
            'email_mappings': account_to_email,
            'total_events': len(expanded_events)
        }
        
        return serialize_datetime_objects(result)
        
    except Exception as e:
        print(f"Error in workflow initialization: {str(e)}")
        traceback.print_exc()
        raise


def get_health_events():
    """
    Fetch health events from AWS Health API or S3 override
    """
    # Check for S3 override first
    s3_override_arn = os.environ.get('OVERRIDE_S3_HEALTH_EVENTS_ARN', '')
    if s3_override_arn:
        print(f"Using S3 override: {s3_override_arn}")
        return get_health_events_from_s3(s3_override_arn)
    
    # Otherwise use Health API
    return get_health_events_from_api()


def parse_csv_health_events(csv_content):
    """
    Parse CSV content and convert to health events format
    """
    try:
        # Parse CSV content
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        events = []
        
        for row in csv_reader:
            # Convert CSV row to health event format
            # Adjust field mappings based on your CSV structure
            event = {
                'arn': row.get('arn', ''),
                'service': row.get('service', ''),
                'eventTypeCode': row.get('eventTypeCode', ''),
                'eventTypeCategory': row.get('eventTypeCategory', ''),
                'region': row.get('region', ''),
                'startTime': row.get('startTime', ''),
                'endTime': row.get('endTime', ''),
                'lastUpdatedTime': row.get('lastUpdatedTime', ''),
                'statusCode': row.get('statusCode', ''),
                'eventDescription': row.get('eventDescription', '') or row.get('description', '') or row.get('Description', '') or row.get('message', ''),
                'affectedEntities': []
            }
            
            # Add affected account if present - try multiple possible column names
            affected_account = row.get('affectedAccount') or row.get('accountId') or row.get('account_id')
            if affected_account:
                event['accountId'] = affected_account
                event['affectedEntities'] = [{'entityValue': affected_account}]
            
            events.append(event)
        
        return events
        
    except Exception as e:
        print(f"Error parsing CSV content: {str(e)}")
        raise


def get_health_events_from_s3(s3_arn):
    """
    Get health events from S3 file
    """
    try:
        # Parse S3 ARN or bucket/key path
        if s3_arn.startswith('arn:aws:s3:::'):
            # Proper ARN format: arn:aws:s3:::bucket/key
            s3_path = s3_arn[13:]  # Remove 'arn:aws:s3:::'
            bucket_name, key = s3_path.split('/', 1)
        else:
            # Assume bucket/key format: bucket-name/key-path
            if '/' not in s3_arn:
                raise ValueError(f"Invalid S3 path format: {s3_arn}. Expected 'bucket/key' or 'arn:aws:s3:::bucket/key'")
            bucket_name, key = s3_arn.split('/', 1)
        
        print(f"Downloading health events from S3: bucket={bucket_name}, key={key}")
        
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        
        # Determine file type and parse accordingly
        if key.lower().endswith('.csv'):
            # Handle CSV file
            content = response['Body'].read().decode('utf-8-sig')  # Handle BOM
            events = parse_csv_health_events(content)
            print(f"Loaded {len(events)} events from CSV file")
        else:
            # Handle JSON file
            content = response['Body'].read().decode('utf-8-sig')  # Handle BOM
            data = json.loads(content)
            # Extract events from the JSON structure
            events = data.get('events', [])
            print(f"Loaded {len(events)} events from JSON file")
        
        return events
        
    except Exception as e:
        print(f"Error loading health events from S3: {str(e)}")
        traceback.print_exc()
        raise


def get_health_events_from_api():
    """
    Get health events from AWS Health API
    """
    try:
        # Health API client (must be us-east-1)
        health_client = boto3.client('health', region_name='us-east-1')
        
        # Calculate date range - look from ANALYSIS_WINDOW_DAYS ago into the future
        analysis_window_days = int(os.environ.get('ANALYSIS_WINDOW_DAYS', '8'))
        start_time = datetime.now(timezone.utc) - timedelta(days=analysis_window_days)
        # No end_time cap - look into the future indefinitely
        
        print(f"Fetching health events from {start_time.isoformat()} onwards (no end date)")
        
        # Build filter
        event_filter = {
            'startTime': {
                'from': start_time
                # No 'to' field - look into the future indefinitely
            },
            'eventStatusCodes': ['open', 'upcoming']  # Only actionable events
        }
        
        # Add event categories filter if specified
        event_categories = os.environ.get('EVENT_CATEGORIES', '')
        if event_categories:
            categories = [cat.strip() for cat in event_categories.split(',') if cat.strip()]
            if categories:
                event_filter['eventTypeCategories'] = categories
                print(f"Filtering by categories: {categories}")
        
        # Add excluded services filter
        excluded_services = os.environ.get('EXCLUDED_SERVICES', '')
        if excluded_services:
            excluded = [svc.strip() for svc in excluded_services.split(',') if svc.strip()]
            if excluded:
                print(f"Will exclude services: {excluded}")
        
        # Fetch events using organization view
        print("Fetching events for organization...")
        paginator = health_client.get_paginator('describe_events_for_organization')
        
        all_events = []
        for page in paginator.paginate(filter=event_filter):
            events = page.get('events', [])
            
            # Filter out excluded services
            if excluded_services:
                excluded = [svc.strip() for svc in excluded_services.split(',') if svc.strip()]
                events = [e for e in events if e.get('service', '') not in excluded]
            
            all_events.extend(events)
        
        print(f"Fetched {len(all_events)} health events from API")
        
        # Get event details for each event
        if all_events:
            print("Fetching event details...")
            all_events = get_event_details(health_client, all_events)
        
        return all_events
        
    except Exception as e:
        print(f"Error fetching health events from API: {str(e)}")
        traceback.print_exc()
        raise


def get_event_details(health_client, events):
    """
    Get detailed information for health events
    """
    try:
        # Batch process event details (max 10 at a time)
        batch_size = 10
        detailed_events = []
        
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            event_arns = [event['arn'] for event in batch]
            
            try:
                # Get event details
                details_response = health_client.describe_event_details_for_organization(
                    eventArns=event_arns
                )
                
                # Create a mapping of ARN to details
                details_map = {}
                for detail in details_response.get('successfulSet', []):
                    event_arn = detail['event']['arn']
                    details_map[event_arn] = detail
                
                # Merge details with events
                for event in batch:
                    event_arn = event['arn']
                    if event_arn in details_map:
                        detail = details_map[event_arn]
                        # Add description from event details
                        event_description = detail.get('eventDescription', {})
                        if event_description and 'latestDescription' in event_description:
                            event['eventDescription'] = event_description['latestDescription']
                    
                    detailed_events.append(event)
                    
            except Exception as e:
                print(f"Error getting details for batch: {str(e)}")
                # Add events without details
                detailed_events.extend(batch)
        
        print(f"Retrieved details for {len(detailed_events)} events")
        return detailed_events
        
    except Exception as e:
        print(f"Error in get_event_details: {str(e)}")
        return events  # Return original events if details fetch fails