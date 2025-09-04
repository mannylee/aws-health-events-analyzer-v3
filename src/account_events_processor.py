"""
Account Events Processor Lambda Function
Processes events for a specific account and prepares them for Bedrock analysis
"""
import json
import os
import traceback
from datetime import datetime


def lambda_handler(event, context):
    """
    Process events for a specific account
    """
    try:
        print(f"Processing account events: {json.dumps(event, default=str)}")
        
        workflow_id = event['workflow_id']
        account_id = event['account_id']
        events = event['events']
        email_mapping = event.get('email_mapping')
        total_accounts = event.get('total_accounts')  # Get total accounts for completion tracking
        
        print(f"Processing {len(events)} events for account {account_id}")
        
        # Apply business logic and filters
        processed_events = process_account_events(events, account_id)
        
        # Prepare for Bedrock analysis
        bedrock_payload = prepare_bedrock_payload(processed_events, account_id)
        
        result = {
            'workflow_id': workflow_id,
            'account_id': account_id,
            'email_mapping': email_mapping,
            'processed_events': processed_events,
            'bedrock_payload': bedrock_payload,
            'event_count': len(processed_events),
            'total_accounts': total_accounts,  # Pass through for completion tracking
            'timestamp': datetime.now().isoformat()
        }
        
        print(f"Processed {len(processed_events)} events for account {account_id}")
        return result
        
    except Exception as e:
        print(f"Error processing account events: {str(e)}")
        traceback.print_exc()
        raise


def process_account_events(events, account_id):
    """
    Apply business logic and filtering to account events
    """
    processed_events = []
    excluded_services = get_excluded_services()
    
    for event in events:
        # Skip excluded services
        service = event.get('service', '')
        if service in excluded_services:
            print(f"Skipping event for excluded service: {service}")
            continue
        
        # Add processing metadata
        processed_event = event.copy()
        processed_event['processed_at'] = datetime.now().isoformat()
        processed_event['processor'] = 'account_events_processor'
        
        # Debug: Check if eventDescription is present
        if not processed_event.get('eventDescription'):
            print(f"Warning: Event {event.get('arn', 'unknown')} has no eventDescription")
        
        # Categorize event severity based on status and type
        processed_event['severity'] = categorize_event_severity(event)
        
        # Extract key information for analysis
        processed_event['analysis_summary'] = extract_analysis_summary(event)
        
        processed_events.append(processed_event)
    
    # Sort by severity (critical first)
    severity_order = {'critical': 0, 'high': 1, 'medium': 2, 'low': 3}
    processed_events.sort(key=lambda x: severity_order.get(x.get('severity', 'low'), 3))
    
    return processed_events


def categorize_event_severity(event):
    """
    Categorize event severity based on type and status
    """
    event_type_category = event.get('eventTypeCategory', '').lower()
    status_code = event.get('statusCode', '').lower()
    service = event.get('service', '').lower()
    
    # Critical events
    if event_type_category == 'issue' and status_code == 'open':
        return 'critical'
    
    # High priority events
    if event_type_category in ['issue', 'investigation']:
        return 'high'
    
    # Medium priority for scheduled changes
    if event_type_category == 'scheduledchange':
        return 'medium'
    
    # Account notifications are generally low unless they're security related
    if event_type_category == 'accountnotification':
        event_type_code = event.get('eventTypeCode', '').lower()
        if any(keyword in event_type_code for keyword in ['security', 'billing', 'limit']):
            return 'medium'
        return 'low'
    
    return 'low'


def extract_analysis_summary(event):
    """
    Extract key information for Bedrock analysis
    """
    return {
        'service': event.get('service', 'Unknown'),
        'region': event.get('region', 'Unknown'),
        'event_type': event.get('eventTypeCode', 'Unknown'),
        'category': event.get('eventTypeCategory', 'Unknown'),
        'status': event.get('statusCode', 'Unknown'),
        'start_time': event.get('startTime', ''),
        'end_time': event.get('endTime', ''),
        'description_preview': (event.get('eventDescription', '') or '')[:200] + '...' if event.get('eventDescription') else 'No description available'
    }


def prepare_bedrock_payload(events, account_id):
    """
    Prepare payload for Bedrock analysis
    """
    if not events:
        return None
    
    # Create summary for Bedrock
    event_summary = {
        'account_id': account_id,
        'total_events': len(events),
        'events_by_severity': {},
        'events_by_service': {},
        'events_by_category': {},
        'critical_events': [],
        'sample_events': events[:5]  # First 5 events for detailed analysis
    }
    
    # Count by severity
    for event in events:
        severity = event.get('severity', 'low')
        event_summary['events_by_severity'][severity] = event_summary['events_by_severity'].get(severity, 0) + 1
        
        service = event.get('service', 'Unknown')
        event_summary['events_by_service'][service] = event_summary['events_by_service'].get(service, 0) + 1
        
        category = event.get('eventTypeCategory', 'Unknown')
        event_summary['events_by_category'][category] = event_summary['events_by_category'].get(category, 0) + 1
        
        # Collect critical events
        if severity == 'critical':
            event_summary['critical_events'].append({
                'arn': event.get('arn', ''),
                'service': event.get('service', ''),
                'event_type': event.get('eventTypeCode', ''),
                'description': event.get('eventDescription', '')[:500] if event.get('eventDescription') else 'No description'
            })
    
    # Create analysis prompt
    prompt = create_analysis_prompt(event_summary)
    
    return {
        'account_id': account_id,
        'event_summary': event_summary,
        'analysis_prompt': prompt,
        'timestamp': datetime.now().isoformat()
    }


def create_analysis_prompt(event_summary):
    """
    Create enhanced outage-focused prompt for Bedrock analysis
    """
    account_id = event_summary['account_id']
    total_events = event_summary['total_events']
    
    # Get the most critical event for detailed analysis
    primary_event = None
    if event_summary['critical_events']:
        primary_event = event_summary['critical_events'][0]
    elif event_summary['sample_events']:
        primary_event = event_summary['sample_events'][0]
    
    prompt = f"""You are an AWS expert specializing in outage analysis and business continuity. Your task is to analyze these AWS Health events and determine their potential impact on workload availability, system connectivity, and service outages.

ACCOUNT: {account_id}

EVENT SUMMARY:
- Total Events: {total_events}
- Events by Severity: {json.dumps(event_summary['events_by_severity'], indent=2)}
- Events by Service: {json.dumps(event_summary['events_by_service'], indent=2)}
- Events by Category: {json.dumps(event_summary['events_by_category'], indent=2)}

CRITICAL EVENTS REQUIRING IMMEDIATE ATTENTION:
{json.dumps(event_summary['critical_events'], indent=2) if event_summary['critical_events'] else 'None identified'}

SAMPLE EVENTS FOR DETAILED ANALYSIS:
{json.dumps([e.get('analysis_summary', {}) for e in event_summary['sample_events']], indent=2)}

IMPORTANT ANALYSIS FOCUS:
1. Will these events cause workload downtime if required actions are not taken?
2. Will there be any service outages associated with these events?
3. Will applications/workloads experience network integration issues between connecting systems?
4. What specific AWS services or resources could be impacted?
5. Are there any SSL/TLS certificate issues that could affect connectivity?

CRITICAL EVENT CRITERIA:
- Any event that will cause service downtime should be marked as CRITICAL
- Any event that will cause network integration or SSL issues between systems should be marked as CRITICAL
- Any event that requires immediate action to prevent outage should be marked as URGENT time sensitivity
- Events with high impact but no immediate downtime should be marked as HIGH risk level

Please analyze these events and provide the following information in JSON format:

{{
    "critical": boolean,
    "risk_level": "Critical|High|Medium|Low",
    "account_impact": "Critical|High|Medium|Low", 
    "time_sensitivity": "Critical|Urgent|Routine",
    "risk_category": "Availability|Security|Performance|Cost|Compliance",
    "required_actions": "string describing specific actions needed",
    "impact_analysis": "string describing potential outages and connectivity issues",
    "consequences_if_ignored": "string describing what outages will occur if not addressed",
    "affected_resources": "string listing specific resources that could be impacted",
    "key_date": "YYYY-MM-DD or null",
    "business_impact": "assessment of business impact including potential downtime",
    "summary": "brief executive summary focusing on outage risk"
}}

IMPORTANT GUIDELINES:
- In your impact_analysis field, be very specific about:
  1. Potential outages and their estimated duration
  2. Connectivity issues between systems  
  3. Whether this will cause downtime if actions are not taken
  4. SSL/TLS certificate expiration impacts

- In your consequences_if_ignored field, clearly state what outages or disruptions will occur if the events are not addressed

- For the key_date field:
  * Analyze event descriptions for any dates that customers need to be aware of
  * Look for dates when actions must be taken, when changes will occur, or when impacts will begin
  * Return the EARLIEST date that will impact the customer in YYYY-MM-DD format
  * If no specific date is mentioned or can be determined, return null
  * Common patterns: deadlines, maintenance windows, deprecation dates, end-of-life dates, migration deadlines

RISK LEVEL GUIDELINES:
- CRITICAL: Will cause service outage or severe disruption if not addressed
- HIGH: Significant impact but not an immediate outage  
- MEDIUM: Moderate impact requiring attention
- LOW: Minimal impact, routine maintenance

Ensure your response is valid JSON that can be parsed programmatically."""
    
    return prompt.strip()


def get_excluded_services():
    """
    Get list of excluded services from environment
    """
    excluded_services_str = os.environ.get('EXCLUDED_SERVICES', '')
    return [s.strip() for s in excluded_services_str.split(',') if s.strip()]