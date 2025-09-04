"""
Bedrock Analyzer Lambda Function
Handles AI analysis of events using Amazon Bedrock with rate limiting
"""
import boto3
import json
import os
import re
import time
import traceback
from datetime import datetime
from botocore.exceptions import ClientError
from shared_utils import store_workflow_result


def lambda_handler(event, context):
    """
    Process Bedrock analysis from SQS queue with graceful failure handling
    """
    successful_messages = []
    failed_messages = []
    
    for record in event['Records']:
        message = None
        try:
            message = json.loads(record['body'])
            account_id = message.get('account_id', 'unknown')
            
            print(f"Processing Bedrock analysis for account {account_id}")
            
            # Get message attributes to track retry count
            retry_count = get_message_retry_count(record)
            max_retries = 3  # Maximum number of retries before using placeholder
            
            if retry_count >= max_retries:
                print(f"Message for account {account_id} has exceeded {max_retries} retries. Using placeholder analysis.")
                analysis_result = create_placeholder_analysis(message)
                analysis_result['bedrock_failure'] = True
                analysis_result['retry_count'] = retry_count
                analysis_result['failure_reason'] = 'Exceeded maximum retry attempts'
            else:
                # Check rate limit before processing
                if not check_bedrock_rate_limit():
                    print(f"Rate limit exceeded - message will be retried (attempt {retry_count + 1})")
                    # Only retry if we haven't exceeded max attempts
                    if retry_count < max_retries - 1:
                        failed_messages.append({
                            'itemIdentifier': record['receiptHandle']
                        })
                        continue
                    else:
                        # Max retries reached due to rate limiting - create placeholder
                        print(f"Max retries reached due to rate limiting for account {account_id}. Using placeholder analysis.")
                        analysis_result = create_placeholder_analysis(message)
                        analysis_result['bedrock_failure'] = True
                        analysis_result['retry_count'] = retry_count
                        analysis_result['failure_reason'] = 'Rate limit exceeded after maximum retries'
                else:
                    # Process with Bedrock - wrap in try-catch to handle any Bedrock failures
                    try:
                        analysis_result = analyze_with_bedrock(message)
                        analysis_result['bedrock_failure'] = False
                        analysis_result['retry_count'] = retry_count
                    except Exception as bedrock_error:
                        print(f"Bedrock analysis failed for account {account_id}: {str(bedrock_error)}")
                        # Create placeholder analysis if Bedrock fails
                        analysis_result = create_placeholder_analysis(message)
                        analysis_result['bedrock_failure'] = True
                        analysis_result['retry_count'] = retry_count
                        analysis_result['failure_reason'] = f'Bedrock analysis failed: {str(bedrock_error)}'
            
            # Store result in DynamoDB (always store, even placeholder)
            store_workflow_result(
                workflow_id=message['workflow_id'],
                account_id=account_id,
                result_data={
                    'analysis': analysis_result,
                    'email_mapping': message.get('email_mapping'),
                    'processed_events': message.get('processed_events', []),
                    'event_count': message.get('event_count', 0)
                }
            )
            
            # Note: Email queue triggering is handled by Step Functions workflow
            # The email queue manager will be triggered after all accounts are processed
            
            successful_messages.append(record['receiptHandle'])
            
            if analysis_result.get('bedrock_failure'):
                print(f"Stored placeholder analysis for account {account_id} after {retry_count} failed attempts")
            else:
                print(f"Successfully analyzed events for account {account_id}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException':
                print(f"Bedrock throttling detected: {e}")
                update_rate_limit_backoff()
                
                # Check if we should use placeholder instead of retrying
                retry_count = get_message_retry_count(record)
                if retry_count >= 2:  # Use placeholder after 2 throttling attempts
                    print(f"Using placeholder analysis due to persistent throttling for account {message.get('account_id', 'unknown')}")
                    try:
                        analysis_result = create_placeholder_analysis(message)
                        analysis_result['bedrock_failure'] = True
                        analysis_result['failure_reason'] = 'Persistent Bedrock throttling'
                        
                        store_workflow_result(
                            workflow_id=message['workflow_id'],
                            account_id=message.get('account_id', 'unknown'),
                            result_data={
                                'analysis': analysis_result,
                                'email_mapping': message.get('email_mapping'),
                                'processed_events': message.get('processed_events', []),
                                'event_count': message.get('event_count', 0)
                            }
                        )
                        successful_messages.append(record['receiptHandle'])
                        print(f"Stored placeholder analysis due to throttling for account {message.get('account_id', 'unknown')}")
                    except Exception as placeholder_error:
                        print(f"Failed to create placeholder analysis: {placeholder_error}")
                        # Even if placeholder creation fails, don't retry the message indefinitely
                        successful_messages.append(record['receiptHandle'])
                        print(f"Marked message as processed to prevent infinite retry")
                else:
                    # Only retry if we haven't hit the limit
                    failed_messages.append({
                        'itemIdentifier': record['receiptHandle']
                    })
            else:
                print(f"Bedrock error: {e}")
                # For non-throttling errors, create placeholder immediately
                try:
                    if message:
                        analysis_result = create_placeholder_analysis(message)
                        analysis_result['bedrock_failure'] = True
                        analysis_result['failure_reason'] = f"Bedrock error: {e.response['Error']['Code']}"
                        
                        store_workflow_result(
                            workflow_id=message['workflow_id'],
                            account_id=message.get('account_id', 'unknown'),
                            result_data={
                                'analysis': analysis_result,
                                'email_mapping': message.get('email_mapping'),
                                'processed_events': message.get('processed_events', []),
                                'event_count': message.get('event_count', 0)
                            }
                        )
                        successful_messages.append(record['receiptHandle'])
                        print(f"Stored placeholder analysis due to Bedrock error for account {message.get('account_id', 'unknown')}")
                    else:
                        failed_messages.append({
                            'itemIdentifier': record['receiptHandle']
                        })
                except Exception as placeholder_error:
                    print(f"Failed to create placeholder analysis: {placeholder_error}")
                    # Don't retry indefinitely - mark as processed to prevent infinite loop
                    successful_messages.append(record['receiptHandle'])
                    print(f"Marked message as processed to prevent infinite retry")
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            traceback.print_exc()
            
            # Try to create placeholder analysis for unexpected errors
            try:
                if message:
                    analysis_result = create_placeholder_analysis(message)
                    analysis_result['bedrock_failure'] = True
                    analysis_result['failure_reason'] = f"Unexpected error: {str(e)}"
                    
                    store_workflow_result(
                        workflow_id=message['workflow_id'],
                        account_id=message.get('account_id', 'unknown'),
                        result_data={
                            'analysis': analysis_result,
                            'email_mapping': message.get('email_mapping'),
                            'processed_events': message.get('processed_events', []),
                            'event_count': message.get('event_count', 0)
                        }
                    )
                    successful_messages.append(record['receiptHandle'])
                    print(f"Stored placeholder analysis due to unexpected error for account {message.get('account_id', 'unknown')}")
                else:
                    failed_messages.append({
                        'itemIdentifier': record['receiptHandle']
                    })
            except Exception as placeholder_error:
                print(f"Failed to create placeholder analysis: {placeholder_error}")
                # Don't retry indefinitely - mark as processed to prevent infinite loop
                successful_messages.append(record['receiptHandle'])
                print(f"Marked message as processed to prevent infinite retry")
    
    # Return partial batch failure info (SQS will retry failed messages)
    if failed_messages:
        return {
            'batchItemFailures': failed_messages
        }
    
    return {
        'status': 'success',
        'processed': len(successful_messages),
        'failed': len(failed_messages)
    }


def check_bedrock_rate_limit():
    """
    Check if we can make a Bedrock call based on current rate limit
    """
    try:
        table_name = os.environ.get('BEDROCK_RATE_LIMITER_TABLE')
        if not table_name:
            print("No rate limiter table configured, allowing call")
            return True
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        
        response = table.get_item(Key={'limiter_id': 'bedrock_calls'})
        
        if 'Item' not in response:
            # Initialize rate limiter
            table.put_item(Item={
                'limiter_id': 'bedrock_calls',
                'last_call_time': int(time.time()),
                'calls_this_minute': 0,
                'max_calls_per_minute': 20,  # Increased limit
                'ttl': int(time.time()) + 3600  # 1 hour TTL
            })
            return True
        
        item = response['Item']
        current_time = int(time.time())
        
        # Reset counter if a minute has passed
        if current_time - item['last_call_time'] >= 60:
            table.update_item(
                Key={'limiter_id': 'bedrock_calls'},
                UpdateExpression='SET calls_this_minute = :zero, last_call_time = :time',
                ExpressionAttributeValues={':zero': 0, ':time': current_time}
            )
            return True
        
        # Check if we're under the limit
        if item['calls_this_minute'] < item['max_calls_per_minute']:
            table.update_item(
                Key={'limiter_id': 'bedrock_calls'},
                UpdateExpression='ADD calls_this_minute :inc',
                ExpressionAttributeValues={':inc': 1}
            )
            return True
        
        print(f"Rate limit reached: {item['calls_this_minute']}/{item['max_calls_per_minute']} calls this minute")
        return False
        
    except Exception as e:
        print(f"Rate limit check failed: {e}")
        return True  # Fail open


def update_rate_limit_backoff():
    """
    Update rate limiter to reduce calls per minute when throttled
    """
    try:
        table_name = os.environ.get('BEDROCK_RATE_LIMITER_TABLE')
        if not table_name:
            return
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        
        # Reduce max calls per minute
        table.update_item(
            Key={'limiter_id': 'bedrock_calls'},
            UpdateExpression='SET max_calls_per_minute = GREATEST(:min_val, max_calls_per_minute - :dec)',
            ExpressionAttributeValues={':min_val': 2, ':dec': 2}
        )
        print("Reduced Bedrock rate limit due to throttling")
        
    except Exception as e:
        print(f"Error updating rate limit: {e}")


def get_message_retry_count(record):
    """
    Get the retry count for an SQS message based on receive count
    """
    try:
        # SQS provides approximate receive count in message attributes
        attributes = record.get('attributes', {})
        receive_count = int(attributes.get('ApproximateReceiveCount', '1'))
        return receive_count - 1  # Convert to retry count (0-based)
    except (ValueError, KeyError):
        return 0


def create_placeholder_analysis(message):
    """
    Create placeholder analysis when Bedrock fails
    """
    account_id = message.get('account_id', 'unknown')
    processed_events = message.get('processed_events', [])
    event_count = message.get('event_count', 0)
    
    # Analyze event severity to determine risk level
    critical_events = 0
    high_events = 0
    services_affected = set()
    regions_affected = set()
    
    for event in processed_events:
        severity = event.get('severity', 'low')
        if severity == 'critical':
            critical_events += 1
        elif severity == 'high':
            high_events += 1
        
        services_affected.add(event.get('service', 'Unknown'))
        regions_affected.add(event.get('region', 'Unknown'))
    
    # Determine risk level based on event analysis
    if critical_events > 0:
        risk_level = 'Critical'
        risk_justification = f"Account has {critical_events} critical health events requiring immediate attention"
    elif high_events > 0:
        risk_level = 'High'
        risk_justification = f"Account has {high_events} high-priority health events"
    elif event_count > 5:
        risk_level = 'Medium'
        risk_justification = f"Account has {event_count} health events that should be reviewed"
    elif event_count > 0:
        risk_level = 'Low'
        risk_justification = f"Account has {event_count} health events with minimal impact"
    else:
        risk_level = 'Low'
        risk_justification = "No health events found for this account"
    
    # Check if this is test mode
    test_skip_bedrock = os.environ.get('TEST_SKIP_BEDROCK', 'false').lower()
    is_test_mode = test_skip_bedrock in ['true', '1', 'yes']
    
    # Create comprehensive placeholder analysis
    placeholder_analysis = {
        'risk_level': risk_level,
        'risk_justification': risk_justification,
        'impact_analysis': {
            'affected_services': list(services_affected),
            'affected_regions': list(regions_affected),
            'potential_impact': f"AI analysis unavailable - manual review required for {event_count} events across {len(services_affected)} services"
        },
        'recommended_actions': [
            {
                'priority': 'High',
                'action': 'Manual review required - Bedrock AI analysis was unavailable due to rate limiting or service issues' if not is_test_mode else 'TEST MODE: Bedrock analysis skipped for testing purposes',
                'timeline': 'Immediate'
            },
            {
                'priority': 'High',
                'action': f'Review all {event_count} health events for account {account_id}',
                'timeline': 'Within 24 hours'
            },
            {
                'priority': 'Medium',
                'action': 'Check AWS Health Dashboard for additional context and recommendations',
                'timeline': 'Within 48 hours'
            }
        ],
        'business_impact': 'Impact assessment unavailable - manual review required to determine business implications',
        'summary': f'{"🧪 TEST MODE: " if is_test_mode else "⚠️ MANUAL REVIEW REQUIRED: "}Bedrock AI analysis was {"skipped for testing" if is_test_mode else "unavailable"} for account {account_id}{"" if is_test_mode else " (likely due to rate limiting)"}. This account has {event_count} health events that require {"manual assessment" if not is_test_mode else "review"}. Please review the attached event details and consult the AWS Health Dashboard for comprehensive analysis.',
        'analysis_status': 'TEST_PLACEHOLDER_ANALYSIS' if is_test_mode else 'PLACEHOLDER_ANALYSIS',
        'manual_review_required': True,
        'placeholder_reason': 'TEST_SKIP_BEDROCK flag enabled' if is_test_mode else 'Bedrock AI analysis service was unavailable after multiple attempts (likely rate limiting)'
    }
    
    return placeholder_analysis


def analyze_with_bedrock(message):
    """
    Analyze events using Amazon Bedrock with enhanced outage-focused analysis
    """
    # Check for testing flag to skip Bedrock calls
    test_skip_bedrock = os.environ.get('TEST_SKIP_BEDROCK', 'false').lower()
    if test_skip_bedrock in ['true', '1', 'yes']:
        print("TEST_SKIP_BEDROCK is enabled - using placeholder analysis instead of Bedrock")
        placeholder_analysis = create_placeholder_analysis(message)
        placeholder_analysis['test_mode'] = True
        placeholder_analysis['placeholder_reason'] = 'TEST_SKIP_BEDROCK flag enabled'
        return placeholder_analysis
    
    bedrock_payload = message.get('bedrock_payload')
    if not bedrock_payload:
        return {
            'risk_level': 'Low',
            'risk_justification': 'No events to analyze',
            'summary': 'No health events found for analysis'
        }
    
    # Get processed events for detailed analysis
    processed_events = message.get('processed_events', [])
    account_id = message.get('account_id', 'unknown')
    
    # Create enhanced prompt with outage focus
    prompt = create_enhanced_analysis_prompt(processed_events, account_id)
    
    # Call Bedrock with retry and enhanced response parsing
    return call_bedrock_with_retry(prompt)


def create_enhanced_analysis_prompt(processed_events, account_id):
    """
    Create enhanced prompt focused on outage analysis and business continuity
    """
    if not processed_events:
        return "No events to analyze for this account."
    
    # Get the most critical event for detailed analysis
    critical_events = [e for e in processed_events if e.get('severity') == 'critical']
    primary_event = critical_events[0] if critical_events else processed_events[0]
    
    # Extract event details
    event_type = primary_event.get('eventTypeCode', 'Unknown')
    event_category = primary_event.get('eventTypeCategory', 'Unknown')
    region = primary_event.get('region', 'Unknown')
    start_time = primary_event.get('startTime', 'Unknown')
    description = primary_event.get('eventDescription', 'No description available')
    
    # Format start time if it's a datetime object
    if hasattr(start_time, 'isoformat'):
        start_time = start_time.isoformat()
    
    # Create event summary
    event_summary = {
        'total_events': len(processed_events),
        'critical_events': len([e for e in processed_events if e.get('severity') == 'critical']),
        'high_events': len([e for e in processed_events if e.get('severity') == 'high']),
        'services_affected': list(set([e.get('service', 'Unknown') for e in processed_events])),
        'regions_affected': list(set([e.get('region', 'Unknown') for e in processed_events]))
    }
    
    prompt = f"""You are an AWS expert specializing in outage analysis and business continuity. Your task is to analyze these AWS Health events and determine their potential impact on workload availability, system connectivity, and service outages.

ACCOUNT: {account_id}

PRIMARY EVENT ANALYSIS:
- Type: {event_type}
- Category: {event_category}
- Region: {region}
- Start Time: {start_time}

Event Description:
{description}

ACCOUNT SUMMARY:
- Total Events: {event_summary['total_events']}
- Critical Events: {event_summary['critical_events']}
- High Priority Events: {event_summary['high_events']}
- Affected Services: {', '.join(event_summary['services_affected'])}
- Affected Regions: {', '.join(event_summary['regions_affected'])}

IMPORTANT ANALYSIS FOCUS:
1. Will this event cause workload downtime if required actions are not taken?
2. Will there be any service outages associated with this event?
3. Will the application/workload experience network integration issues between connecting systems?
4. What specific AWS services or resources could be impacted?

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

IMPORTANT: In your impact_analysis field, be very specific about:
1. Potential outages and their estimated duration
2. Connectivity issues between systems
3. Whether this will cause downtime if actions are not taken

In your consequences_if_ignored field, clearly state what outages or disruptions will occur if the event is not addressed.

For the key_date field:
- Analyze the event description for any dates that customers need to be aware of
- Look for dates when actions must be taken, when changes will occur, or when impacts will begin
- Return the EARLIEST date that will impact the customer in YYYY-MM-DD format
- If no specific date is mentioned or can be determined, return null
- Common date patterns to look for: deadlines, maintenance windows, deprecation dates, end-of-life dates, migration deadlines

RISK LEVEL GUIDELINES:
- CRITICAL: Will cause service outage or severe disruption if not addressed
- HIGH: Significant impact but not an immediate outage
- MEDIUM: Moderate impact requiring attention
- LOW: Minimal impact, routine maintenance

Ensure your response is valid JSON that can be parsed programmatically."""
    
    return prompt


def call_bedrock_with_retry(prompt, max_retries=3):
    """
    Call Bedrock with exponential backoff retry
    """
    bedrock = boto3.client('bedrock-runtime')
    
    model_id = os.environ.get('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20240620-v1:0')
    max_tokens = int(os.environ.get('BEDROCK_MAX_TOKENS', '4000'))
    temperature = float(os.environ.get('BEDROCK_TEMPERATURE', '0.3'))
    top_p = float(os.environ.get('BEDROCK_TOP_P', '0.9'))
    
    for attempt in range(max_retries):
        try:
            print(f"Calling Bedrock (attempt {attempt + 1}/{max_retries})")
            
            # Prepare request body
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "temperature": temperature,
                "top_p": top_p,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            }
            
            # Call Bedrock
            response = bedrock.invoke_model(
                modelId=model_id,
                body=json.dumps(request_body)
            )
            
            # Parse response
            response_body = json.loads(response['body'].read())
            
            # Extract content from Claude response
            content = response_body.get('content', [])
            if content and len(content) > 0:
                analysis_text = content[0].get('text', '')
                
                # Store the full analysis text
                print(f"Bedrock response length: {len(analysis_text)}")
                
                # Enhanced JSON parsing with multiple extraction methods
                analysis_result = parse_bedrock_response(analysis_text)
                
                if analysis_result:
                    print("Successfully parsed Bedrock response")
                    # Add the raw response for debugging
                    analysis_result['analysis_text'] = analysis_text
                    return analysis_result
                else:
                    print("Failed to parse Bedrock response, using fallback")
                    return create_fallback_analysis(analysis_text)
            else:
                print("No content in Bedrock response")
                return {
                    'risk_level': 'Low',
                    'risk_justification': 'No analysis content returned from Bedrock',
                    'summary': 'Bedrock analysis completed but no content returned'
                }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ThrottlingException':
                if attempt < max_retries - 1:
                    # Exponential backoff: 2^attempt seconds
                    wait_time = 2 ** attempt
                    print(f"Throttled, waiting {wait_time} seconds before retry {attempt + 1}")
                    time.sleep(wait_time)
                    continue
                else:
                    print("Final attempt failed due to throttling")
                    return {
                        'risk_level': 'Medium',
                        'risk_justification': 'Analysis failed due to persistent Bedrock throttling',
                        'summary': 'Unable to complete Bedrock analysis due to throttling after multiple attempts',
                        'error': 'Bedrock throttling',
                        'manual_review_required': True,
                        'bedrock_failure': True,
                        'failure_reason': 'Persistent Bedrock throttling after multiple retry attempts'
                    }
            else:
                print(f"Bedrock error: {error_code} - {e.response['Error']['Message']}")
                return {
                    'risk_level': 'Medium',
                    'risk_justification': f'Analysis failed due to Bedrock error: {error_code}',
                    'summary': f'Unable to complete Bedrock analysis due to {error_code}',
                    'error': f'Bedrock error: {error_code}',
                    'manual_review_required': True,
                    'bedrock_failure': True,
                    'failure_reason': f'Bedrock service error: {error_code} - {e.response["Error"]["Message"]}'
                }
        except Exception as e:
            print(f"Unexpected error calling Bedrock: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                return {
                    'risk_level': 'Medium',
                    'risk_justification': f'Analysis failed due to unexpected error: {str(e)}',
                    'summary': 'Unable to complete Bedrock analysis due to unexpected error',
                    'error': f'Unexpected error: {str(e)}',
                    'manual_review_required': True,
                    'bedrock_failure': True,
                    'failure_reason': f'Unexpected error during Bedrock analysis: {str(e)}'
                }
    
    # If we get here, all retries failed
    return {
        'risk_level': 'Medium',
        'risk_justification': 'Analysis failed due to repeated Bedrock errors',
        'summary': 'Unable to complete Bedrock analysis after multiple attempts',
        'error': 'Bedrock analysis failed',
        'manual_review_required': True,
        'bedrock_failure': True,
        'failure_reason': 'Bedrock analysis failed after multiple retry attempts'
    }


def parse_bedrock_response(response_text):
    """
    Enhanced parsing of Bedrock response with multiple extraction methods
    """
    import re
    
    # Method 1: Try to extract JSON from code blocks
    json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
    if json_match:
        json_str = json_match.group(1)
    else:
        # Method 2: Look for JSON object in the response
        json_match = re.search(r'({.*})', response_text, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_str = response_text
    
    # Try to parse the JSON
    try:
        # Clean the JSON string to handle control characters
        cleaned_json = json_str.replace('\n', '\\n').replace('\t', '\\t').replace('\r', '\\r')
        analysis = json.loads(cleaned_json)
        
        # Normalize and validate the response
        analysis = normalize_analysis_response(analysis)
        return analysis
        
    except json.JSONDecodeError as e:
        print(f"JSON parsing failed: {str(e)}")
        print(f"Attempting manual field extraction...")
        
        # Method 3: Manual field extraction if JSON parsing fails
        return extract_fields_manually(response_text)


def normalize_analysis_response(analysis):
    """
    Normalize and validate the analysis response
    """
    # Ensure required fields exist with defaults
    defaults = {
        'critical': False,
        'risk_level': 'Low',
        'account_impact': 'Low',
        'time_sensitivity': 'Routine',
        'risk_category': 'Operational',
        'required_actions': 'Review event details',
        'impact_analysis': 'Impact assessment pending',
        'consequences_if_ignored': 'Unknown consequences',
        'affected_resources': 'Unknown resources',
        'key_date': None,
        'business_impact': 'Business impact assessment pending',
        'summary': 'Analysis summary pending'
    }
    
    # Apply defaults for missing fields
    for key, default_value in defaults.items():
        if key not in analysis:
            analysis[key] = default_value
    
    # Normalize risk level to ensure consistency
    if 'risk_level' in analysis:
        risk_level = str(analysis['risk_level']).strip().upper()
        
        if risk_level in ['CRITICAL', 'SEVERE']:
            analysis['risk_level'] = 'Critical'
            analysis['critical'] = True
        elif risk_level == 'HIGH':
            analysis['risk_level'] = 'High'
        elif risk_level in ['MEDIUM', 'MODERATE']:
            analysis['risk_level'] = 'Medium'
        elif risk_level == 'LOW':
            analysis['risk_level'] = 'Low'
    
    # Ensure critical flag is consistent with risk level
    if analysis.get('critical', False) and analysis['risk_level'] != 'Critical':
        analysis['risk_level'] = 'Critical'
    
    # Normalize time sensitivity
    if 'time_sensitivity' in analysis:
        time_sens = str(analysis['time_sensitivity']).strip().title()
        if time_sens not in ['Critical', 'Urgent', 'Routine']:
            analysis['time_sensitivity'] = 'Routine'
        else:
            analysis['time_sensitivity'] = time_sens
    
    return analysis


def extract_fields_manually(response_text):
    """
    Manually extract fields from response text when JSON parsing fails
    """
    import re
    
    manual_analysis = {}
    
    # Extract critical status
    if '"critical": false' in response_text.lower() or '"critical":false' in response_text.lower():
        manual_analysis['critical'] = False
    elif '"critical": true' in response_text.lower() or '"critical":true' in response_text.lower():
        manual_analysis['critical'] = True
    else:
        manual_analysis['critical'] = False
    
    # Extract risk level
    risk_match = re.search(r'"risk_level":\s*"([^"]+)"', response_text, re.IGNORECASE)
    if risk_match:
        manual_analysis['risk_level'] = risk_match.group(1).title()
    else:
        manual_analysis['risk_level'] = 'Medium'
    
    # Extract other key fields
    field_patterns = {
        'account_impact': r'"account_impact":\s*"([^"]+)"',
        'time_sensitivity': r'"time_sensitivity":\s*"([^"]+)"',
        'risk_category': r'"risk_category":\s*"([^"]+)"',
        'required_actions': r'"required_actions":\s*"([^"]+)"',
        'impact_analysis': r'"impact_analysis":\s*"([^"]+)"',
        'consequences_if_ignored': r'"consequences_if_ignored":\s*"([^"]+)"',
        'affected_resources': r'"affected_resources":\s*"([^"]+)"',
        'business_impact': r'"business_impact":\s*"([^"]+)"',
        'summary': r'"summary":\s*"([^"]+)"'
    }
    
    for field, pattern in field_patterns.items():
        match = re.search(pattern, response_text, re.IGNORECASE | re.DOTALL)
        if match:
            # Clean up the extracted text
            value = match.group(1).replace('\\n', '\n').replace('\\t', '\t')
            manual_analysis[field] = value[:500]  # Limit length
    
    # Extract key_date
    date_match = re.search(r'"key_date":\s*"([^"]+)"', response_text, re.IGNORECASE)
    if date_match:
        date_value = date_match.group(1)
        manual_analysis['key_date'] = date_value if date_value.lower() != 'null' else None
    else:
        manual_analysis['key_date'] = None
    
    # Apply defaults for missing fields
    defaults = {
        'account_impact': 'Medium',
        'time_sensitivity': 'Routine',
        'risk_category': 'Operational',
        'required_actions': 'Review event details manually - automated parsing incomplete',
        'impact_analysis': 'Impact analysis extraction incomplete - manual review required',
        'consequences_if_ignored': 'Consequences assessment incomplete - manual review required',
        'affected_resources': 'Resource identification incomplete - manual review required',
        'business_impact': 'Business impact assessment incomplete - manual review required',
        'summary': 'Analysis summary extraction incomplete - manual review required'
    }
    
    for key, default_value in defaults.items():
        if key not in manual_analysis:
            manual_analysis[key] = default_value
    
    # Normalize the manually extracted analysis
    return normalize_analysis_response(manual_analysis)


def create_fallback_analysis(response_text):
    """
    Create fallback analysis when all parsing methods fail
    """
    return {
        'critical': False,
        'risk_level': 'Medium',
        'account_impact': 'Medium',
        'time_sensitivity': 'Routine',
        'risk_category': 'Operational',
        'required_actions': 'Manual review required - automated analysis parsing failed',
        'impact_analysis': 'Impact analysis unavailable due to parsing failure - manual review required',
        'consequences_if_ignored': 'Consequences assessment unavailable - manual review required',
        'affected_resources': 'Resource identification unavailable - manual review required',
        'key_date': None,
        'business_impact': 'Business impact assessment unavailable due to parsing failure',
        'summary': 'Analysis parsing failed - manual review of health events required',
        'analysis_text': response_text[:1000] + '...' if len(response_text) > 1000 else response_text,
        'parsing_error': True
    }
# Note: Email queue triggering is now handled by Step Functions workflow