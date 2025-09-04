"""
Email Queue Manager and Workflow Cleanup Lambda Function
Combined completion checker, email queue manager, and workflow cleanup
"""
import boto3
import json
import os
import traceback
from datetime import datetime
from collections import defaultdict
from shared_utils import IncompleteWorkflowException, get_workflow_results, cleanup_workflow_results


def lambda_handler(event, context):
    """
    Combined completion checker, email queue manager, and workflow cleanup
    """
    try:
        workflow_id = event['workflow_id']
        total_accounts = event['total_accounts']
        fallback_mode = event.get('fallback_mode', False)
        triggered_by = event.get('triggered_by', 'step_functions')
        
        if fallback_mode:
            print(f"Running in fallback mode for workflow {workflow_id} (triggered by {triggered_by})")
        else:
            print(f"Checking completion and queuing emails for workflow {workflow_id} (triggered by {triggered_by})")
        
        # Step 1: Check completion status
        completion_status = check_workflow_completion(workflow_id, total_accounts)
        
        if not completion_status['all_complete']:
            if fallback_mode:
                # In fallback mode, we're more patient and provide better logging
                print(f"Fallback mode: Workflow {workflow_id} still incomplete: "
                      f"{completion_status['completed_count']}/{total_accounts} accounts processed")
                print("This is expected if Bedrock analyzer is still processing accounts.")
                print("Emails should be triggered automatically when the last account completes.")
            
            # Raise exception to trigger Step Functions retry (don't cleanup yet)
            raise IncompleteWorkflowException(
                f"Workflow {workflow_id} incomplete: "
                f"{completion_status['completed_count']}/{total_accounts} accounts processed"
            )
        
        print(f"All {total_accounts} accounts completed. Queuing email notifications...")
        
        # Step 2: Queue email notifications (only if complete)
        email_results = queue_email_notifications(workflow_id, completion_status['results'])
        
        print(f"Email notifications queued successfully.")
        
        return {
            'workflow_id': workflow_id,
            'completion_status': completion_status,
            'email_queue_results': email_results,
            'status': 'emails_queued'
        }
        
    except IncompleteWorkflowException:
        # Re-raise to trigger Step Functions retry (don't cleanup)
        raise
    except Exception as e:
        print(f"Error in email queue manager and cleanup: {str(e)}")
        traceback.print_exc()
        raise


def check_workflow_completion(workflow_id, total_accounts):
    """
    Check if all Bedrock analysis is complete
    """
    try:
        # Get all results for this workflow
        results = get_workflow_results(workflow_id)
        completed_count = len(results)
        
        print(f"Workflow {workflow_id}: {completed_count}/{total_accounts} accounts completed")
        
        return {
            'all_complete': completed_count >= total_accounts,
            'completed_count': completed_count,
            'total_accounts': total_accounts,
            'results': results if completed_count >= total_accounts else []
        }
        
    except Exception as e:
        print(f"Error checking completion: {e}")
        return {
            'all_complete': False,
            'completed_count': 0,
            'total_accounts': total_accounts,
            'results': []
        }


def queue_email_notifications(workflow_id, all_results):
    """
    Queue all email notifications with idempotency protection
    """
    # Check if emails were already queued for this workflow
    if check_emails_already_queued(workflow_id):
        print(f"DUPLICATE PREVENTION: Emails already queued for workflow {workflow_id}, skipping duplicate queuing")
        print(f"This prevents duplicate email notifications. Workflow ID: {workflow_id}")
        return {
            'total_emails_queued': 0,
            'queued_emails': [],
            'status': 'already_queued',
            'workflow_id': workflow_id
        }
    
    sqs = boto3.client('sqs')
    email_queue_url = os.environ['EMAIL_NOTIFICATION_QUEUE_URL']
    
    # Group results by email address
    emails_to_send = group_results_by_email(all_results)
    
    queued_emails = []
    
    # Queue account-specific emails (unless TEST_MASTER_EMAIL_ONLY is enabled)
    test_master_only = os.environ.get('TEST_MASTER_EMAIL_ONLY', 'false').lower() == 'true'
    
    if test_master_only:
        print("TEST_MASTER_EMAIL_ONLY enabled - skipping account-specific emails")
    else:
        for email_address, account_results in emails_to_send.items():
            try:
                message_body = {
                    'email_type': 'account_specific',
                    'email_address': email_address,
                    'workflow_id': workflow_id,
                    'account_results': account_results,
                    'timestamp': datetime.now().isoformat()
                }
                
                # Create a more robust deduplication ID that includes content hash
                import hashlib
                content_hash = hashlib.md5(json.dumps(sorted([r.get('account_id') for r in account_results]), default=str).encode()).hexdigest()[:8]
                dedup_id = f"{workflow_id}-{email_address}-{content_hash}-account"
                
                response = sqs.send_message(
                    QueueUrl=email_queue_url,
                    MessageBody=json.dumps(message_body, default=str),
                    MessageGroupId=f"account-{email_address}",
                    MessageDeduplicationId=dedup_id
                )
                
                queued_emails.append({
                    'email': email_address,
                    'accounts': len(account_results),
                    'message_id': response['MessageId'],
                    'type': 'account_specific'
                })
                print(f"Queued account-specific email for {email_address} ({len(account_results)} accounts)")
                
            except Exception as e:
                print(f"Failed to queue email for {email_address}: {e}")
    
    # Queue master report email
    try:
        default_recipients = os.environ.get('RECIPIENT_EMAILS', '').split(',')
        default_recipients = [email.strip() for email in default_recipients if email.strip()]
        
        if default_recipients:
            message_body = {
                'email_type': 'master_report',
                'email_addresses': default_recipients,
                'workflow_id': workflow_id,
                'all_results': all_results,
                'timestamp': datetime.now().isoformat()
            }
            
            # Create a more robust deduplication ID for master report
            import hashlib
            content_hash = hashlib.md5(json.dumps(sorted([r.get('account_id') for r in all_results]), default=str).encode()).hexdigest()[:8]
            dedup_id = f"{workflow_id}-{content_hash}-master-report"
            
            response = sqs.send_message(
                QueueUrl=email_queue_url,
                MessageBody=json.dumps(message_body, default=str),
                MessageGroupId="master-report",
                MessageDeduplicationId=dedup_id
            )
            
            queued_emails.append({
                'email': 'master_report',
                'recipients': default_recipients,
                'message_id': response['MessageId'],
                'type': 'master_report'
            })
            print(f"Queued master report email for {len(default_recipients)} recipients")
        
    except Exception as e:
        print(f"Failed to queue master report: {e}")
    
    # Mark emails as queued to prevent duplicates
    marked_successfully = mark_emails_as_queued(workflow_id)
    
    if not marked_successfully and len(queued_emails) > 0:
        # Race condition detected - another process already queued emails
        print(f"Warning: Race condition detected for workflow {workflow_id}. Emails may have been queued by another process.")
        print("This could result in duplicate emails. Consider purging the email queue if duplicates are detected.")
    
    return {
        'total_emails_queued': len(queued_emails),
        'queued_emails': queued_emails,
        'race_condition_detected': not marked_successfully,
        'workflow_id': workflow_id
    }


def group_results_by_email(all_results):
    """
    Group analysis results by email address
    """
    email_groups = defaultdict(list)
    
    for result in all_results:
        result_data = result.get('result_data', {})
        email_mapping = result_data.get('email_mapping')
        
        if email_mapping:
            email_groups[email_mapping].append(result)
        else:
            print(f"No email mapping for account {result.get('account_id', 'unknown')}")
    
    print(f"Grouped results into {len(email_groups)} email addresses")
    for email, results in email_groups.items():
        accounts = [r.get('account_id', 'unknown') for r in results]
        print(f"Email {email}: accounts {accounts}")
    
    return dict(email_groups)


def check_emails_already_queued(workflow_id):
    """
    Check if emails were already queued for this workflow using DynamoDB
    """
    try:
        dynamodb = boto3.resource('dynamodb')
        table_name = os.environ.get('WORKFLOW_RESULTS_TABLE')
        table = dynamodb.Table(table_name)
        
        # Check for a special marker item indicating emails were queued
        response = table.get_item(
            Key={
                'workflow_id': workflow_id,
                'account_id': 'EMAIL_QUEUED_MARKER'
            }
        )
        
        return 'Item' in response
        
    except Exception as e:
        print(f"Error checking email queue status: {e}")
        return False


def mark_emails_as_queued(workflow_id):
    """
    Mark that emails have been queued for this workflow with conditional write to prevent race conditions
    """
    try:
        import time
        from botocore.exceptions import ClientError
        dynamodb = boto3.resource('dynamodb')
        table_name = os.environ.get('WORKFLOW_RESULTS_TABLE')
        table = dynamodb.Table(table_name)
        
        # Use conditional write to prevent race conditions
        try:
            table.put_item(
                Item={
                    'workflow_id': workflow_id,
                    'account_id': 'EMAIL_QUEUED_MARKER',
                    'result_data': {
                        'emails_queued_at': datetime.now().isoformat(),
                        'status': 'emails_queued'
                    },
                    'status': 'EMAIL_MARKER',
                    'timestamp': datetime.now().isoformat(),
                    'ttl': int(time.time()) + (7 * 24 * 60 * 60)  # 7 days TTL
                },
                ConditionExpression='attribute_not_exists(workflow_id)'
            )
            print(f"Marked emails as queued for workflow {workflow_id}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                print(f"Emails already marked as queued for workflow {workflow_id} (race condition avoided)")
                return False
            else:
                raise
        
    except Exception as e:
        print(f"Error marking emails as queued: {e}")
        return False


