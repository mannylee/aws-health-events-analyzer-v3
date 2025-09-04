"""
Email Queue Manager Lambda Function
Combined completion checker and email queue manager
"""
import boto3
import json
import os
import traceback
from datetime import datetime
from collections import defaultdict
from shared_utils import IncompleteWorkflowException, get_workflow_results


def lambda_handler(event, context):
    """
    Combined completion checker and email queue manager
    """
    try:
        workflow_id = event['workflow_id']
        total_accounts = event['total_accounts']
        
        print(f"Checking completion and queuing emails for workflow {workflow_id}")
        
        # Step 1: Check completion status
        completion_status = check_workflow_completion(workflow_id, total_accounts)
        
        if not completion_status['all_complete']:
            # Raise exception to trigger Step Functions retry
            raise IncompleteWorkflowException(
                f"Workflow {workflow_id} incomplete: "
                f"{completion_status['completed_count']}/{total_accounts} accounts processed"
            )
        
        print(f"All {total_accounts} accounts completed. Queuing email notifications...")
        
        # Step 2: Queue email notifications (only if complete)
        email_results = queue_email_notifications(workflow_id, completion_status['results'])
        
        return {
            'workflow_id': workflow_id,
            'completion_status': completion_status,
            'email_queue_results': email_results,
            'status': 'emails_queued'
        }
        
    except IncompleteWorkflowException:
        # Re-raise to trigger Step Functions retry
        raise
    except Exception as e:
        print(f"Error in email queue manager: {str(e)}")
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
    Queue all email notifications
    """
    sqs = boto3.client('sqs')
    email_queue_url = os.environ['EMAIL_NOTIFICATION_QUEUE_URL']
    
    # Group results by email address
    emails_to_send = group_results_by_email(all_results)
    
    queued_emails = []
    
    # Queue account-specific emails
    for email_address, account_results in emails_to_send.items():
        try:
            message_body = {
                'email_type': 'account_specific',
                'email_address': email_address,
                'workflow_id': workflow_id,
                'account_results': account_results,
                'timestamp': datetime.now().isoformat()
            }
            
            response = sqs.send_message(
                QueueUrl=email_queue_url,
                MessageBody=json.dumps(message_body, default=str)
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
            
            response = sqs.send_message(
                QueueUrl=email_queue_url,
                MessageBody=json.dumps(message_body, default=str)
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
    
    return {
        'total_emails_queued': len(queued_emails),
        'queued_emails': queued_emails
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