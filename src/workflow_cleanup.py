"""
Workflow Cleanup Lambda Function
Handles cleanup of workflow data after email notifications are queued
"""
import boto3
import json
import os
import traceback
from shared_utils import cleanup_workflow_results


def lambda_handler(event, context):
    """
    Clean up workflow data after email notifications are queued
    """
    try:
        workflow_id = event['workflow_id']
        
        print(f"Starting cleanup for workflow {workflow_id}")
        
        # Clean up workflow data
        cleanup_results = cleanup_workflow_data(workflow_id)
        
        return {
            'workflow_id': workflow_id,
            'cleanup_results': cleanup_results,
            'status': 'cleanup_completed'
        }
        
    except Exception as e:
        print(f"Error in workflow cleanup: {str(e)}")
        traceback.print_exc()
        
        # Don't fail the workflow for cleanup issues - just log and continue
        return {
            'workflow_id': event.get('workflow_id', 'unknown'),
            'cleanup_results': {
                'status': 'cleanup_failed',
                'error': str(e)
            },
            'status': 'cleanup_failed_but_continuing'
        }


def cleanup_workflow_data(workflow_id):
    """
    Clean up workflow data after completion
    """
    try:
        if not workflow_id:
            print("No workflow_id provided for cleanup")
            return {'status': 'no_workflow_id'}
        
        print(f"Cleaning up workflow data for: {workflow_id}")
        
        # Clean up workflow results from DynamoDB
        cleanup_workflow_results(workflow_id)
        
        print(f"Successfully cleaned up workflow {workflow_id}")
        
        return {
            'status': 'cleanup_completed',
            'workflow_id': workflow_id
        }
        
    except Exception as e:
        print(f"Cleanup failed for workflow {workflow_id}: {e}")
        traceback.print_exc()
        
        # Don't fail the workflow for cleanup issues
        return {
            'status': 'cleanup_failed',
            'workflow_id': workflow_id,
            'error': str(e)
        }