"""
Workflow Cleanup Lambda Function
Cleans up temporary workflow data after completion
"""
import traceback
from shared_utils import cleanup_workflow_results


def lambda_handler(event, context):
    """
    Clean up workflow data after completion
    """
    try:
        workflow_id = event.get('workflow_id')
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