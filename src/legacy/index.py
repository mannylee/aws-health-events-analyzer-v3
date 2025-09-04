"""
AWS Health Events Analyzer - Compatibility Wrapper
This file maintains backward compatibility while the system transitions to microservices
"""
import json
import traceback
from workflow_initializer import lambda_handler as workflow_initializer_handler


def lambda_handler(event, context):
    """
    Backward compatibility wrapper for the original monolithic function
    This allows manual invocation while the system uses Step Functions for scheduled runs
    """
    try:
        print("AWS Health Events Analyzer - Compatibility Mode")
        print("Note: This is running in compatibility mode. For production, use the Step Functions workflow.")
        
        # Check if this is a manual invocation or test
        source = event.get('source', '')
        detail_type = event.get('detail-type', '')
        
        if source == 'aws.events' and 'Scheduled Event' in detail_type:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Scheduled execution should use Step Functions workflow, not this function',
                    'workflow_arn': 'Check CloudFormation outputs for HealthEventsWorkflowArn'
                })
            }
        
        # For manual invocations, run the workflow initializer as a demo
        print("Running workflow initializer for demonstration...")
        result = workflow_initializer_handler(event, context)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Workflow initialization completed successfully',
                'result': result,
                'note': 'This is a demonstration. Full processing requires the Step Functions workflow.'
            }, default=str)
        }
        
    except Exception as e:
        print(f"Error in compatibility wrapper: {str(e)}")
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Error in compatibility mode. Check CloudWatch logs for details.'
            })
        }