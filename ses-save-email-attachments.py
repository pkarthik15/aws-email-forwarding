import os
import json
import boto3
import email
from email.parser import BytesParser
from email import policy


s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')


def lambda_handler(event, context):
    
    ses_mail = event['Records'][0]['ses']['mail']
    message_id = ses_mail['messageId']

    print(message_id)

    bucket_name = "your_bucket_name"
    raw_email_prefix = "source_email_folder"
    attachment_prefix = "attachments_folder"
    
    data = s3.get_object(Bucket=bucket_name, Key=f"{raw_email_prefix}/{message_id}")
    contents = data['Body'].read().decode("utf-8")
    msg = email.message_from_string(contents)
    content_data = msg.get_payload()
    
    attachments = []

    try:
        if content_data != []:
            for index, attachment_value in enumerate(content_data):
                attachments.append(attachment_value)
            
            for index, attachment in enumerate(attachments):
                if not attachment or type(attachment) == None:
                    continue
                
                contentType = attachment.get_content_type()
                file_name = attachment.get_filename()
                print("type of attachment >> ",type(attachment), contentType, file_name)
                
                
                if file_name != "" and file_name != None and file_name and len(file_name.split(".")) > 0:
                    fileKey = message_id
                    try:
                        file_ext = file_name.split(".")[-1]
                        local_path = '/tmp/{}.{}'.format(fileKey,file_ext)
                        
                        if file_ext == 'pdf' or file_ext == 'png' or file_ext == 'jpeg' or file_ext == 'jpg':
                            final_s3_path = attachment_prefix +'/' + fileKey + "." + file_ext
                            open(local_path, 'wb').write(attachment.get_payload(decode=True))
                            s3.upload_file(local_path, bucket_name, final_s3_path)
                            os.remove(local_path)
                        else:
                            print("file is of other type so cannot be processed!")
                            
                    except Exception as e:
                        print("exception >> ",str(e))
        else:
            print("no attachment found")
        
       
        return {
            'statusCode': 200,
            'body': json.dumps('SES Email received and attachment aaved!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps('Error in processing SES Email!')
        }
