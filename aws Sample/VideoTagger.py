
import logging
import os
import json
from pprint import pprint
import time
import boto3
from botocore.exceptions import ClientError
import requests

from rekognition_objects import (
    RekognitionFace, RekognitionCelebrity, RekognitionLabel,
    RekognitionModerationLabel, RekognitionPerson)
    
from rekognition_video_detection import RekognitionVideo

logger = logging.getLogger(__name__)

def CreateListOfVideoObject(targetDir, bkt, recursive = True):
    FindItemsInDirectory(targetDir, )
    for vid in videoList:
        print('Uploading:' + str(targetDir))
        upload_file(vid, bkt )

# Modified from an aws example, use this to upload the footage to a bucket and then run analysis later
# Might make more sense to run everything asyncronously
def upload_file(file_path, bucketObj, object_name=None):
    """Upload a file to an S3 bucket

    :param file_path: File to upload
    :param bucketObj: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        head_tail = os.path.split(file_path) 
        object_name = head_tail[1]

    # Create an object
    object = bucketObj.Object(object_name)

    # Upload the file
    try:
        response = object.upload_file(file_path)
    except ClientError as e:
        logging.error(e)
        return False

    return object

def main():
    print('-'*88)
    print("Welcome to the Amazon Rekognition video detection demo!")
    print('-'*88)

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    print("Creating Amazon S3 bucket and uploading video.")
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.create_bucket(
        Bucket=f'doc-example-bucket-rekognition-{time.time_ns()}',
        CreateBucketConfiguration={
            'LocationConstraint': s3_resource.meta.client.meta.region_name
        })

    # Upload each video
    # targetPath = 'D:\ResolveTestingOneSecond'
    videoPath = 'C:/VMShared/GitRepos/PersonalProject/VideoTagger/Videos/testvideo.mp4'
    
    video_object = upload_file(videoPath, bucket)

    # This is a making a rekognition object
    rekognition_client = boto3.client('rekognition')

    video = RekognitionVideo.from_bucket(video_object, rekognition_client)
    
    # Create a notification channel for this video
    print("Creating notification channel from Amazon Rekognition to Amazon SQS.")
    iam_resource = boto3.resource('iam')
    sns_resource = boto3.resource('sns')
    sqs_resource = boto3.resource('sqs')
    video.create_notification_channel(
        'doc-example-video-rekognition4', iam_resource, sns_resource, sqs_resource)

    print("Detecting labels in the video.")
    labels = video.do_label_detection()
    print(f"Detected {len(labels)} labels, here are the first twenty:")
    for label in labels[:20]:
        pprint(label.to_dict())
    input("Press Enter when you're ready to continue.")

    print("Deleting resources created for the demo.")
    video.delete_notification_channel()
    bucket.objects.delete()
    bucket.delete()
    logger.info("Deleted bucket %s.", bucket.name)
    print("All resources cleaned up. Thanks for watching!")
    print('-'*88)


if __name__ == '__main__':
        main()
