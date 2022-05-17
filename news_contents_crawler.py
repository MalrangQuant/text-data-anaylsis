import pdb
import boto3
import datetime as dt

if __name__ == '__main__':

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='naver-news-list')

    while True:
        print(f'[{dt.datetime.now()}] Fetching news')

        messages = queue.receive_messages(
            MessageAttributeNames = ['ALL'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        ) #Call messages from the queue

        print(messages)

        pdb.set_trace()