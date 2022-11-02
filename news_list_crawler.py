import json
import pdb
import boto3
import requests
import datetime as dt


from urllib.parse import urlparse
from bs4 import BeautifulSoup as bs
from dateutil.relativedelta import relativedelta

def fetch_news_list(datestr, page):
    print(f'Fetching page {page}')

    url = f'https://news.naver.com/main/list.naver?mode=LSD&mid=sec&sid1=101&date={datestr}&page={page}'

    #print(url)

    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' } # 이걸 붙여서 크롤러가 아니라는 것을 증명하는 방법임

    resp = requests.get(url, headers=headers)

    #print(resp.status_code)
    #print(resp.text)

    soup = bs(resp.text, 'html.parser')
#    print(soup.title)con

    list_body = soup.find("div", {"class":"list_body"})

    buffer = []
    
    for item in list_body.find_all("li"):
#        print(item)

        link = item.find_all("a")[-1]
        title = link.text.strip()

        parsed_url = urlparse(link['href']) # url 값을 쪼개주기

        msg_id = parsed_url.path.split('/')
        msg_id = '-'.join(msg_id[-2:])
        

        # print(title)
        # print(msg_id)

        body = {
            'msg_id' : msg_id,
            'title' : title,
            'url' : link['href'],
        }

        entry = {
            'Id' : msg_id,
            'MessageBody' : json.dumps(body),
        }

#        print(entry)

        buffer.append(entry)

    return buffer
   
    pass

def fetch_news_list_for_date(queue, date):
    '''news address requires date and page'''
    datestr = date.strftime('%Y%m%d')

    print(f'{dt.datetime.now()} Fetching news list on {datestr}')

    buffer = []

    for page in range(1, 1000):
        entries = fetch_news_list(datestr, page)

        if len(entries) == 0:
            break

        if len(buffer) > 0 and entries[-1]['Id'] == buffer[-1]['Id']:
            break #마지막 페이지 도착시 루프 멈추기

        buffer += entries

        if len(entries) < 20:
            break

    print(f'Total number of fetched articles: {len(buffer)}')

    temp = {x['Id']: x for x in buffer}
    buffer = list(temp.values())
    
    print('Pushing to AWS SQS', end='')

    if len(buffer) > 0:
        for idx in range(0, len(buffer), 10):
            print('.', end='', flush=True)

            chunk = buffer[idx:idx+10]
            queue.send_messages(Entries=chunk)

        print('Successfully pushed to AWS SQS!')

#    pdb.set_trace()

if __name__ == "__main__":

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='naver-news-list')

    base_date = dt.datetime(2022, 9, 1)

    for days in range(30):
        date = base_date + relativedelta(days=days)

        fetch_news_list_for_date(queue, date)

