import re
import pdb
import time
import json
import boto3
import requests
import traceback
from regex import D
from config import *
import datetime as dt
from bs4 import BeautifulSoup as bs



def fetch_news_content(msg):
    
    item = json.loads(msg.body)
    #print(item)

    headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }

    r = requests.get(item['url'], headers=headers)

    assert r.status_code == 200 # 안전장치

    soup = bs(r.text, 'html.parser')

    node = soup.find("meta", {"name" : "twitter:creator"}) #신문사
    publisher = node['content']

    #print('publisher', publisher) 

    datestr_list, source_url = parse_meta_info(soup)

    if len(datestr_list) == 1:
        created_at = parse_datestr(datestr_list[0])
        updated_at = created_at

    elif len(datestr_list) == 2:
        created_at = parse_datestr(datestr_list[0])
        updated_at = parse_datestr(datestr_list[1])
    else:
        raise RuntimeError("Invalid datestr_list", datestr_list)

    body = soup.find("div", {"id" : "newsct_article"})

    assert body is not None

    body_text = clean_test_body(body)
    # print(body_text)

    images = body.find_all('img')
    image_urls = [x['data-src'] for x in images]
    image_urls = list(set(image_urls))
    # print(image_urls)

    byline = soup.find("span", {"class" : "byline_s"})
    reporter_name, reporter_email = extract_reporter(byline)


    # print('publisher', publisher)
    # print('created_at', created_at)
    # print('updated_at', updated_at)
    # print('reporter_name', reporter_name)
    # print('reporter_email', reporter_email)

    entry = {
        'id' : item['msg_id'],
        'title' : item['title'],
        'section' : 'economy', #경제뉴스만 하구있으니깡
        'naver_url' : item['url'],
        'source_url' : source_url,
        'image_urls' : image_urls,
        'publisher' : publisher,
        'created_at' : created_at.isoformat(),
        'updated_at' : updated_at.isoformat(),
        'reporter_name' : reporter_name,
        'reporter_email' : reporter_email,
        'body' : body_text
    }

    return entry


def extract_reporter (byline):

    if byline is None:
        return '', ''

    byline = byline.text

    reporter = re.findall(r'([\wㄱ-ㅎ가-힣]+)\s*(기자)?\s*\(([\w\.]+@[\w\.]+)\)?', byline)

    if len(reporter) > 0:
        return reporter[0][0], reporter[0][2]

    reporter = re.findall(r'([\wㄱ-ㅎ가-힣]+)\s*(기자)?', byline)

    if len(reporter) > 0 :
        return reporter [0][0], ''

    return '', ''


def clean_test_body (body):

    body_text = body.get_text("\n")
    body_text = body_text.strip()

    buffer = []

    for line in body_text.split('\n'):
        line = line.strip()
        if len(line) > 0:
            buffer.append(line)
    
    return '\n'.join(buffer)


def parse_datestr (date_span):

    if date_span.has_attr('data-date-time'):
        datestr = date_span['data-date-time']

    elif date_span.has_attr('data-modify-date-time'):
        datestr = date_span['data-modify-date-time']
    
    else:
        raise RuntimeError('Unknown date span', date_span)

    date = dt.datetime.fromisoformat(datestr)

    return date



def parse_meta_info (soup):
    
    media_info = soup.find("div", {"class" : "media_end_head_info_datestamp"}) #게시 날짜, 수정 날짜 (있으면) 받아오기 위한 class 찾아주깅
    datestr_list = media_info.find_all("span", {"class" : "media_end_head_info_datestamp_time"})

    link = media_info.find("a", {'class': 'media_end_head_origin_link'})
    source_url = link['href'] if link is not None else ''

    return datestr_list, source_url


def upload_to_elastic_search (buffer):

    if len(buffer) == 0:
        return

    data = ''

    for x in buffer:
        index = {
            "index" : {
                "_id" : x['id']
            }
        }

        data += json.dumps(index) + "\n"

        del x['id']
        
        data += json.dumps(x) + "\n"

    headers = { 'Content-Type' : 'application/json' }

    resp = requests.post(
        f'{ELASTIC_SEARCH_URL}/news/_bulk?pretty&refresh',
        headers = headers,
        data=data,
        auth=ELASTIC_SEARCH_AUTH
    )

    assert resp.status_code == 200

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

        # print(messages)

        if len(messages) == 0 :
            print(" - queue is empty. Wait for a minute.")
            time.sleep(60)
            continue

        for msg in messages:
            msg.delete()

        buffer = []

        for msg in messages:
            try:
                news = fetch_news_content(msg)
                buffer.append(news)
            except Exception as e:
                print('*** Exception occurred! ***')
                print(e)
                print(msg.body)
                print(traceback.format_exc())

                raise e 

        upload_to_elastic_search(buffer)

        pdb.set_trace()