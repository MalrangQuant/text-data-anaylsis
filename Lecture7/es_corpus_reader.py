import datetime as dt

from dateutil.relativedelta import relativedelta
from download_data import fetch_news_docs

class EsCorpusReader:
    def __init__(self, date_from, date_to):
        if date_to:
            self.date_to = date_to
        else:
            self.date_to = dt.datetime.now()

        if date_from:
            self.date_from = date_from
        else:
            self.date_from = self.date_to - relativedelta(days=1)
    
        self._buffer = []
        self._next_page = 0


    def fetch_next_page(self):


if __name__ == '__main__':
    reader = EsCorpusReader(date_from=dt.datetime(2022, 4, 1))

    print(reader)