import io
import time
import datetime
import json
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor
from threading import Lock  # TODO - Improvement: Don't use locks
from TwitterSearch import TwitterSearch

DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"  # "Fri Mar 29 11:03:41 +0000 2013";
SCRAPING_RATE = 100


class TwitterSlicer(TwitterSearch):
    """
    Inspired by: https://github.com/simonlindgren/TwitterScraper/blob/master/TwitterSucker.py
    The concept is to have an implementation that actually splits the query into multiple days.
    The only additional parameters a user has to input, is a minimum date, and a maximum date.
    This method also supports parallel scraping.
    """

    def __init__(self, logger, session, rate_delay, error_delay, since, until, limit, filepath, useragent_cache_path,
                 n_threads=1):
        super(TwitterSlicer, self).__init__(logger, session, rate_delay, error_delay, useragent_cache_path)
        self.logger = logger
        self.since = since
        self.until = until
        self.limit = limit
        self.n_threads = n_threads
        self.counter = 0
        self.counter_lock = Lock()
        self.filepath = filepath
        self.jsonl_files_dicts = {}  # Day dict -> List of o -> File
        self.UA = UserAgent(fallback='Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/0.8.12',
                            path=useragent_cache_path)

    def search(self, query):
        # Specify a user agent to prevent Twitter from returning a profile card
        headers = {'User-Agent': self.UA.random}
        self.session.headers.update(headers)

        time_since = datetime.datetime.strptime(self.since, "%Y-%m-%d")
        time_until = datetime.datetime.strptime(self.until, "%Y-%m-%d")

        n_days = (time_until - time_since).days
        tp = ThreadPoolExecutor(max_workers=n_days)
        for i in range(0, n_days):
            since_date = time_since + datetime.timedelta(days=i)
            until_date = time_since + datetime.timedelta(days=(i + 1))
            since_date_str = since_date.strftime("%Y-%m-%d")
            day_query = "%s since:%s until:%s" % (query, since_date_str, until_date.strftime("%Y-%m-%d"))
            tp.submit(self.perform_search, day_query, since_date_str)
        tp.shutdown(wait=True)

    def save_tweets(self, tweets, since_date_str):
        """
        Saves tweets to file in json format
        :return: True until we have reached the max tweets to save, False otherwise
        """

        for tweet in tweets:
            # Determine the hour of the tweet to save it in the respective file
            created_at_str = tweet['created_at']
            tweet_time = time.strptime(created_at_str, DATE_FORMAT)  # 'Wed Apr 11 23:59:59 +0000 2018'
            tweet_hour_str = str(tweet_time.tm_hour)

            # Initialize auxiliary data structures, if needed
            if since_date_str not in self.jsonl_files_dicts:
                self.jsonl_files_dicts[since_date_str] = {}
                self.jsonl_files_dicts[since_date_str][tweet_hour_str] = {}
                # Create new file for the first hour
                filename = '{0}.{1}.{2}.{3}'.format(self.filepath, since_date_str, tweet_hour_str, 'jsonl')
                self.jsonl_files_dicts[since_date_str][tweet_hour_str] = io.open(filename, 'w', encoding='utf-8')

            if tweet_hour_str not in self.jsonl_files_dicts[since_date_str]:
                # Close the previous file to free resources
                self.close_file(since_date_str, tweet_hour_str)
                # Create new file for the next hour
                filename = '{0}.{1}.{2}.{3}'.format(self.filepath, since_date_str, tweet_hour_str, 'jsonl')
                self.jsonl_files_dicts[since_date_str][tweet_hour_str] = io.open(filename, 'w', encoding='utf-8')

            file = self.jsonl_files_dicts[since_date_str][tweet_hour_str]

            # Lets add a counter so we only collect a max number of tweets
            self.counter_lock.acquire()
            self.counter += 1
            self.counter_lock.release()

            data = json.dumps(tweet, ensure_ascii=False)
            file.write(data + '\n')

            self.counter_lock.acquire()
            if self.counter % SCRAPING_RATE == 0:
                self.logger.info("%s : %i items saved to file.", self.filepath, self.counter)
            self.counter_lock.release()

            # When we've reached our max limit, return False so collection stops
            self.counter_lock.acquire()
            if self.counter >= self.limit:
                self.counter_lock.release()
                return False
            self.counter_lock.release()
        return True

    def close_file(self, since_date_str, tweet_hour_str):
        previous_hour = str(int(tweet_hour_str) + 1)
        self.jsonl_files_dicts[since_date_str][previous_hour].close()
        del self.jsonl_files_dicts[since_date_str][previous_hour]
