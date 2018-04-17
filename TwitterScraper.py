#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import io
import sys
import argparse
import requests
from requests.exceptions import HTTPError
import json
import datetime
from os import path
from abc import ABCMeta, abstractmethod
from urllib.parse import urlencode
from urllib.parse import urlunparse
from bs4 import BeautifulSoup
import time
import logging
from fake_useragent import UserAgent, settings as fake_useragent_settings
from concurrent.futures import ThreadPoolExecutor
from threading import Lock  # TODO - Improvement: Don't use locks
import functools
from mem_top import mem_top


__author__ = 'Tom Dickinson, Flavio Martins, David Semedo, Gustavo Goncalves'


#  TODO The code is no saving to different files (flushing?) producing a memory leak. Maybe the locks are to blame.
#  TODO we have to re-query after 50000 in the case of having more tweets to download.
#  TODO Update the filters by redoing the query after a predefined time-interval. (4 hours?)
#  TODO create a 1 month recursive search after comparing the scraping with twarc.

logger = logging.getLogger(__name__)

# TODO is there any benefit for this rate delay be random generated?
DEFAULT_RATE_DELAY = 0.0
DEFAULT_ERROR_DELAY = 5.0
DEFAULT_LIMIT = 50000
MAX_RETRIES_SESSION = 5
MAX_RETRIES = MAX_RETRIES_SESSION*5
SCRAPING_RATE = 100
DEFAULT_NUM_THREADS = 8
DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"  # "Fri Mar 29 11:03:41 +0000 2013";


class TwitterSearch:

    __metaclass__ = ABCMeta

    def __init__(self, session, rate_delay, error_delay, useragent_cache_path=fake_useragent_settings.DB):
        """
        :param rate_delay: How long to pause between calls to Twitter
        :param error_delay: How long to pause when an error occurs
        """
        self.session = session
        self.rate_delay = rate_delay
        self.error_delay = error_delay
        self.UA = UserAgent(path=useragent_cache_path)

    def perform_search(self, query, since_date_str): # TODO: I was here about to add recursive query for scraping with more than 50000 tweets
        """
        Scrape items from twitter
        :param query:   Query to search Twitter with. Takes form of queries constructed with using Twitters
                        advanced search: https://twitter.com/search-advanced
        :param since_date_str: string of the day that will be searched.
        """
        url = self.construct_url(query)
        continue_search = True
        min_tweet = None

        response = self.execute_search(url)
        while response is not None and continue_search and response['items_html'] is not None:
            tweets = self.parse_tweets(response['items_html'])

            # If we have no tweets, then we can break the loop early
            if len(tweets) == 0:
                break

            # If we haven't set our min tweet yet, set it now
            if min_tweet is None:
                min_tweet = tweets[0]

            # print(mem_top(verbose_types=[dict, list]))

            continue_search = self.save_tweets(tweets, since_date_str)

            # Our max tweet is the last tweet in the list
            max_tweet = tweets[-1]
            if min_tweet['id_str'] is not max_tweet['id_str']:
                max_position = "TWEET-%s-%s" % (max_tweet['id_str'], min_tweet['id_str'])
                url = self.construct_url(query, max_position=max_position)
                # Sleep for our rate_delay
                time.sleep(self.rate_delay)
                response = self.execute_search(url)

    def execute_search(self, url, retry_num=0, error_num=0):
        """
        Executes a search to Twitter for the given URL
        :param url: URL to search twitter with
        :param retry_num: Retry number of current function call
        :param error_num: HTTP error number of current function call
        :return: A JSON object with data from Twitter
        """
        try:
            response = self.session.get(url)
            response.raise_for_status()  # raise on any HTTPError
            data = response.json()
            return data
        except HTTPError as e:
            error_num += 1
            # 400 Bad Request
            if e.response.status_code == 400:
                logger.warning("HTTP 400 received")
                logger.error(e.response.json())  # TODO check if this e.response works as expected
            # 429 Too many requests
            elif e.response.status_code == 429:
                logger.warning("HTTP 429 - Too many requests")
                reset = int(e.response.headers['x-rate-limit-reset'])
                logger.debug("Reset time: %s", str(reset))
                seconds = reset + (error_num + retry_num) * self.error_delay
                logger.warning("Going to sleep for %s seconds.", str(seconds))
                time.sleep(seconds)
            else:
                # If we get a HTTPError exception due to a request timing out, we sleep for our error delay, then make
                # another attempt
                logger.debug(e.response.status_code)
                logger.debug(e.response.json())
                logger.info("Sleeping for %i", self.error_delay)
                time.sleep(self.error_delay)
                if retry_num % MAX_RETRIES_SESSION == 0 and retry_num > 0:
                    headers = {'user-agent': self.UA.random}
                    self.session = requests.session()
                    self.session.headers.update(headers)
                elif retry_num == MAX_RETRIES:
                    return None
                return self.execute_search(url, retry_num + 1, error_num)

    @staticmethod
    def parse_tweets(items_html):
        """
        Parses Tweets from the given HTML
        :param items_html: The HTML block with tweets
        :return: A JSON list of tweets
        """
        soup = BeautifulSoup(items_html, "html.parser")
        tweets = []
        for li in soup.find_all("li", class_='js-stream-item'):

            # If our li doesn't have a tweet-id, we skip it as it's not going to be a tweet.
            if 'data-item-id' not in li.attrs:
                continue

            tweet = {
                'text': None,
                'id_str': li['data-item-id'],
                'id': int(li['data-item-id']),
                'epoch': None,
                'created_at': None,
                'retweet_count': 0,
                'favorite_count': 0,
                'user': {
                    'id': None,
                    'id_str': None,
                    'screen_name': None,
                    'name': None,
                },
            }

            # Tweet Text
            text_p = li.find("p", class_="tweet-text")
            if text_p is not None:
                for a in text_p.find_all('a'):
                    a_text = a.text
                    if re.match("^https?://", a_text):
                        a.replace_with(" %s" % a_text)
                tweet['text'] = text_p.get_text()

            # Tweet User ID, User Screen Name, User Name
            user_details_div = li.find("div", class_="tweet")
            if user_details_div is not None:
                tweet['user']['id_str'] = user_details_div['data-user-id']
                tweet['user']['id'] = int(user_details_div['data-user-id'])
                tweet['user']['screen_name'] = user_details_div['data-screen-name']
                tweet['user']['name'] = user_details_div['data-name']

            # Tweet date
            date_span = li.find("span", class_="_timestamp")
            if date_span is not None:
                tweet['epoch'] = int(date_span['data-time'])

            if tweet['epoch'] is not None:
                t = datetime.datetime.utcfromtimestamp((tweet['epoch']))
                tweet['created_at'] = t.strftime(DATE_FORMAT)

            # Tweet Retweets
            retweet_span = li.select("span.ProfileTweet-action--retweet > span.ProfileTweet-actionCount")
            if retweet_span is not None and len(retweet_span) > 0:
                tweet['retweet_count'] = int(retweet_span[0]['data-tweet-stat-count'])
                tweet['retweeted'] = tweet['retweet_count'] > 0

            # Tweet Favourites
            favorite_span = li.select("span.ProfileTweet-action--favorite > span.ProfileTweet-actionCount")
            if favorite_span is not None and len(favorite_span) > 0:
                tweet['favorite_count'] = int(favorite_span[0]['data-tweet-stat-count'])
                tweet['favorited'] = tweet['favorite_count'] > 0

            tweets.append(tweet)
        return tweets

    @staticmethod
    def construct_url(query, max_position=None):
        """
        For a given query, will construct a URL to search Twitter with
        :param query: The query term used to search twitter
        :param max_position: The max_position value to select the next pagination of tweets
        :return: A string URL
        """

        params = {
            # Type Param
            'f': 'tweets',
            # Query Param
            'q': query
        }

        # If our max_position param is not None, we add it to the parameters
        if max_position is not None:
            params['max_position'] = max_position

        url_tupple = ('https', 'twitter.com', '/i/search/timeline', '', urlencode(params), '')
        return urlunparse(url_tupple)

    @abstractmethod
    def save_tweets(self, tweets, since_date_str):
        """
        An abstract method that's called with a list of tweets.
        When implementing this class, you can do whatever you want with these tweets.
        """


class TwitterSlicer(TwitterSearch):
    """
    Inspired by: https://github.com/simonlindgren/TwitterScraper/blob/master/TwitterSucker.py
    The concept is to have an implementation that actually splits the query into multiple days.
    The only additional parameters a user has to input, is a minimum date, and a maximum date.
    This method also supports parallel scraping.
    """
    def __init__(self, session, rate_delay, error_delay, since, until, limit, filepath, useragent_cache_path,
                 n_threads=1):
        super(TwitterSlicer, self).__init__(session, rate_delay, error_delay, useragent_cache_path)
        self.since = since
        self.until = until
        self.limit = limit
        self.n_threads = n_threads
        self.counter = 0
        self.counter_lock = Lock()
        self.filepath = filepath
        self.jsonl_files_dicts = {}  # Day dict -> List of o -> File

    def search(self, query):
        # Specify a user agent to prevent Twitter from returning a profile card
        headers = {'user-agent': self.UA.random}
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
            future = tp.submit(self.perform_search, day_query, since_date_str)
            # future.add_done_callback(functools.partial(self.close_files, since_date_str))
        tp.shutdown(wait=False)

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
                # TODO [Improvement][Performance][Medium] - clean previous aux data structures data are not being used
                # Close the previous file to free resources
                previous_hour = str(int(tweet_hour_str)+1)
                self.jsonl_files_dicts[since_date_str][previous_hour].close()
                del self.jsonl_files_dicts[since_date_str][previous_hour]
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
                logger.info("%s : %i items saved to file.", self.filepath, self.counter)
            self.counter_lock.release()

            # When we've reached our max limit, return False so collection stops
            self.counter_lock.acquire()
            if self.counter >= self.limit:
                self.counter_lock.release()
                return False
            self.counter_lock.release()
        return True

    def close_files(self, since_date_str):
        for file in self.jsonl_files_dicts[since_date_str].values():
            file.close()

    def close_all_files(self):
        file_day_dicts = self.jsonl_files_dicts
        for since_date_str in file_day_dicts.keys():
            self.close_files(since_date_str)


def twitter_search(search_terms=None, since=None, until=None, accounts=None, rate_delay=DEFAULT_RATE_DELAY,
                   error_delay=DEFAULT_ERROR_DELAY, limit=DEFAULT_LIMIT, output_dir=".", output_file=None,
                   useragent_cache_path=fake_useragent_settings.DB, n_threads=DEFAULT_NUM_THREADS):

    session = requests.Session()

    search_str = ""

    if search_terms:
        search_str = " ".join(search_terms)

    #  if since:
    #     search_str += " since:" + since

    #  if until:
    #      search_str += " until:" + until

    if not accounts:
        if not search_terms:
            logger.error("Nothing to search")
            sys.exit(1)
        elif not output_file:
            logger.error("No output_file specified")
            sys.exit(1)
        else:
            filepath = path.join(output_dir, output_file)
            twit = TwitterSlicer(session, rate_delay, error_delay, since, until, limit, filepath, useragent_cache_path,
                                 n_threads)
            logger.info("Search : %s", search_str)
            twit.search(search_str)
            #  twit.close_all_files() TODO
    else:
        if not path.isdir(output_dir):
            logger.error('Output directory does not exist.')
            sys.exit(1)

        for act in accounts:
            filepath = path.join(output_dir, act + '.jsonl')
            try:
                if path.getsize(filepath) > 0:
                    logger.debug('%s : File already has content.', filepath)
                    continue
            except OSError:
                pass

            twit = TwitterSlicer(session, rate_delay, error_delay, since, until, limit, filepath, useragent_cache_path,
                                 n_threads)
            search_str_from = search_str + " from:" + act
            logger.info("Search : %s", search_str_from)
            try:
                twit.search(search_str_from)
            except:
                logger.error("Unexpected error.")
            twit.close_all_files()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--search", default=[], nargs='+', required=False)
    parser.add_argument('--accounts', nargs='+', required=False)
    parser.add_argument("--since", type=str)
    parser.add_argument("--until", type=str)
    parser.add_argument("--rate_delay", type=int, default=DEFAULT_RATE_DELAY)
    parser.add_argument("--error_delay", type=int, default=DEFAULT_ERROR_DELAY)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--output_dir", type=str, default='.')
    parser.add_argument("--output_file", type=str)
    parser.add_argument("--fake_useragent_cache_path", type=str, default=fake_useragent_settings.DB)
    parser.add_argument("--n_threads", type=int, default=DEFAULT_NUM_THREADS)
    args = parser.parse_args()

    #  since or until: YYYY-MM-DD

    twitter_search(search_terms=args.search, since=args.since, until=args.until, accounts=args.accounts,
                   rate_delay=args.rate_delay, error_delay=args.error_delay, limit=args.limit,
                   output_dir=args.output_dir, output_file=args.output_file,
                   useragent_cache_path=args.fake_useragent_cache_path, n_threads=args.n_threads)
