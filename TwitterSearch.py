#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import datetime
import time
import requests
from requests import HTTPError
from abc import ABCMeta, abstractmethod
from urllib.parse import urlencode
from urllib.parse import urlunparse
from bs4 import BeautifulSoup
from fake_useragent import UserAgent, settings as fake_useragent_settings

MAX_RETRIES_SESSION = 5
MAX_RETRIES = MAX_RETRIES_SESSION * 5
DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"  # "Fri Mar 29 11:03:41 +0000 2013"


class TwitterSearch:
    __metaclass__ = ABCMeta

    def __init__(self, logger, session, rate_delay, error_delay, useragent_cache_path=fake_useragent_settings.DB):
        """
        :param rate_delay: How long to pause between calls to Twitter
        :param error_delay: How long to pause when an error occurs
        """
        self.logger = logger
        self.session = session
        self.rate_delay = rate_delay
        self.error_delay = error_delay
        self.UA = UserAgent(path=useragent_cache_path)

    def perform_search(self, query, since_date_str):  # TODO: I was here about to add recursive query for scraping with more than 50000 tweets
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

            continue_search = self.save_tweets(tweets, since_date_str)

            # Our max tweet is the last tweet in the list
            max_tweet = tweets[-1]
            if min_tweet['id_str'] is not max_tweet['id_str']:
                max_position = "TWEET-%s-%s" % (max_tweet['id_str'], min_tweet['id_str'])
                url = self.construct_url(query, max_position=max_position)
                # Sleep for our rate_delay
                time.sleep(self.rate_delay)
                response = self.execute_search(url)

    def execute_search(self, url, retry_num=0):
        """
        Executes a search to Twitter for the given URL
        :param url: URL to search twitter with
        :param retry_num: Retry number of current function call
        :return: A JSON object with data from Twitter
        """
        try:
            response = self.session.get(url)
            data = response.json()
            return data
        except HTTPError as e:
            print("STATUS CODE: "+str(e.response.status_code))
            # 400 Bad Request
            if e.response.status_code == 400:
                self.logger.debug("HTTP 400 - Bad Request")
                self.logger.debug(e.response.json())
                return e.response.json()
            elif e.response.status_code == 429:
                self.logger.debug("HTTP 429 - Too many requests")
                self.logger.debug(e.response.headers)
                reset = int(e.response.headers['x-rate-limit-reset'])
                self.logger.debug("Reset time: %s", str(reset))
                seconds = reset + retry_num * self.error_delay
                self.logger.debug("Going to sleep for %s seconds.", str(seconds))
                time.sleep(seconds)
            else:
                # If we get a HTTP Error due to a request timing out, we sleep for our error delay, then make
                # another attempt
                self.logger.info("Sleeping for %i", self.error_delay)
                time.sleep(self.error_delay)
                if retry_num % MAX_RETRIES_SESSION == 0 and retry_num > 0:
                    headers = {'user-agent': self.UA.random}
                    self.session = requests.session()
                    self.session.headers.update(headers)
                elif retry_num == MAX_RETRIES:
                    return None

                return self.execute_search(url, retry_num + 1)


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
