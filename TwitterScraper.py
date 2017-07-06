#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import sys
import argparse
import requests
from requests.exceptions import HTTPError
import six
import json
import datetime
from os import path
from abc import ABCMeta, abstractmethod

try:
    from urllib.parse import urlencode
    from urllib.parse import urlunparse
except ImportError:
    from urllib import urlencode
    from urlparse import urlunparse
from bs4 import BeautifulSoup
from time import sleep
import logging
from fake_useragent import UserAgent

__author__ = 'Tom Dickinson, Flavio Martins'

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

# Only needed in case user additional details are required
# If that is the case, the library requires the package python-twitter
TWITTER_REST_API_CONSUMER_KEY = ""
TWITTER_REST_API_CONSUMER_SECRET = ""
TWITTER_REST_API_ACCESS_TOKEN = ""
TWITTER_REST_API_ACCESS_TOKEN_SECRET = ""

DEFAULT_RATE_DELAY = 0
DEFAULT_ERROR_DELAY = 5
DEFAULT_LIMIT = 50000
MAX_RETRIES_SESSION = 5
MAX_RETRIES = MAX_RETRIES_SESSION*5
PROGRESS_PER = 100
DEFAULT_TARGET_TYPE = "tweets"
DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y"  # "Fri Mar 29 11:03:41 +0000 2013";
UA = UserAgent(fallback='Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0')


class TwitterSearch:
    __metaclass__ = ABCMeta

    def __init__(self, session, rate_delay, error_delay=5):
        """
        :param rate_delay: How long to pause between calls to Twitter
        :param error_delay: How long to pause when an error occurs
        """
        self.session = session
        self.rate_delay = rate_delay
        self.error_delay = error_delay

    def search(self, query, target_type, **kwargs):
        """
        Scrape items from twitter
        :param query:   Query to search Twitter with. Takes form of queries constructed with using Twitters
                        advanced search: https://twitter.com/search-advanced
        :param target_type:    Can be "tweets" or "users"
        """
        url = self.construct_url(query, target_type=target_type, language=kwargs['language'])
        continue_search = True
        min_item = None

        # Initialize search function wrapper according to the target type
        parse_tweets_fn = self.parse_tweets if target_type == DEFAULT_TARGET_TYPE else self.parse_users

        response = self.execute_search(url)
        while response is not None and continue_search and response['items_html'] is not None:
            items = parse_tweets_fn(response['items_html'])

            # Check if we should collect additional user details
            if target_type == "users" and kwargs["user_stats"]:
                self.retrieve_user_details(items)

            # If we have no items, then we can break the loop early
            if len(items) == 0:
                break

            continue_search = self.save_items(items)

            max_item = response["min_position"]

            if min_item is not max_item:
                url = self.construct_url(query, target_type=target_type, max_position=max_item,
                                         language=kwargs['language'])
                # Sleep for our rate_delay
                sleep(self.rate_delay)
                response = self.execute_search(url)
                min_item = max_item

    def execute_search(self, url, retry_num=0):
        """
        Executes a search to Twitter for the given URL
        :param url: URL to search twitter with
        :param retry_num: Retry number of current function call
        :return: A JSON object with data from Twitter
        """
        try:
            logger.info("URL: " + url)
            response = self.session.get(url)
            response.raise_for_status()  # raise on any HTTPError
            data = response.json()
            return data
        # If we get a HTTPError exception due to a request timing out, we sleep for our error delay, then make
        # another attempt
        except HTTPError as e:
            # 400 Bad Request
            if e.response.status_code == 400:
                return response.json()
            else:
                logger.info("Sleeping for %i", self.error_delay)
                sleep(self.error_delay)
                if retry_num % MAX_RETRIES_SESSION == 0 and retry_num > 0:
                    headers = {'user-agent': UA.random}
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
                tweet['text'] = text_p.get_text()

            # Tweet User ID, User Screen Name, User Name
            user_details_div = li.find("div", class_="tweet")
            if user_details_div is not None:
                tweet['user']['id_str'] = user_details_div['data-user-id']
                tweet['user']['id'] = int(user_details_div['data-user-id'])
                tweet['user']['screen_name'] = user_details_div['data-screen-name']
                tweet['user']['name'] = user_details_div['data-name']

            # Twitter image
            image_field = li.find("div", class_="AdaptiveMedia-photoContainer")
            if image_field:
                tweet['image-url'] = image_field['data-image-url']

            # Twitter video
            twitter_video_field = li.find("div", class_="AdaptiveMedia-videoContainer")
            if twitter_video_field:
                tweet['video-url'] = "https://twitter.com/i/videos/tweet/%s" % tweet["id_str"]

            media_card_container = li.find("div", class_="js-media-container")
            if (twitter_video_field is None) and (image_field is None) and media_card_container:

                media_card_type = media_card_container.get("data-card2-name", None)
                if media_card_type:   # Only care about media. Ignore Tweet Quotes, etc.

                    if "summary" in media_card_type:  # Expanded URL w/ image
                        iframe_container = media_card_container.find("div", class_="js-macaw-cards-iframe-container")
                        tweet['expanded_url_card'] = "https://twitter.com" + iframe_container['data-src']
                        tweet['expanded_url'] = li.find("a", class_="twitter-timeline-link")['data-expanded-url']

                    if "player" in media_card_type:  # Embedded video
                        tweet['video-url'] = li.find("a", class_="twitter-timeline-link")['data-expanded-url']

                    # else: it's some other element that we do not care (e.g. a poll)

            # Tweet date
            date_span = li.find("span", class_="_timestamp")
            if date_span is not None:
                tweet['epoch'] = int(date_span['data-time'])

            if tweet['epoch'] is not None:
                t = datetime.datetime.fromtimestamp((tweet['epoch']))
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
    def parse_users(items_html):
        """
        Parses Users from the given HTML
        :param items_html: The HTML block with items
        :return: A JSON list of items
        """
        soup = BeautifulSoup(items_html, "html.parser")
        items = []
        for div in soup.find_all("div", class_='js-stream-item'):

            # If our li doesn't have a tweet-id, we skip it as it's not going to be a tweet.
            if 'data-item-id' not in div.attrs:
                continue

            user = {
                'bio': None,
                'id_str': div['data-item-id'],
                'id': int(div['data-item-id']),
                'screen_name': None,
                'name': None,
            }

            # User Bio
            text_p = div.find("p", class_="ProfileCard-bio")
            if text_p is not None:
                user['bio'] = text_p.get_text()

            # Tweet User ID, User Screen Name, User Name
            user_details_div = div.find("div", class_="user-actions")
            if user_details_div is not None:
                user['screen_name'] = user_details_div['data-screen-name']
                user['name'] = user_details_div['data-name']

            user_fields_div = div.find("div", class_="ProfileCard-userFields")
            user_verified_span = user_fields_div.find("span", class_="Icon--verified")
            user['verified'] = True if user_verified_span else False

            items.append(user)
        return items

    @staticmethod
    def construct_url(query, target_type, max_position=None, language=None):
        """
        For a given query, will construct a URL to search Twitter with
        :param query: The query term used to search twitter
        :param target_type:    Can be "tweets" or "users"
        :param max_position: The max_position value to select the next pagination of items
        :param language: Specifies a language to filter search results
        :return: A string URL
        """
        params = {
            # Type Param
            'f': target_type,
            # Query Param
            'q': query,
            'max_position': max_position
        }

        if language:
            params['l'] = language

        url_tupple = ('https', 'twitter.com', '/i/search/timeline', '', urlencode(params), '')
        return urlunparse(url_tupple)

    @staticmethod
    def construct_user_url(query, target_type, max_position=None):
        """
        For a given query, will construct a URL to search Twitter with
        :param query: The query term used to search twitter
        :param target_type:    Can be "tweets" or "users"
        :param max_position: The max_position value to select the next pagination of items
        :return: A string URL
        """
        params = {
            # Type Param
            'f': target_type,
            # Query Param
            'q': query,
            'max_position': max_position
        }

        url_tupple = ('https', 'twitter.com', '/i/search/timeline', '', urlencode(params), '')
        return urlunparse(url_tupple)

    @abstractmethod
    def save_items(self, items):
        """
        An abstract method that's called with a list of items.
        When implementing this class, you can do whatever you want with these items.
        """

    def retrieve_user_details(self, items):
        """
        For a given set of crawled users, retrieves additional information using the Twitter REST API
        :param items: A list of user dictionarities
        :return: An updated list with additional fields of User dictionaries
        """
        # The user lookup API limit per request is 100
        step = 100

        import twitter
        api = twitter.Api(consumer_key=TWITTER_REST_API_CONSUMER_KEY,
                          consumer_secret=TWITTER_REST_API_CONSUMER_SECRET,
                          access_token_key=TWITTER_REST_API_ACCESS_TOKEN,
                          access_token_secret=TWITTER_REST_API_ACCESS_TOKEN_SECRET)

        for i in range(0, len(items), step):
            statuses = api.UsersLookup(screen_name=[item["screen_name"] for item in items[i:step]])
            for j in range(min(len(statuses), step)):
                items[i + j] = {**items[i + j], **statuses[j].AsDict()}

        return items


class TwitterSearchImpl(TwitterSearch):
    def __init__(self, session, rate_delay, error_delay, max_items, filepath):
        """
        :param rate_delay: How long to pause between calls to Twitter
        :param error_delay: How long to pause when an error occurs
        :param max_items: Maximum number of items to collect for this example
        """
        super(TwitterSearchImpl, self).__init__(session, rate_delay, error_delay)
        self.max_items = max_items
        self.counter = 0
        self.filepath = filepath
        self.jsonl_file = None

    def search(self, query, target_type, **kwargs):
        # Specify a user agent to prevent Twitter from returning a profile card
        headers = {'user-agent': UA.random}
        self.session.headers.update(headers)

        self.jsonl_file = io.open(self.filepath, 'w', encoding='utf-8')
        super(TwitterSearchImpl, self).search(query, target_type=target_type, **kwargs)
        self.jsonl_file.close()

    def save_items(self, items):
        """
        Just prints out items
        :return:
        """
        for item in items:
            # Lets add a counter so we only collect a max number of items
            self.counter += 1

            if six.PY2:
                data = json.dumps(item, ensure_ascii=False, encoding='utf-8')
            else:
                data = json.dumps(item, ensure_ascii=False)

            self.jsonl_file.write(data + '\n')

            if self.counter % PROGRESS_PER == 0:
                logger.info("%s : %i items saved to file.", self.filepath, self.counter)

            # When we've reached our max limit, return False so collection stops
            if self.counter >= self.max_items:
                return False

        return True


def twitter_search(search_terms=None, since=None, until=None, language=None, accounts=None, search_filter=None,
                   target_type=DEFAULT_TARGET_TYPE,
                   rate_delay=DEFAULT_RATE_DELAY, error_delay=DEFAULT_ERROR_DELAY, user_stats=False,
                   limit=DEFAULT_LIMIT,
                   output_dir=".", output_file=None):
    session = requests.Session()

    search_str = ""

    if search_terms:
        search_str = " ".join(search_terms)

    if since:
        search_str += " since:" + since

    if until:
        search_str += " until:" + until

    if search_filter:
        search_str += " filter:" + search_filter

    if not accounts:
        if not search_terms:
            logger.error("Nothing to search")
            sys.exit(1)
        elif not output_file:
            logger.error("No output_file specified")
            sys.exit(1)
        else:
            filepath = path.join(output_dir, output_file)
            twit = TwitterSearchImpl(session, rate_delay, error_delay,
                                     limit, filepath)
            logger.info("Search : %s", search_str)
            twit.search(search_str, target_type=target_type, user_stats=user_stats, language=language)
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

            twit = TwitterSearchImpl(session, rate_delay, error_delay,
                                     limit, filepath)
            search_str_from = search_str + " from:" + act
            logger.info("Search : %s", search_str_from)
            twit.search(search_str_from, target_type=DEFAULT_TARGET_TYPE, language=language)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--search", default=[], nargs='+')
    parser.add_argument("-f", default=DEFAULT_TARGET_TYPE, type=str)
    parser.add_argument("--user_stats", action="store_true", default=False, required=False)
    parser.add_argument('--accounts', nargs='+', required=False)
    parser.add_argument('-l', type=str, required=False)
    parser.add_argument("--filter", type=str)
    parser.add_argument("--since", type=str)
    parser.add_argument("--until", type=str)
    parser.add_argument("--rate_delay", type=int, default=DEFAULT_RATE_DELAY)
    parser.add_argument("--error_delay", type=int, default=DEFAULT_ERROR_DELAY)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--output_dir", type=str, default='.')
    parser.add_argument("--output_file", type=str)
    args = parser.parse_args()

    twitter_search(target_type=args.f, search_terms=args.search, since=args.since, until=args.until, language=args.l,
                   accounts=args.accounts, search_filter=args.filter, rate_delay=args.rate_delay,
                   error_delay=args.error_delay, limit=args.limit,
                   output_dir=args.output_dir, output_file=args.output_file, user_stats=args.user_stats)


if __name__ == '__main__':
    main()