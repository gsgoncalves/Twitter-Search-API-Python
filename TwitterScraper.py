import urllib2
import json
import datetime
from abc import ABCMeta
from urllib import urlencode
from abc import abstractmethod
from urlparse import urlunparse
from bs4 import BeautifulSoup
from time import sleep

__author__ = 'Tom Dickinson, Flavio Martins'


DATE_FORMAT = "%a %b %d %H:%M:%S +0000 %Y" # "Fri Mar 29 11:03:41 +0000 2013";


class TwitterSearch:

    __metaclass__ = ABCMeta

    def __init__(self, rate_delay, error_delay=5):
        """
        :param rate_delay: How long to pause between calls to Twitter
        :param error_delay: How long to pause when an error occurs
        """
        self.rate_delay = rate_delay
        self.error_delay = error_delay

    def search(self, query):
        """
        Scrape items from twitter
        :param query:   Query to search Twitter with. Takes form of queries constructed with using Twitters
                        advanced search: https://twitter.com/search-advanced
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

            continue_search = self.save_tweets(tweets)

            # Our max tweet is the last tweet in the list
            max_tweet = tweets[-1]
            if min_tweet['id_str'] is not max_tweet['id_str']:
                max_position = "TWEET-%s-%s" % (max_tweet['id_str'], min_tweet['id_str'])
                url = self.construct_url(query, max_position=max_position)
                # Sleep for our rate_delay
                sleep(self.rate_delay)
                response = self.execute_search(url)

    def execute_search(self, url):
        """
        Executes a search to Twitter for the given URL
        :param url: URL to search twitter with
        :return: A JSON object with data from Twitter
        """
        try:
            # Specify a user agent to prevent Twitter from returning a profile card
            headers = {
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36'
            }
            req = urllib2.Request(url, headers=headers)
            response = urllib2.urlopen(req)
            data = json.loads(response.read())
            return data

        # If we get a ValueError exception due to a request timing out, we sleep for our error delay, then make
        # another attempt
        except ValueError as e:
            print e.message
            print "Sleeping for %i" % self.error_delay
            sleep(self.error_delay)
            return self.execute_search(url)

    @staticmethod
    def parse_tweets(items_html):
        """
        Parses Tweets from the given HTML
        :param items_html: The HTML block with tweets
        :return: A JSON list of tweets
        """
        soup = BeautifulSoup(items_html, "html5lib")
        tweets = []
        for li in soup.find_all("li", class_='js-stream-item'):

            # If our li doesn't have a tweet-id, we skip it as it's not going to be a tweet.
            if 'data-item-id' not in li.attrs:
                continue

            tweet = {
                'text': None,
                'id_str': li['data-item-id'],
                'id': long(li['data-item-id']),
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
                tweet['user']['id'] = long(user_details_div['data-user-id'])
                tweet['user']['screen_name'] = user_details_div['data-screen-name']
                tweet['user']['name'] = user_details_div['data-name']

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
    def save_tweets(self, tweets):
        """
        An abstract method that's called with a list of tweets.
        When implementing this class, you can do whatever you want with these tweets.
        """


class TwitterSearchImpl(TwitterSearch):

    def __init__(self, rate_delay, error_delay, max_tweets):
        """
        :param rate_delay: How long to pause between calls to Twitter
        :param error_delay: How long to pause when an error occurs
        :param max_tweets: Maximum number of tweets to collect for this example
        """
        super(TwitterSearchImpl, self).__init__(rate_delay, error_delay)
        self.max_tweets = max_tweets
        self.counter = 0

    def save_tweets(self, tweets):
        """
        Just prints out tweets
        :return:
        """
        for tweet in tweets:
            # Lets add a counter so we only collect a max number of tweets
            self.counter += 1

            print json.dumps(tweet)

            # When we've reached our max limit, return False so collection stops
            if self.counter >= self.max_tweets:
                return False

        return True


if __name__ == '__main__':
    twit = TwitterSearchImpl(0, 5, 50000)
    twit.search("from:reuters since:2013-02-01 until:2013-02-02")
