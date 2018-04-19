#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import argparse
import requests
from os import path
import logging
from fake_useragent import settings as fake_useragent_settings
from TwitterSlicer import TwitterSlicer


__author__ = 'Tom Dickinson, Flavio Martins, David Semedo, Gustavo Goncalves'

#  TODO - [Improvement][High] we have to re-query after 50000 in the case of having more tweets to download.
#  TODO - [Improvement][Medium] Update the filters by redoing the query after a predefined time-interval. (4 hours?)
#  TODO - [Improvement][Low] create a 1 month recursive search after comparing the scraping with twarc.
#  TODO - [NTH][Low] update the hour by hour file management data structures

# TODO is there any benefit for this rate delay be random generated?
DEFAULT_RATE_DELAY = 0.0
DEFAULT_ERROR_DELAY = 5.0
DEFAULT_LIMIT = 50000
DEFAULT_NUM_THREADS = 8


def twitter_search(search_terms=None, since=None, until=None, accounts=None, rate_delay=DEFAULT_RATE_DELAY,
                   error_delay=DEFAULT_ERROR_DELAY, limit=DEFAULT_LIMIT, output_dir=".", output_file=None,
                   useragent_cache_path=fake_useragent_settings.DB, n_threads=DEFAULT_NUM_THREADS):
    logger = logging.getLogger(__name__)

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
            twit = TwitterSlicer(logger, session, rate_delay, error_delay, since, until, limit, filepath,
                                 useragent_cache_path,
                                 n_threads)
            logger.info("Search : %s", search_str)
            twit.search(search_str)
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

            twit = TwitterSlicer(logger, session, rate_delay, error_delay, since, until, limit, filepath,
                                 useragent_cache_path,
                                 n_threads)
            search_str_from = search_str + " from:" + act
            logger.info("Search : %s", search_str_from)
            try:
                twit.search(search_str_from)
            except:
                logger.error("Unexpected error.")


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
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
