#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2012 Gustav Arngården

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# From https://github.com/arngarden/TwitterStream/blob/master/TwitterStream.py

import sys, argparse, inspect, time, pycurl, urllib, json, ConfigParser, ujson
import requests, HTMLParser, traceback, shapely.geometry
import oauth2 as oauth

API_ENDPOINT_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'

config = ConfigParser.ConfigParser()
config.read('config.txt')

# Locations are lower left long, lower left lat, upper right long, upper right lat.
# Mostly pretty arbitrarily chosen.
#
# Note! If there is a |coordinates| field in the tweet, that will be tested
# against our parameters here. If not, the |place.bounding_box| will be tested,
# and ANY overlap will match. This means we'll get a ton of tweets that are
# not in Pittsburgh, just because they're very inaccuate and so have a huge
# bounding box.
#
# More info: https://dev.twitter.com/docs/streaming-apis/parameters#locations
# Can use -180,-90,180,90 to get all geotagged tweets.

# Prints out a log including the stack trace, time, and a message, when an
# exception has occurred.
def log_exception(message):
    traceback.print_exc()

# Prints out a log including the time and a message.
def log(message):
    callerframerecord = inspect.stack()[1] # 0=this line, 1=line at caller
    frame = callerframerecord[0]
    info = inspect.getframeinfo(frame)
    line_no = int(info.lineno)
    print '%d, line %d: %s' % (time.time(), line_no, message)

# Reads in the geojson, pulls out the neighborhood that you want, returns it.
def get_shape(neighborhood):
    shapes = ujson.load(open('neighborhoods.json'))['features']
    for shape in shapes:
        if shape['properties']['HOOD'].lower() == neighborhood.lower():
            return shapely.geometry.asShape(shape['geometry'])

# Given a string of text, returns all the foods that are in it. 1 and 2 word
# phrases. Looks them up in the foods.txt file.
foodlist = None
def find_foods(foods_filename, text):
    global foodlist
    foods = []
    if foodlist is None:
        foodlist = [f.strip().lower() for f in open(foods_filename)]
    words = text.split(' ') # TODO not awesome I know
    bigrams = [' '.join(pair) for pair in zip(words, words[1:])]
    foods += [bigram for bigram in bigrams if bigram.lower() in foodlist]
    foods += [word for word in words if word.lower() in foodlist]
    # TODO if you say "ice cream" it shouldn't return ice cream, ice, and cream.
    return foods

class TwitterStream:
    def __init__(self, neighborhood, timeout=False):
        self.set_credentials()

        self.neighborhood = neighborhood
        self.nghd_shape = get_shape(neighborhood)
        self.min_lon = self.nghd_shape.bounds[0]
        self.min_lat = self.nghd_shape.bounds[1]
        self.max_lon = self.nghd_shape.bounds[2]
        self.max_lat = self.nghd_shape.bounds[3]
        self.post_params = {'locations': ','.join([str(n) for n in self.nghd_shape.bounds])}
        self.tweet_col = neighborhood
        self.conn = None
        self.html_parser = HTMLParser.HTMLParser()
        self.buffer = ''
        self.timeout = timeout
        self.num_reconnect = 0
        self.setup_connection()

    def set_credentials(self):
        twitter_cred_name = 'twitter'
        oauth_keys = {'consumer_key': config.get(twitter_cred_name, 'consumer_key'),
                      'consumer_secret': config.get(twitter_cred_name, 'consumer_secret'),
                      'access_token_key': config.get(twitter_cred_name, 'access_token_key'),
                      'access_token_secret': config.get(twitter_cred_name, 'access_token_secret')}
        self.oauth_token = oauth.Token(key=oauth_keys['access_token_key'], secret=oauth_keys['access_token_secret'])
        self.oauth_consumer = oauth.Consumer(key=oauth_keys['consumer_key'], secret=oauth_keys['consumer_secret'])

    def setup_connection(self):
        """ Create persistant HTTP connection to Streaming API endpoint using cURL.
        """
        if self.conn:
            self.conn.close()
            self.buffer = ''
        self.conn = pycurl.Curl()
        # Restart connection if less than 1 byte/s is received during "timeout" seconds
        if isinstance(self.timeout, int):
            self.conn.setopt(pycurl.LOW_SPEED_LIMIT, 1)
            self.conn.setopt(pycurl.LOW_SPEED_TIME, self.timeout)
        self.conn.setopt(pycurl.URL, API_ENDPOINT_URL)
        # self.conn.setopt(pycurl.USERAGENT, USER_AGENT)
        # Using gzip is optional but saves us bandwidth.
        self.conn.setopt(pycurl.ENCODING, 'deflate, gzip')
        self.conn.setopt(pycurl.POST, 1)
        self.conn.setopt(pycurl.POSTFIELDS, urllib.urlencode(self.post_params))
        self.conn.setopt(pycurl.HTTPHEADER, ['Host: stream.twitter.com',
                                             'Authorization: %s' % self.get_oauth_header()])
        # self.handle_tweet is the method that are called when new tweets arrive
        self.conn.setopt(pycurl.WRITEFUNCTION, self.handle_tweet)

        self.conn.setopt(pycurl.VERBOSE, True)

    def get_oauth_header(self):
        """ Create and return OAuth header.
        """
        params = {'oauth_version': '1.0',
                  'oauth_nonce': oauth.generate_nonce(),
                  'oauth_timestamp': int(time.time())}
        req = oauth.Request(method='POST', parameters=params, url='%s?%s' % (API_ENDPOINT_URL,
                                                                             urllib.urlencode(self.post_params)))
        req.sign_request(oauth.SignatureMethod_HMAC_SHA1(), self.oauth_consumer, self.oauth_token)
        return req.to_header()['Authorization'].encode('utf-8')

    def start(self):
        """ Start listening to Streaming endpoint.
        Handle exceptions according to Twitter's recommendations.
        """
        backoff_network_error = 0.25
        backoff_http_error = 5
        backoff_rate_limit = 60
        while True:
            self.setup_connection() # I guess make sure the connection is open?
            try:
                self.conn.perform()
            except Exception as e:
                # Network error, use linear back off up to 16 seconds
                log('Network error: %s' % self.conn.errstr())
                log('Waiting %s seconds before trying again. Num reconnect: %s' % (backoff_network_error, self.num_reconnect))
                time.sleep(backoff_network_error)
                backoff_network_error = min(backoff_network_error + 1, 16)
                self.num_reconnect += 1
                continue
            # HTTP Error
            sc = self.conn.getinfo(pycurl.HTTP_CODE)
            print sc
            if sc == 420:
                # Rate limit, use exponential back off starting with 1 minute
                # and double each attempt
                log('Rate limit, waiting %s seconds' % backoff_rate_limit)
                time.sleep(backoff_rate_limit)
                backoff_rate_limit *= 2
            else:
                # HTTP error, use exponential back off up to 320 seconds
                log('HTTP error %s, %s' % (sc, self.conn.errstr()))
                log('Waiting %s seconds' % backoff_http_error)
                time.sleep(backoff_http_error)
                backoff_http_error = min(backoff_http_error * 2, 320)

    def handle_tweet(self, data):
        """ This method is called when data is received through Streaming endpoint.
        """
        self.buffer += data
        if data.endswith('\r\n') and self.buffer.strip():
            # complete message received
            message = json.loads(self.buffer)
            self.buffer = ''
            msg = ''
            if message.get('limit'):
                log('Rate limiting caused us to miss %s tweets' % (message['limit'].get('track')))
            elif message.get('disconnect'):
                raise Exception('Got disconnect: %s' % message['disconnect'].get('reason'))
            elif message.get('warning'):
                log('Got warning: %s' % message['warning'].get('message'))
            elif message['coordinates'] == None:
                pass # message with no actual coordinates, just a bounding box
            else:
                lon = message['coordinates']['coordinates'][0]
                lat = message['coordinates']['coordinates'][1]
                if lon >= self.min_lon and lon <= self.max_lon and \
                        lat >= self.min_lat and lat <= self.max_lat:
                    point = shapely.geometry.Point(lon, lat)
                    if self.nghd_shape.contains(point):
                        foods = find_foods('drugs.txt', message['text'])
                        # db[self.tweet_col].insert(dict(message))
                        for food in foods:
                            log(food)
                        # log('Got tweet with text: %s' % message.get('text').encode('utf-8'))

        sys.stdout.flush()
        sys.stderr.flush()
        return len(data)
        


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mongo_port', '-m', default=27017, type=int,
        help='Which port MongoDB is on.')
    parser.add_argument('--neighborhood', '-n', required=True)
    args = parser.parse_args()

    # db = pymongo.MongoClient('localhost', args.mongo_port)['tweet']

    print "Getting stream in " + args.neighborhood + " on port " + str(args.mongo_port)

    timestamp = time.time()
    outFile = open('logs/output_drugs_%s.log' % args.neighborhood, 'w')
    sys.stdout = outFile

    ts = TwitterStream(args.neighborhood)
    ts.setup_connection()
    ts.start()
