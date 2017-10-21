#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Generates a roku json file for all files in s3://rokufiles/ bucket
Upload the resulting json file to S3 with:
aws s3 cp mctv-roku.json s3://rokufiles/ --acl public-read
'''

import datetime
import hashlib
import json
import re
import urllib
import boto3

CDN_URI = "http://d3mgvwaiuadt8i.cloudfront.net"
MCC_TITLE = "Millbrae City Council Meetings"
DEFAULT_THUMBNAIL = "https://s3-us-west-1.amazonaws.com/rokufiles/Roku+City+Council.png"
DEFAULT_DURATION = 10800 # 3 hours in seconds
DEFAULT_SHORT_DESCRIPTION = "A regular meeting of the Millbrae City Council"
DEFAULT_LONG_DESCRIPTION = "A regular meeting of the Millbrae City Council"

TYPE_PAT = r'^(?P<type>\w+)'
NAME_PAT = r'(?P<name>[\w\s]+)'
SEASON_PAT = r'(?P<season>\d+)'
FILENAME_PAT = r'(?P<filename>[^\.]+)\.(?P<ext>\w+)$'

def get_thumbnail(item):
    # hardcoded for now
    return DEFAULT_THUMBNAIL

def get_last_modified_date(item):
    return item.last_modified.isoformat()[:10]

def get_last_modified_datetime(item):
    return item.last_modified.isoformat()

def get_duration(client, item):
    value = _get_tag(client, item, 'duration')
    if value is not None:
        print "Tagging duration", value
        return int(value)
    else:
        return DEFAULT_DURATION

def get_short_description(client, item):
    return DEFAULT_SHORT_DESCRIPTION

def get_long_description(client, item):
    return DEFAULT_LONG_DESCRIPTION

def _get_tag(client, item, key):
    tagset = client.get_object_tagging(Bucket='rokufiles', Key=item.key)
    # print(repr(tagset['TagSet']))
    for tag in tagset['TagSet']:
        if tag['Key'] == key:
            return tag['Value']
    return None

def add_episode_to_content(content, year, episode):
    '''Add episode to the specified dict'''
    season = next(
        season for season in content["seasons"] if season["seasonNumber"] == int(year)
    )
    if not season['episodes']:
        episode['episodeNumber'] = 1
    else:
        episode['episodeNumber'] = season['episodes'][-1]['episodeNumber'] + 1
    season['episodes'].append(episode)

def create_episode(ctype, title, season, basename, vidtype, datetimeAdded, duration, thumbnail, shortDesc, longDesc, dateAdded):
    '''Create and return a new series episode'''
    filename = basename + '.' + vidtype
    return {
        "id": digest(filename),
        "title": basename,
        "content": {
            "dateAdded": datetimeAdded,
            "videos": [
                {
                    "url": CDN_URI + "/" + urllib.quote('/'.join([ctype, title, season, filename])),
                    "quality": "FHD",
                    "videoType": vidtype.upper()
                }
            ],
            "duration": duration
        },
        "thumbnail": thumbnail,
        "episodeNumber": 1,
        "shortDescription": shortDesc,
        "longDescription": longDesc,
        "releaseDate": dateAdded
    }

def create_season(year):
    '''Return a dict for a new season'''
    return {"seasonNumber": int(year), "episodes": []}

def has_content_type(feed, ctype, title):
    '''Return content if dict contains content type with specified name'''
    try:
        return (content for content in feed[ctype] if content['title'] == title).next()
    except StopIteration:
        return None

def has_season(content, year):
    '''Return True if dict contains season for specified year'''
    return any(map(lambda x: x['seasonNumber'] == int(year), content['seasons']))

def insert_new_content(feed, ctype, title):
    if ctype == "series":
        content = new_series(title)
    feed[ctype].append(content)
    return content

def digest(string):
    '''Return sha256 digest of string'''
    return hashlib.sha256(string.encode()).hexdigest()

def new_series(title):
    return {
        "id": digest(title),
        "tags": ["video", "educational", title],
        "releaseDate": "2016-01-01",
        "title": title,
        "seasons": [],
        "genres": ["educational", "news"],
        "thumbnail": DEFAULT_THUMBNAIL,
        "shortDescription": "Short description of " + title,
        "longDescription": "Long description for " + title
    }

def main():
    '''Main function'''

    feed = {
        "providerName": "Millbrae Community Television",
        "lastUpdated": datetime.datetime.isoformat(datetime.datetime.now()),
        "language": "en",
        "series": [],
        # "movies": [],
        # "shortFormVideos": [],
        # "tvSpecials": [],
        # "categories": [],
        # "playlists": []
    }

    aws_s3 = boto3.resource('s3')
    rokufiles = aws_s3.Bucket('rokufiles')
    client = boto3.client('s3')

    object_pattern = TYPE_PAT + '/' + NAME_PAT + '/'

    for item in rokufiles.objects.all():
        match = re.search(object_pattern, item.key)
        if not match:
            print "Failed to match:", item.key
            continue

        print "Matched groups:", match.groups()

        ctype = match.group('type')
        name = match.group('name')

        content = has_content_type(feed, ctype, name)
        if content is None:
            print "Creating new", ctype, name
            content = insert_new_content(feed, ctype, name)

        if ctype == 'series':
            object_pattern = TYPE_PAT + '/' + NAME_PAT + '/' + SEASON_PAT + '/' + FILENAME_PAT
            match = re.search(object_pattern, item.key)

            if not match:
                print "Failed to match:", item.key
                continue

            season = match.group('season')
            filename = match.group('filename')
            extension = match.group('ext')

            if not has_season(content, season):
                print "Creating new season:", season
                new_season = create_season(season)
                content["seasons"].append(new_season)

            print "Adding", name,  "episode", filename, "to season", season

            new_episode = create_episode(
                    ctype,
                    name,
                    season,
                    filename,
                    extension,
                    get_last_modified_datetime(item),
                    get_duration(client, item),
                    get_thumbnail(item),
                    get_short_description(client, item),
                    get_long_description(client, item),
                    get_last_modified_date(item))

            add_episode_to_content(content, season, new_episode)

    with open("mctv-roku.json", "w") as handle:
        handle.write(json.dumps(feed, indent=4))


if __name__ == "__main__":
    main()
