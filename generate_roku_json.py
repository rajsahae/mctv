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
DEFAULT_DURATION = 10800 # 3 hours in seconds

TYPE_PAT = r'^(?P<type>\w+)'
NAME_PAT = r'(?P<name>[\w\s]+)'
SEASON_PAT = r'(?P<season>\d+)'
FILENAME_PAT = r'(?P<filename>[^\.]+)\.(?P<ext>\w+)$'
TITLE_PAT = r'(?P<prefix>[^\d]+)(?P<date>\d{6})$'

TITLES = { 
    "default" : "Millbrae City Council Meeting",
    "MCC" : "Millbrae City Council Meeting",
    "MPC" : "Millbrae Planning Commission Meeting",
    "MCC_YouthGov_" : "Millbrae Youth and Government Meeting"
}

EPISODE_THUMBNAILS = {
    "default" : "https://s3-us-west-1.amazonaws.com/rokufiles/Roku+City+Council.png",
    "MCC" : "https://s3-us-west-1.amazonaws.com/rokufiles/Roku+City+Council.png",
    "MPC" : "https://s3-us-west-1.amazonaws.com/rokufiles/Roku+Planning+Commission.png",
    "MCC_YouthGov_" : "https://s3-us-west-1.amazonaws.com/rokufiles/Roku+Special+Events+and+Meetings.jpg",
}

SERIES_THUMBNAILS = {
    "default" : "https://s3-us-west-1.amazonaws.com/rokufiles/Roku+City+Council.png",
    "Millbrae City Council Meetings" : "https://s3-us-west-1.amazonaws.com/rokufiles/Roku+City+Council.png",
    "Millbrae Planning Commission Meetings" : "https://s3-us-west-1.amazonaws.com/rokufiles/Roku+Planning+Commission.png",
    "Millbrae Special Meetings" : "https://s3-us-west-1.amazonaws.com/rokufiles/Roku+Special+Events+and+Meetings.jpg",
}

def get_release_date(filename):
    match = re.search(TITLE_PAT, filename)
    if not match:
        return '2017-01-01'

    datestring = match.group('date')
    year = int('20' + datestring[0:2])
    month = int(datestring[2:4])
    day = int(datestring[4:6])

    return datetime.date(year, month, day).isoformat()[:10]


def get_title(filename):
    match = re.search(TITLE_PAT, filename)
    if not match:
        return filename

    prefix = match.group('prefix')
    if prefix in TITLES:
        prefix = TITLES[prefix]

    datestring = match.group('date')
    year = int('20' + datestring[0:2])
    month = int(datestring[2:4])
    day = int(datestring[4:6])

    suffix = datetime.date(year, month, day).strftime('%b %d, %Y')

    return suffix

def get_series_thumbnail(title):
    if title in SERIES_THUMBNAILS:
        return SERIES_THUMBNAILS[title]

    return SERIES_THUMBNAILS["default"]

def get_episode_thumbnail(filename):
    match = re.search(TITLE_PAT, filename)
    if not match:
        return EPISODE_THUMBNAILS["default"]

    prefix = match.group('prefix')
    if prefix in EPISODE_THUMBNAILS:
        return EPISODE_THUMBNAILS[prefix]

    return EPISODE_THUMBNAILS["default"]

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
    value = _get_tag(client, item, 'shortDescription')
    if value is not None:
        print "Tagging short description", value
        return value
    else:
        return None

def get_long_description(client, item):
    value = _get_tag(client, item, 'longDescription')
    if value is not None:
        print "Tagging long description", value
        return value

    value = get_short_description(client, item)
    if value is not None:
        print "Tagging long description", value
        return value

    return None

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
    short_title = get_title(basename)

    return {
        "id": digest(filename),
        "title": short_title,
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
        "shortDescription": shortDesc or short_title,
        "longDescription": longDesc or short_title,
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
        "releaseDate": "2017-01-01",
        "title": title,
        "seasons": [],
        "genres": ["educational", "news"],
        "thumbnail": get_series_thumbnail(title),
        "shortDescription": title,
        "longDescription": title
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
                    get_episode_thumbnail(filename),
                    get_short_description(client, item),
                    get_long_description(client, item),
                    get_release_date(filename))

            add_episode_to_content(content, season, new_episode)

    with open("mctv-roku.json", "w") as handle:
        handle.write(json.dumps(feed, indent=4))


if __name__ == "__main__":
    main()
