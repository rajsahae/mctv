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
import boto3

CDN_URI = "http://d3mgvwaiuadt8i.cloudfront.net"
MCC_TITLE = "Millbrae City Council Meetings"
THUMBNAIL = "https://upload.wikimedia.org/" + \
        "wikipedia/commons/thumb/f/f8/" + \
        "Aspect-ratio-16x9.svg/2000px-Aspect-ratio-16x9.svg.png"

def add_episode_to_season(full_dict, year, episode):
    '''Add episode to the specified dict'''
    season = next(
        season for season in full_dict["series"][0]["seasons"] if season["seasonNumber"] == int(year)
    )
    if not season['episodes']:
        episode['episodeNumber'] = 1
    else:
        episode['episodeNumber'] = season['episodes'][-1]['episodeNumber'] + 1
    season['episodes'].append(episode)

def new_episode(season, basename, vidtype):
    '''Create and return a new series episode'''
    filename = basename + '.' + vidtype
    return {
        "id": digest(filename),
        "title": basename,
        "content": {
            "dateAdded": datetime.datetime.isoformat(datetime.datetime.now()),
            "videos": [
                {
                    "url": '/'.join([CDN_URI, season, filename]),
                    "quality": "FHD",
                    "videoType": vidtype.upper()
                }
            ],
            "duration": 10800
        },
        "thumbnail": THUMBNAIL,
        "episodeNumber": 1,
        "shortDescription": basename,
        "releaseDate": datetime.datetime.isoformat(datetime.datetime.now())
    }

def new_season(year):
    '''Return a dict for a new season'''
    return {"seasonNumber": int(year), "episodes": []}

def has_season(full_dict, year):
    '''Return True if dict contains season for specified year'''
    return any(map(lambda x: x['seasonNumber'] == int(year), full_dict['series'][0]['seasons']))

def digest(string):
    '''Return sha256 digest of string'''
    return hashlib.sha256(string.encode()).hexdigest()

            {
                "id": digest(MCC_TITLE),
                "tags": ["video", "educational"],
                "releaseDate": "2011-08-01",
                "title": MCC_TITLE,
                "seasons": [],
                "genres": ["educational", "news"],
                "thumbnail": THUMBNAIL,
                "shortDescription": "Video collection of Millbrae city council meetings"
            },

def main():
    '''Main function'''

    json_dict = {
        "providerName": "Millbrae Community Television",
        "lastUpdated": datetime.datetime.isoformat(datetime.datetime.now()),
        "language": "en",
        "movies": [],
        "shortFormVideos": [],
        "tvSpecials": [],
        "series": [],
        "categories": [],
        "playlists": []
    }

    aws_s3 = boto3.resource('s3')
    rokufiles = aws_s3.Bucket('rokufiles')

    type_pat = r'(?P<type>\w+)'
    name_pat = r'(?P<name>\w+)'
    season_pat = r'(?P<season>\d+)'
    filename_pat = r'(?P<filename>[^\.]+)\.(?P<ext>\w+)'

    object_pattern = type_pat + '/' + name_pat + '/' + season_pat + '/' + filename_pat

    for item in rokufiles.objects.all():
        print "processing", item.key
        match = re.search(object_pattern, item.key)
        if match:
            print "Matched groups:", match.groups()
            if not has_season(json_dict, match.group('season')):
                print "Creating new season:", match.group('season')
                current_season = new_season(match.group('season'))
                json_dict["series"][0]["seasons"].append(current_season)

            print "Adding episode", match.group('filename'), "to season", match.group('season')
            current_episode = new_episode(match.group('season'), match.group('filename'), match.group('type'))
            add_episode_to_season(json_dict, match.group('season'), current_episode)
        else:
            print "Match failure:", item.key

    with open("mctv-roku.json", "w") as handle:
        handle.write(json.dumps(json_dict, indent=4))


if __name__ == "__main__":
    main()
