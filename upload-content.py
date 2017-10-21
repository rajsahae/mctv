#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import click
import boto3

@click.command()

@click.option(
    '--video',
    help='Local file path for video file to upload',
    prompt='Please enter the video local file path')

@click.option(
    '--destination',
    help='Relative path in the rokufiles bucket to put the video',
    prompt='Enter the relative destination for the video in the rokufiles bucket')

@click.option(
    '--duration',
    help='The duration of the video in seconds',
    prompt='Length of the video in seconds')

@click.option(
    '--shortdesc',
    help='Short description (200 chars max) of the content',
    prompt='Please enter a short description of the content (200 chars max)')

@click.option(
    '--thumbnail',
    help='Local file path or URL of thumbnail for content. Must be minimum 800x450 WxH, 16x9 aspect ratio',
    prompt='Please enter a thumbnail file path or URL (800x450 min, 16x9 aspect ratio)')

def upload_content(video, destination, duration, shortdesc, thumbnail):

    print("Uploading video")

    vidbasename = os.path.basename(video)

    args = {
        'ACL': 'public-read',
    }

    client = boto3.client('s3')
    response = client.upload_file(
        video,
        'rokufiles',
        destination + '/' + vidbasename,
        ExtraArgs=args)

    print(response)

    if not thumbnail.startswith("http"):
        print("Uploading thumbnail")
        basename = os.path.basename(thumbnail)
        response = client.upload_file(
            thumbnail,
            'rokufiles',
            destination + '/' + basename,
            ExtraArgs=args)
        print(response)
        thumbnail = "https://s3-us-west-1.amazonaws.com/rokufiles/" + destination + "/" + basename

    print("Tagging video")
    response = client.put_object_tagging(
        Bucket='rokufiles',
        Key=destination + '/' + vidbasename,
        Tagging={
            'TagSet': [
                {
                    'Key': 'duration',
                    'Value': duration
                },
                {
                    'Key': 'shortDescription',
                    'Value': shortdesc
                },
                {
                    'Key': 'thumbnail',
                    'Value': thumbnail
                },
            ]
        }
    )

    print(response)


if __name__ == '__main__':
    upload_content()
