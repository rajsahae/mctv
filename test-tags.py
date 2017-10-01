#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import boto3

client = boto3.client('s3')
tagset = client.get_object_tagging(
    Bucket='rokufiles',
    Key='mctv-s3-millbraecc-old-policy.json'
)

print(tagset['TagSet'])
