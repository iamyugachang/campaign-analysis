#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from tqdm import tqdm
import urllib.request, json 

def dropDuplicate(tmp_dataset:list, dataset:list, uid_set:set)->None:
    """
      Parameter:
        dataset: list of dictionary
        uid_set: set of uid occured so far
      Return:
        None 
    """
    for element in tmp_dataset:
        if element['UID'] not in uid_set:
            dataset.append(element)
            uid_set.add(element['UID'])

def fetchRaw(savename='campaign_dataset_noduplicate.json',save=True)->list:
    # Read dataset from URL
    data = []
    URL = 'https://cloud.culture.tw/frontsite/trans/SearchShowAction.do?method=doFindTypeJ&category='
    all_dataset = [URL+str(i) for i in range(1,20)]+ [URL+'all'] \
                + ['https://cloud.culture.tw/frontsite/trans/SearchShowAction.do?method=doFindNewResidentTypeJ']
    dataset_issue = 'https://cloud.culture.tw/frontsite/trans/SearchShowAction.do?method=doFindIssueTypeJ'

    for i in tqdm(range(len(all_dataset))):
        with urllib.request.urlopen(all_dataset[i]) as url:
            data += json.loads(url.read().decode())

    with urllib.request.urlopen(dataset_issue) as url:
        data += json.loads(url.read().decode())['issue']
    
    # Drop duplicated data
    UID_set = set()
    data_noduplicate = []
    dropDuplicate(data, data_noduplicate, UID_set)

    # Write out file
    if save:
        with open(savename, 'w') as f:
            json.dump(data_noduplicate, f)

    return data_noduplicate
