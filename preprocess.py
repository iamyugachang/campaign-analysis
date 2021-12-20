#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
from collections.abc import Iterable
import re
import json
import math
# flatten dict
def transformFlatten(data)->list:
    '''
    Input: list
    Return: list
    '''
    element_showInfo = ['time','location','locationName','onSales','price','latitude','longitude','endTime']
    new_data = []
    for row in data:
        new_row = {}
        for key in row:
            if isinstance(row[key], list):
                stack = [row[key]]
                new_list = []
                while stack:
                    curr = stack.pop()
                    if isinstance(curr, list):
                        stack += curr
                    elif isinstance(curr, dict):
                        for k in curr:
                            new_row[k] = curr[k]
                    else:
                        new_list.append(curr)
                if new_list:
                    new_row[key] = new_list
            else:
                new_row[key] = row[key]
            if (key == 'showInfo') and (not any(row[key])):
                for e in element_showInfo:
                    new_row[e] = None
        new_data.append(new_row)
    return new_data

# Generate columns
def to_dtype(element, dtype):
    if isinstance(element, Iterable):
        if not any(element):
            return None
    if element is None:
        return element
    if dtype == int:
        return int(element)
    elif dtype == float:
        return float(element)
    elif dtype == 'dt_detail':
        return datetime.strptime(element, "%Y/%m/%d %H:%M:%S")
    elif dtype == 'dt':
        return datetime.strptime(element, "%Y/%m/%d")
    elif dtype == bool:
        return (lambda x: True if x == 'Y' else False)(element)
    elif dtype == str:
        tmp = element.replace('\r', ' ').replace('\n', ' ').split()
        return " ".join(tmp)
    elif dtype == list:
        return element
    else:
        return 'dtype not found!'

def transformType(data:list)->list:
    change_to_int = set(['hitRate'])
    change_to_datetime_detail = set(['time', 'endTime','editModifyDate'])
    change_to_datetime = set(['startDate','endDate'])
    change_to_bool = set(['onSales'])
    change_to_float = set(['latitude','longitude'])
    change_list = [change_to_int,change_to_datetime_detail,change_to_datetime,change_to_bool,change_to_float]
    change_dtype = [int, 'dt_detail', 'dt', bool, float]
    
    
    new_data = []
    for row in data:
        new_row = {}
        for key in row:
            target_type = None
            # operation for columns in changing set
            for c,t in zip(change_list,change_dtype):
                if key in c:
                    target_type = t
                    break
            # operation for columns not in changing set
            if not target_type:
                if isinstance(row[key],str):
                    target_type = str
                elif isinstance(row[key],list):
                    target_type = list
            new_row[key] = to_dtype(row[key],target_type)
        new_data.append(new_row)
    return new_data

def finder(regex, text):
    find = re.finditer(regex, text)
    matches = re.findall(regex, text)
    exclusion = set()
    for m,n in [f.span() for f in find]:
        exclusion.update(list(range(m,n)))
    new_text = ''
    for i in range(len(text)):
        if i not in exclusion:
            new_text += text[i]
    return matches, new_text

def transformDetail(data:list)->list:
    new_data = []
    for row in data:
        new_row = {}
        for key in row:
            new_row[key] = row[key]
            
        # Handle detail of price
        new_row['priceinfo'], new_row['price'] = new_row['price'], None
        if new_row['priceinfo'] != None:
            ans = set()
            handle_regex = ['\d+[,]\d+','(?<=[$])\d+','\d+[/]\d+','\d+[/]','\d{2,}(?=[; 元、.，↑（,])']
            text = new_row['priceinfo']+' '
            for ex in handle_regex:
                result, text = finder(ex, text)
                for e in result:
                    if ',' in e:
                        if len("".join(e.split(','))) > 5:
                            for i in e.split(','):
                                ans.add(int(i))
                        else:
                            ans.add(int("".join(e.split(','))))
                    elif '/' in e:
                        if re.search('\d+[/]$',e):
                            ans.add(int(e.split('/')[0]))
                    else:
                        if int(e) < 100000:
                            ans.add(int(e))

            # change column name
            ans = sorted(list(set(ans)))
            ans = [i for i in ans if i > 9]
            new_row['price'] = ans

            # Handle free
            if not ans:
                if re.search('免費',text):
                    new_row['price'] = [0]
                else:
                    new_row['price'] = None

        # Handle county, city
        new_row['city'],new_row['region'] = None, None
        value_addr = (lambda x: '' if not x else x)(new_row['location'])
        value_name = (lambda x: '' if not x else x)(new_row['locationName'])
        total_location = " ".join([value_addr, value_name]).replace('台','臺')
        rough = re.findall('[^ 0-9]{1,2}[區鄉鎮市縣]',total_location)
        # rough = []
        # for e in tmp:
        #     if e not in rough:
        #         rough.append(e)
        if rough:
            if len(rough) >= 2:
                if re.search('[區鄉鎮]',rough[0]):
                    new_row['region'] = rough[0]
                else:
                    new_row['city'],new_row['region'] = rough[0], rough[1]
            else:
                if re.search('[區鄉鎮]',rough[0]):
                    new_row['region'] = rough[0]
                else:
                    new_row['city'] = rough[0]
                # row['city'] = str(rough) -> find out all of them are city       
            # 其餘可以用Google Map API補latitude, longitude，或利用座標搜尋關鍵字

        # Handle Online
        new_row['isOnline'] = False
        if re.search('線上',total_location):
            new_row['isOnline'] = True
                
        new_data.append(new_row)
    return new_data

def preprocess(data:list, load_from_local='campaign_dataset_noduplicate.json')->list:
    if any(load_from_local):
        with open(load_from_local) as f:
            data = json.load(f)
    if not any(data):
        print('Empty data!')
        return False
    data_flat = transformFlatten(data)
    data_newtype = transformType(data_flat)
    data_detail = transformDetail(data_newtype)
    return data_detail