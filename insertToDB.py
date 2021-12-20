#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import Table, Column, MetaData, ForeignKey, create_engine
from sqlalchemy import Integer, String, Date, DateTime, ARRAY, Boolean, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import mapper
from sqlalchemy.dialects.postgresql import JSONB

def connect(username, password, database, host='localhost', port='5432'):     
 
    engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
    base = declarative_base()
    session = sessionmaker(bind=engine)
    meta = MetaData(bind=engine)  
  
    return engine, meta, base, session 

def insertToDB(data:list, usr='yugachang',pwd='yuga',db='campaigndb',port='5432',host='127.0.0.1')->None:
    engine, meta, Base, Session = connect(usr, pwd, db, host, port)
    # wrap column name and type
    column_name = ['UID', 'version','title','category', 'showUnit','discountInfo',\
                'descriptionFilterHtml','imageUrl','masterUnit','subUnit','supportUnit','otherUnit',\
                'webSales', 'sourceWebPromote', 'comment', 'editModifyDate', 'sourceWebName', \
                'startDate', 'endDate', 'hitRate', 'time', 'location', 'locationName', 'onSales',\
                'price', 'latitude', 'longitude', 'endTime', 'priceinfo', 'city', 'region', 'isOnline']
    column_type = [String, String, String, String, String, String, \
                String, String, ARRAY(String),ARRAY(String),ARRAY(String),ARRAY(String),\
                String, String, String, DateTime, String,\
                DateTime, DateTime, Integer, DateTime, String, String, Boolean,\
                ARRAY(Integer), Float, Float, DateTime, String, String, String, Boolean]

    class Campaign(Base):
        __tablename__ = 'campaign_all'
        id = Column(Integer, primary_key = True, autoincrement=True)
        
    for name, dtype in zip(column_name, column_type):
        setattr(Campaign, name, Column(dtype))
        
    Base.metadata.create_all(engine)

    session = Session()
    for row in data:
        session.add(Campaign(**row))
    session.commit()
