# -*- coding: utf-8 -*- 
from __future__ import print_function
from django.core.management.base import BaseCommand


import datetime
import os
import sys
import subprocess
import gzip
import shutil
import sys
import pdb
import re

from wikistats.models import ArticlePerDayStats, ArticleStats

baseUrl="https://dumps.wikimedia.org/other/pageviews/2017/"
fileDownloadsPath="/Users/somebody/wsfun/visio.ai/wikistats"

#Stored all-already processed files in this file, so that we don't have to do them again
statusFile="/Users/somebody/wsfun/visio.ai/wikistats/statusFile.txt"

LOOK_BACK_PERIOD = 10
NUM_HOURS_PER_DAY = 24
TILDA="~"
COMMA=","
COLON=":"
TESTING_NUM_LINES_TO_PARSE=100


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

#Dump the dictionary conatining the final per article, per day stats info
def dumpArticleViewStats(articleInfoDict):
    prev = ""
    for article, val in articleInfoDict.iteritems():
        try:
            article = article.decode('utf8')
            line = "%s ---  %s" % (article, val)
            print (line)
            prev = article
        except Exception as e:
            eprint ("Problem ")
            eprint (e)

def doStuff():
    allArticleStats=dict()
    #Remove everything from the database as we will regenerate it
    ArticleStats.objects.all().delete()
    #return

    #Go over each row in ArticlePerDayStats and sum the values to get an aggregate value
    allArticlesFromDB=ArticlePerDayStats.objects.all()
    for item in allArticlesFromDB.iterator():
        key = item.article
        val = item.perDayViews
        aggregateNumViews = 0;
        #val would be of the form string~<comma seperated values>:<comma seperated values>:....
        #Seperate out the date part and just get the day/hour view counts
        myarray1 = val.split(TILDA)
        #Get daily view counts as a comma separated string
        myarray2 = myarray1[1].split(COLON)
        for mystr in myarray2:
            #mystr is a comma seperated list of values
            myarray3 = mystr.split(COMMA)
            for mystr1 in myarray3:
                if mystr1 != '':
                    aggregateNumViews = aggregateNumViews + int(mystr1)
                

        allArticleStats[key] = aggregateNumViews

    dumpArticleViewStats(allArticleStats)

    for article_, val_ in allArticleStats.iteritems():
        try:
            obj = ArticleStats.objects.get(article=article_)
            if obj.viewCount == val_:
                pass
            else:#if value has changed, update it
                obj.viewCount = val_
                obj.save()
        except ArticleStats.DoesNotExist:
            obj = ArticleStats(article=article_, viewCount=val_) 
            obj.save()

    pass


allArticles=dict()

class Command(BaseCommand):

    def handle(self, **options):
        # now do the things that you want with your models here
        print ("1 somebody is the best")
        #Initialize from database
        doStuff()
        print ("2 somebody is the best")
        sys.exit(0)


