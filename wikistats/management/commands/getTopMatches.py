# -*- coding: utf-8 -*- 
from __future__ import print_function
from django.core.management.base import BaseCommand

#Given a string which presents a wikipedia page's title/slug, 
#return a sorted list of all links in the page

import datetime
import os
import sys
import subprocess
import gzip
import shutil
import sys
import pdb
import re
#import wikipedia
from mediawiki import MediaWiki
import logging                                 
import operator
from bs4 import BeautifulSoup
#import wikitextparser as wtp



from wikistats.models import ArticleStats
from wikistats.models import WikiArticles


LOOK_BACK_PERIOD = 10
NUM_HOURS_PER_DAY = 24
TILDA="~"
COMMA=","
COLON=":"
TESTING_NUM_LINES_TO_PARSE=100
DJANGO_BULK_CREATE_BATCH_SIZE=500


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)



def doStuff():
    global allArticleStats
    #Remove everything from the database as we will regenerate it
    #return

    #allArticlesFromDB=ArticleStats.objects.all()
    #for item in allArticlesFromDB.iterator():
    #    key = item.article
    #    val = item.viewCount
    #    allArticleStats[key] = val
    #    for key, val in allArticleStats.iteritems():
    #        try:
    #            l.warning ("key = " + str(key) + " val=" + str(val))
    #        except:
    #            pass

    pass

def covertHtml2Text(html):
    soup = BeautifulSoup(html)

    ## kill all script and style elements
    #for script in soup(["script", "style"]):
    #    script.extract()    # rip it out
    
    # get text
    text = soup.get_text()
    
    ## break into lines and remove leading and trailing space on each
    #lines = (line.strip() for line in text.splitlines())
    ## break multi-headlines into a line each
    #chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    ## drop blank lines
    #text = '\n'.join(chunk for chunk in chunks if chunk)

    return text

def addToKeywordOccurenceMap(title,text,keyword,keywordOccurenenceMap):
    #How many times is this keyword in link's associated wikipedia page
    abc = "START How many times does " + keyword + " come in "
    l.warning (abc)
    count = text.count(keyword)
    abc = "DONE START How many times does " + keyword + " come in "
    l.warning (abc)
    keywordOccurenenceMap[title] = count

    return keywordOccurenenceMap

def getTopMatchesUsingDumbMethod(links, numMatches):
    #statsFromDB=list(ArticleStats.objects.filter(article__in=links))
    unsortedResult = {}
    statsFromDB=ArticleStats.objects.filter(article__in=links).order_by('-viewCount')[:numMatches]
    for stat in statsFromDB:
        unsortedResult[stat.article] = stat.viewCount
        #try:
        #    #l.warning ("stats are " + stat.article + " count=" + str(stat.viewCount))
        #except:
        #    l.warning ("Problem")
         
    return unsortedResult



def getTopMatchesUsingCorrelation(keyword, links, numMatches):
    #Calculate correlation
    #Download each link. For each link, find out how many times, current keyword occurs.

    #how many times does this keyword  in each of its links
    keywordOccurenenceMap = {}
    remainingLinkSet = set(links)
    wikipedia = MediaWiki()

    #First get all links from db/cache
    articlesInCache = WikiArticles.objects.filter(title__in=links)
    for articleInCache in articlesInCache:
        #How many times is this keyword in link's associated wikipedia page
        title=articleInCache.title
        html=articleInCache.text
        text = covertHtml2Text(html)
        #Note that we are using link here and title as first argument
        addToKeywordOccurenceMap(title,text,
                            keyword,keywordOccurenenceMap)
        #Remove from set, so that at the end we know what keyword we should fetch from wikipedia
        remainingLinkSet.remove(articleInCache.title)
        
    
    newWikiArticles = []
    for link in remainingLinkSet:
        
        try:
            l.warning ("analyzing " + link)
        except Exception as e:
            l.warning ("1 rags")

        linkPage = None
        try:
            linkPage = wikipedia.page(link)
        except Exception as e:
            #TODO: Log-error 
            continue
    
        if linkPage is None or linkPage == "":
            raise Exception ("Wikipedia page not found/or is empty for keyword " + link)
        title = linkPage.title
        html = linkPage.html
        text = covertHtml2Text(html)
        #Note that we are using link here and title as first argument
        addToKeywordOccurenceMap(link,text,
                            keyword,keywordOccurenenceMap)
        #bulk update
        #newWikiArticle = WikiArticles(title=title,text=text)
        #newWikiArticles.append(newWikiArticle)
        try:
            WikiArticles.objects.create(title=title,
                                text=text)
        except Exception as e:
            l.warning ("Failed to save " + title)
            l.warning (str(e))
            #continue silently
        

    #WikiArticles.objects.bulk_create(newWikiArticles,batch_size=DJANGO_BULK_CREATE_BATCH_SIZE)
    return keywordOccurenenceMap




#Return a dict of top matches and their counts
def getTopMatchesForStr(keyword, numMatches = 90):
    if keyword is None or keyword == "":
        raise Exception ("Empty string passed")

    unsortedResult = {}
    sortedResult = {}
    #This throws an exception that we cannot work with, so just send it to upstream/caller
    wikipedia = MediaWiki()

    page = None
    try:
        page = wikipedia.page(keyword)
    except Exception as e:
        #TODO: Log-error 
        return unsortedResult

    #TODO: What if we raise an error here? For now it is on to just return empty
    if page is None or page == "":
        raise Exception ("Wikipedia page not found/or is empty")

    links = page.links
    sections = page.sections
    html = page.html
    for link in links:
        l.warning ("analyzing " + link)


    if len(links) == 0:
        return unsortedResult
     
    how = "dumbMethod"
    how = "correlationMethod"

    if how == "dumbMethod":
        unsortedResult = getTopMatchesUsingDumbMethod(links, numMatches)

    if how == "correlationMethod":
       unsortedResult = getTopMatchesUsingCorrelation(keyword, links, numMatches)

    sortedResult = reversed(sorted(unsortedResult.items(), key=operator.itemgetter(1)))
    return sortedResult

allArticleStats=dict()

l = logging.getLogger('django.db.backends')    
l.setLevel(logging.DEBUG)                      
l.addHandler(logging.StreamHandler())      


class Command(BaseCommand):

    def handle(self, **options):
        # now do the things that you want with your models here
        l.warning ("1 somebody is the best")
        #Initialize from database
        l.warning (options) 
        doStuff()
        wordlist = ["Television", "Sky", "Flower"] #"India", "Smartphone", "Refrigerator"]
        for word in wordlist:
            l.warning ("Getting matches for string " + word)
            topMatches = getTopMatchesForStr(word, 10)
            for tup in topMatches:
                article = tup[0]
                val = tup[1]
                l.warning (word + " : " + article + " :  " + str(val))
            l.warning ("===================\n")



        l.warning ("2 somebody is the best")
        sys.exit(0)


