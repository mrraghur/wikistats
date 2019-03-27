from __future__ import print_function
from django.core.management.base import BaseCommand


import datetime
import os
import glob
import sys
import subprocess
import gzip
import shutil
import sys
import pdb
import re
import atexit
import signal
import sys

from wikistats.models import ArticlePerDayStats, ArticleStats

baseUrl="https://dumps.wikimedia.org/other/pageviews/2017/"
fileDownloadsPath="/Users/somebody/wsfun/visio.ai/wikistats"
MAX_NUM_FILES=240
VIEWCOUNT_ENUM=0
EDITCOUNT_ENUM=1
MAX_NUM_LINES=None
lockFile="/tmp/lock.aggregateviewcounts.lock"
finalViewAndEditCounts={}
filecount=0
downloadDir="wikistats/workingdir"
finalCounts={}
fileForSavingStats="articleStats.txt"
fileForSavingStatsBkp="articleStats.txt.bkp"
fileForSavingStatsBkp2="articleStats.txt.a"
numTimesSaveMethodCalled=0
DJANGO_BULK_CREATE_BATCH_SIZE=500

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


#When you see ctrl-c, save all data to DB
def signal_handler(signal, frame):
    eprint('You pressed Ctrl+C! Saving all data to db')
    eprint("To kill program, do ctrl-z and then kill -9 <pid>")
    deleteViewAndEditCountsFromDB()
    saveFinalViewCountsToDBOptimize(finalViewAndEditCounts)
    count = 10
    allArticlesFromDB=ArticleStats.objects.all()
    print("Curr data: ")
    for item in allArticlesFromDB.iterator():
        key = item.article
        val = item.viewCount
        try:
            print(key+":"+str(val))
            count = count - 1
        except:
            pass
        if count == 0:
            break 
 


signal.signal(signal.SIGINT, signal_handler)

def readFileAndCount(pathUnCompressed):
    mydict = {}
    visit = 0
    try:
        linecount = 0
        linecountEn = 0
        with open(pathUnCompressed) as f:
            for line in f:
                linecount = linecount + 1
                line = line.strip()
                myarray = line.split()
                if len(myarray) != 4:
                    eprint ("Skipping " + pathUnCompressed + ":" + str(linecount) + ":: " + line) 
                    continue
                lang = myarray[0]
                article = myarray[1]
                viewcount = int(myarray[2])
                editcount = int(myarray[3])
                tup = (viewcount, editcount) 
                #Skip non english stuff
                if lang == "en" :
                    linecountEn = linecountEn + 1
                    mydict[article] = tup
                    visit = 1
                if MAX_NUM_LINES is not None and linecountEn == MAX_NUM_LINES:
                    break
    except IOError:
        msg = "Error while reading uncompressed file %s . Error is : " % pathUnCompressed, e
        msg = msg + ". Removing uncompressed file and proceeding to next one"
        eprint(msg)

    return mydict

    
def mergeViewAndEditCounts(prevStats, currViewAndEditCounts):
    for article, tup in currViewAndEditCounts.iteritems():
        if article in prevStats:
            prevTup  = prevStats[article]
            newTup = (prevTup[VIEWCOUNT_ENUM] + tup[VIEWCOUNT_ENUM] , 
                      prevTup[EDITCOUNT_ENUM] + tup[EDITCOUNT_ENUM])
            prevStats[article] = newTup
        else:
            prevStats[article] = tup

    return prevStats


def printFinalViewCounts(viewAndEditCounts):
    #print viewAndEditCounts 
    for article, tup in viewAndEditCounts.iteritems():
            print (article+"       "+tup[VIEWCOUNT_ENUM])
 
def printFinalEditCounts(viewAndEditCounts):
    #print viewAndEditCounts 
    for article, tup in viewAndEditCounts.iteritems():
            print (article+"       "+tup[EDITCOUNT_ENUM])
 
def saveFinalViewCountsToDB(viewAndEditCounts):
    eprint ("Entering saveFinalViewCountsToDB method")
    for article_, tup in viewAndEditCounts.iteritems():
        try:
            obj = ArticleStats.objects.get(article=article_)
            if obj.viewCount == val_[0]:
                pass
            else:#if value has changed, update it
                obj.viewCount = tup[0]
                obj.save()
        except ArticleStats.DoesNotExist:
            obj = ArticleStats(article=article_, viewCount=tup[0]) 
            obj.save()
    eprint ("Exiting saveFinalViewCountsToDB method")

#1) Save to a file
#2) Save to a db.
def saveFinalViewCountsToDBOptimize(viewAndEditCounts):

    global numTimesSaveMethodCalled
    try:
        #Backup existing file
        if numTimesSaveMethodCalled == 0:
            if os.path.isfile(fileForSavingStats):
                os.rename(fileForSavingStats,fileForSavingStatsBkp)
                numTimesSaveMethodCalled = 1
        else:
            if os.path.isfile(fileForSavingStats):
                os.rename(fileForSavingStats,fileForSavingStatsBkp2)

        f = open(fileForSavingStats, 'w')
        for article_, tup in viewAndEditCounts.iteritems():
            print(article_+":"+str(tup[VIEWCOUNT_ENUM]),file=f)
        f.close()
    except Exception as e:
        eprint ("Could not open fileForSavingStats(" + fileForSavingStats + ")  for updating. Please investigate.");
        eprint(e)
        eprint ("Optimistically Continuing to save to DB")

    articleStatsList = []
    for article_, tup in viewAndEditCounts.iteritems():
        newObj = ArticleStats(article=article_,
                             viewCount=tup[0]);
        
        articleStatsList.append(newObj)
        
    try:
        ArticleStats.objects.bulk_create(articleStatsList,batch_size=DJANGO_BULK_CREATE_BATCH_SIZE)
    except Exception as e:
        eprint("Bulk save failed. Please investiagte")
        eprint(e)
        

    eprint ("Exiting saveFinalViewCountsToDBOptimize method")




def deleteViewAndEditCountsFromDB():
    ArticleStats.objects.all().delete()
    pass

def doStuff():
    global filecount
    global finalViewAndEditCounts
    for filename in glob.glob("*.gz"):
        pathZipped = filename
        baseNameAndExt = os.path.splitext(pathZipped);
        pathUnCompressed = baseNameAndExt[0]

        try:
            with gzip.open(pathZipped, 'rb') as f_in, open(pathUnCompressed, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
            viewandEditCounts = readFileAndCount(pathUnCompressed)
            os.remove(pathUnCompressed)
            finalViewAndEditCounts  = mergeViewAndEditCounts(finalViewAndEditCounts, viewandEditCounts)
            filecount = filecount + 1
            if MAX_NUM_FILES is not None and filecount == MAX_NUM_FILES:#If reached max number of files we want to process, then stop processing
                break
        except IOError, e:
            try:
                msg = "Error while unzipping %s . Error is %s : " % pathZipped, e
                msg = msg + ". Removing uncompressed file and proceeding to next one"
                eprint(msg)
                os.remove(pathUnCompressed)
            except:
                eprint("Error in unzipping " + filename)

        eprint("Finished processing file " + filename)
        if filecount % 10 == 9:
            eprint("Processed " + str(filecount) + " so far")  
            deleteViewAndEditCountsFromDB()
            saveFinalViewCountsToDBOptimize(finalViewAndEditCounts)

    #printFinalViewCounts(finalViewAndEditCounts)
    deleteViewAndEditCountsFromDB()
    saveFinalViewCountsToDBOptimize(finalViewAndEditCounts)
    eprint("Finished collecting data from " + str(filecount) + " number of files")


def removeLockFile(lockFile):
    #Remove the lock file
    try:
        print("Removing lockFile " + lockFile)
        os.remove(lockFile)
    except e:
        eprint ("Error while removing lock file " + lockFile + ". Please investigate. " + e)



class Command(BaseCommand):

    def handle(self, **options):
        # now do the things that you want with your models here
        os.chdir(downloadDir)
        if (os.path.isfile(lockFile)):
            eprint ("Found lockfile. " + lockFile + ". Please wait for job to terminate or remove it before retrying")
            return
        #Create lock file
        try:
            lockFP = open(lockFile, 'w')
            lockFP.close()
            atexit.register(removeLockFile, lockFile)
        except Exception as e:
            eprint ("Exiting because encontered error while creating lock file " + lockFile + ". Please investigate. ")
            eprint (e)
            return

        #Initialize from database
        doStuff()

        sys.exit(0)


