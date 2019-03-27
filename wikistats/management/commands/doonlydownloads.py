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
import atexit

from wikistats.models import ArticlePerDayStats, ArticleStats

baseUrl="https://dumps.wikimedia.org/other/pageviews/2017/"
fileDownloadsPath="/Users/somebody/wsfun/visio.ai/wikistats"

#Stored all-already processed files in this file, so that we don't have to do them again

LOOK_BACK_PERIOD = 60 #365
MAX_NUM_FILES=1160
NUM_HOURS_PER_DAY = 24
TILDA="~"
COMMA=","
COLON=":"
TESTING_NUM_LINES_TO_PARSE=100
allArticles=dict()


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

#Given a look back period, give all strings which should be downloaded from wikipedia
def getTuplesForGivenLookBackPeriod(endDATE, lookbackperiod): 
    result = []
    for i in range(0, lookbackperiod):
        start_date = endDATE + datetime.timedelta(-i)
        wikiFileFormatDatePart = start_date.strftime("%Y/%Y-%m/pageviews-%Y%m%d")
        #https://dumps.wikimedia.org/other/pageviews/2017/2017-07/pageviews-20170701-050000.gz
        #print wikiFileFormatDatePart
        for j in range (0, NUM_HOURS_PER_DAY):
            hourStr = str(j).zfill(2)
            localFileZippedFormat = start_date.strftime("pageviews-%Y%m%d")+"-"+hourStr+"0000.gz"
            localFileUncompressedFormat = start_date.strftime("pageviews-%Y%m%d")+"-"+hourStr+"0000"
            year = start_date.strftime("%Y")
            month = start_date.strftime("%Y-%m")
            linkStr = "%s/%s/%s/%s" % ("https://dumps.wikimedia.org/other/pageviews"
                      , year 
                      , month 
                      , localFileZippedFormat)
            #print linkStr
            #print localFileUncompressedFormat
            tup = (localFileUncompressedFormat, localFileZippedFormat, linkStr)
            result.append(tup);
    
    return result

#Dump the dictionary conatining the final per article, per day stats info
def dumpArticlePerDayStats(articleInfoDict):
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

#Format of dictionary:
#   key : Article title(only english)
#   value : <startdate>~Comma separated Values for day_1:Comma separated values for day_2:...:Comma separated values for day_n
#         : Note the delimiters : tilda, colon and comma
# example allArticles[India]=20170703~1,3,4,,,,,,,,,,,,,6,0,8:0,2,45,8,90,...
# Explanation: For wiki article on india, on July 3rd, 2017, there were 1,3,4 views in first three hours of the day.
#                                         on july 2nd, 2017, there were 0,2,45 views in first three hours of the day
#

#Given an uncompressed file extract how many times an articles was read/viewed
#Return dictionary of all articles and their view/edit counts
#File must be unzipped file
def readFileAndReturnViewCounts(filename):
    linenum = 0
    numEnLines = 0
    result = dict()
    with open(filename) as f:
        for line in f:
            linenum = linenum + 1
            #Discard non english stuff
            if line.find("en") != 0:
                continue
            line = line.strip()
            myarray = line.split(" ")
            if len(myarray) != 4:
                msg = ("Wrong syntax for linenum %d:: Line is %s... Expected 4 but found %d")  % (linenum, line, len(myarray))
                eprint (msg)
                continue #Ignore if line has a syntax error

            article = myarray[1]
            numViews = myarray[2]
            numEdits = myarray[3]
        
            #Small optimization because many long tail pages will have zero views 
            #in short period of one hour
            if (numViews == 0) and (numEdits == 0):
                continue
            numEnLines = numEnLines + 1
            #if (numEnLines > TESTING_NUM_LINES_TO_PARSE):
            #    continue;
            tup = (numViews, numEdits)
            result[article] = tup

    return result


def insertViewCountIntoVal(val, viewCount, dayIndex, hourIndex):

    if (hourIndex < 0 or hourIndex >= 24):
        eprint("hourIndex is wrong (" + hourIndex + ") for val=" + val)
        return ""

    #Seperate out the date part and just get the day/hour view counts
    myarray1 = val.split(TILDA)
    #Get daily view counts as a comma separated string
    myarray2 = myarray1[1].split(COLON)
    stringForaGivenDay = myarray2[dayIndex]
    #hourly view counts
    myarray3 = stringForaGivenDay.split(COMMA)
    currVal = myarray3[hourIndex]
    #if (currVal != ""):
    #    eprint ("currVal should have be been empty. But is " + currVal)
    #    return ""
    #Modify the value
    myarray3[hourIndex] = viewCount;

    
    #Assemble back the string
    
    result = ""
    for i in range (0 , len(myarray2)):
        if (i == dayIndex):
            tmp = ""
            for j in range (0 , len(myarray3)):
                tmp = tmp + myarray3[j]
                if (j != len(myarray3)-1):
                    tmp = tmp + COMMA

            result = result + tmp
        else:
            result = result + myarray2[i]   
            
        if (i != len(myarray2)-1): #If not the last one
            result = result + COLON

    
    #Prefix with day info
    result = myarray1[0] + TILDA + result;


    return result

def readFilesAndAggregateCounts(filenames, startDate, lookbackperiod):
    global allArticles
    if type(filenames) is not list:
        return 

    valuePrefix = startDate.strftime("%Y%m%d")
    for filename in filenames:
        #pageviews-20170729-230000
        if filename.find("gz") != -1:
            eprint("filename " + filename + " does not seem like a text file. Is it a compressed file?")
            continue 
        myarray1 = filename.split("-")
        if len(myarray1) != 3:
            eprint("filename " + filename + " must be of the format pageviews-%Y%m%d-230000")
            continue
        myarray2 = int(myarray1[2])/10000
        hourOfDay = int(myarray2);
        #currDateTimeObj = datetime.datetime.strptime(filename, 'pageviews-%Y%m%d-230000')#
        currDateTimeObj = datetime.datetime.strptime(myarray1[1], '%Y%m%d')#
        currDate = currDateTimeObj.date()
        daysInBetween = (startDate - currDate).days
        if (daysInBetween >= lookbackperiod):
            eprint ("Excluding file " + filename + " from further analysis as it is before lookback period")
            continue
        try: 
            articleViewCounts = readFileAndReturnViewCounts(filename)
            for article, tup in articleViewCounts.iteritems():
                val = ""
                if article in allArticles:
                    val = allArticles[article]
                else:
                    #Create how the string should look like for given lookbackperiod
                    val = ""
                    for i in range(0, lookbackperiod):
                        tmp = ""
                        for j in range(0, NUM_HOURS_PER_DAY-1):#23 commas
                            tmp = tmp + COMMA
                        if i == lookbackperiod-1:
                            val = val + tmp;
                        else:
                            val = val + tmp
                            val = val + COLON
                        
                    #Append datestring in the front
                    val = valuePrefix + TILDA + val
                newVal = insertViewCountIntoVal(val, tup[0], daysInBetween, hourOfDay)
                allArticles[article] = newVal
        except IOError, e:
            msg = "Could not read from file %s .Error was " % filename,e 
            eprint(msg)
            pass
            
    return allArticles

#Read status file and store all processed files in here.
#Duplicated are OK
#status file of the format pageviews-20170701-230000
def readStatusFile(statusFileArg):
    mydict = {}
    try:
        with open(statusFileArg) as f:
            for line in f:
                key = line.strip()
                mydict[key] = True
    except IOError:
        #Create emptyFile
        f = open(statusFileArg, 'w')
        f.close()

    return mydict

#Merge with statusfile dictionary and 
def updateStatusFile(newlyProcessedFiles, previouslyProcessedFiles, statusFileArg):
    for filename_, val_ in newlyProcessedFiles.iteritems():
        previouslyProcessedFiles[filename_] = val_

    try:
            f = open(statusFileArg, 'w')
            for filename_, val_ in previouslyProcessedFiles.iteritems():
                previouslyProcessedFiles[filename_] = val_
                print(filename_, file=f)
            
    except e:
        eprint ("Could not open statusFile(" + statusFileArg + ")  for updation. Please investigate.");
        eprint(e)
        eprint ("Exiting program")
        return previouslyProcessedFiles
    
    return previouslyProcessedFiles



#Go over each file, analyze and download as required and 
def downloadAllFiles(fileLinkTuples, previouslyProcessedFiles, lookbackPeriod):
    newlyProcessedFiles = {}
    filecount = 0
    print ("Entering downloadAllFiles")
    for tup in fileLinkTuples:
        #Check if file exists in filedownloads area
        pathUnCompressed = tup[0]
        pathZipped = tup[1]
        link = tup[2]
        print ("Analyzing " + pathUnCompressed)
        #use wget to fetch the dumps. Continue from previous download if required
        if pathUnCompressed in previouslyProcessedFiles:
            continue
        elif os.path.isfile(pathUnCompressed) and os.path.isfile(pathZipped): #File has been downloaded and unzipped, just not processed yet
            #Parse it and do the processing
            pass
        elif not os.path.isfile(pathUnCompressed) and os.path.isfile(pathZipped): #File was downloaded but not unzipped
            #subprocess.call(["gzip"],"-d", "-k", "x/pageviews-20170729-230000.gz");
            pass
            continue
        else: #File was not downloaded. download it and then do the processing
            subprocess.call(["wget", link, "-c"])
            pass
            ##Now unzip it
            #try:
            #    #pathZipped = "error.gz"
            #    #pathUnCompressed = "error"
            #    with gzip.open(pathZipped, 'rb') as f_in, open(pathUnCompressed, 'wb') as f_out:
            #        shutil.copyfileobj(f_in, f_out)
            #except IOError, e:
            #    msg = "Error while unzipping %s . Error is : " % pathZipped, e
            #    msg = msg + ". Removing uncompressed file and proceeding to next one"
            #    eprint(msg)
            #    os.remove(pathUnCompressed)
            #    continue

        filecount = filecount + 1
        filename = pathUnCompressed

        newlyProcessedFiles[pathUnCompressed] = True
        if filecount == MAX_NUM_FILES:#If reached max number of files we want to process, then stop processing
            break
    return newlyProcessedFiles

def readAllArticlePerDayStatsFromDB():
    global allArticles
    result = {}

    allArticlesFromDB=ArticlePerDayStats.objects.all()
    for item in allArticlesFromDB.iterator():
        key = item.article
        val = item.perDayViews
        allArticles[key] = val

    return result

def saveAllArticlePerDaySyaysToDB(allArticlesToBeSaved):
    for article_, val_ in allArticlesToBeSaved.iteritems():
        try:
            obj = ArticlePerDayStats.objects.get(article=article_)
            if obj.perDayViews == val_:
                pass
            else:#if value has changed, update it
                obj.perDayViews = val_
                obj.save()
        except ArticlePerDayStats.DoesNotExist:
            obj = ArticlePerDayStats(article=article_, perDayViews=val_) 
            obj.save()



def doStuff():
    
    #startDate=datetime.date.today()
    print ("Current working directory is " +  os.getcwd())
    startDate = datetime.datetime.strptime("20170729", '%Y%m%d').date()
    global allArticles
    #pdb.set_trace()
    allArticles = {}#readAllArticlePerDayStatsFromDB()

    allProcessedFiles = readStatusFile(statusFile)

    allDownloadsFromlastThirtyDays = getTuplesForGivenLookBackPeriod(startDate, LOOK_BACK_PERIOD)

    #for tup in allDownloadsFromlastThirtyDays:
    #    print (tup)
    #sys.exit(0)
    newlyProcessedFiles = downloadAllFiles(allDownloadsFromlastThirtyDays, allProcessedFiles, LOOK_BACK_PERIOD)
    allProcessedFiles = updateStatusFile(newlyProcessedFiles, allProcessedFiles, statusFile)


    pass

def removeLockFile(lockFile):
    #Remove the lock file
    try:
        print("Removing lockFile " + lockFile)
        os.remove(lockFile)
    except e:
        eprint ("Error while removing lock file " + lockFile + ". Please investigate. " + e)


startDate = datetime.datetime.strptime("20170729", '%Y%m%d').date()
lockFile="/tmp/lock.makewikistats.lock"
downloadDir="wikistats/workingdir"
statusFile="statusFile.txt"

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


