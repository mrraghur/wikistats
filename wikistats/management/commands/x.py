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


import datetime
import os


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


count=0
allArticlesFromDB=ArticleStats.objects.all()
for item in allArticlesFromDB.iterator():
    count=count+1
    if (count < 10):
        continue
    key = item.article
    val = item.viewCount
    try:
        print ("key=" + key + "~val=" + str(val))
    except Exception as e:
        print ("problem ")
        eprint(e)
    pass


