# -*- coding: utf-8 -*- 
from __future__ import print_function
from wikistats.models import ArticleStats
import wikipedia

DEFAULT_NUM_ARTICLES=5
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def getTopRelatedArticles(keyword, numArticles=DEFAULT_NUM_ARTICLES, exactMatch=True):
    
    #Query the DB for this keyword
    if not keyword:
       raise ValueError("keyword is empty")

    #By default we assume exact match and look in wikipedia for the article
    try:
        article = wikipedia.page(keyword)
    except wikipedia.exceptions.PageError ,e:
        eprint("Article with keyword " + keyword + " Could not be fetched from wikipedia. Error raised is " + e)
        return ValueError("Article not found")

    links = article.links
    linksAndStatsDAO = ArticleStats.objects.filter(article__in=links)
    for linkAndState in linksAndStatsDAO:
        print (linkAndState)
    


    #print (links)
    return links

    



        
    

    
     



