from __future__ import unicode_literals

from django.db import models

# Models for storing a wikipedia article's stats
class ArticlePerDayStats(models.Model):
    article = models.TextField(primary_key=True,blank=False, null=False)
    perDayViews = models.TextField(blank=False, null=False)

    def __str(self):
        return self.article+"---"+self.perDayViews


#Model for aggregate stats. Will be eventually used for querying etc
class ArticleStats(models.Model):
    article = models.TextField(primary_key=True,blank=False, null=False)
    viewCount = models.IntegerField(default=0)
    def __str__(self):
        return self.article+"---"+str(self.viewCount )



#A db model for storing a string and text of its wikipedia source code
class WikiArticles(models.Model):
    title = models.TextField(primary_key=True,blank=False)
    text = models.TextField(blank=False,null=False)

    
    def __repr__(self):
        ''' repr '''
        return self.__str__()
    

    def __unicode__(self):
        ''' python 2.7 unicode '''
        return '''<'{0}' : '{1}'>'''.format(self.title, self.text)


    def __str__(self):
        ''' python > 3 unicode python 2.7 byte str '''
        return str_or_unicode(self.__unicode__())

    def __eq__(self, other):
        ''' base eq function '''
        try:
            return (
                self.title == other.title and
                self.text == other.text 
            )
        except AttributeError:
            return False



