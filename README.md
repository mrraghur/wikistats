

This a django module. Its main purpose is to parse wikipedia hourly page views dump into SqLite.
https://dumps.wikimedia.org/other/pagecounts-ez/merged/


There are a couple of methods/classes to query the database. 
All work is in pre-alpha (i.e. version 0.1). Not enough time. Plus its just a hobby project. 

 
To use, create a django project, copy wikistats directory to be a sibling of manage.py and then include it in settings file as an app.
Run 
python manage.py getTopMatches 
to see some matches.  (Takes VERY VERY long time)



