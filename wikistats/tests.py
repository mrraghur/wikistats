from django.test import TestCase

# Create your tests here.

from wikistats.models import ArticleStats
import wikistats.util as wikistatsutil

#To run, execute
#python manage.py test wikistats.tests


class BookMethodTests(TestCase):

    def test_wiki_keyword(self):
        """
        Check that we actually get links for keywords from wikipedia function.
        """
        links = wikistatsutil.getTopRelatedArticles("India")
        #non empty list would be considered false
        self.assertTrue(links, "links must not be empty")


if __name__ == '__main__':
    unittest.main()


