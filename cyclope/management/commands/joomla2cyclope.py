from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
import mysql.connector
import re
from cyclope.apps.articles.models import Article

class Command(BaseCommand):
    help = """
    Migrates a site in Joomla to CyclopeCMS.

    Usage: (cyclope_workenv)$~ python manage.py joomla2cyclope --server localhost --database REDECO_JOOMLA --user root --password NEW_PASSWORD --prefix wiphala_

    Required params are server host name, database name and database user and password.
    Optional params are joomla's table prefix.
    """

    #NOTE django > 1.8 uses argparse instead of optparse module, 
    #so "You are encouraged to exclusively use **options for new commands."
    #https://docs.djangoproject.com/en/1.9/howto/custom-management-commands/
    option_list = BaseCommand.option_list + (
        make_option('--server',
            action='store',
            dest='server',
            default=None,
            help='Joomla host name'
        ),
        make_option('--database',
            action='store',
            dest='db',
            default=None,
            help='Database name'
        ),
        make_option('--user',
            action='store',
            dest='user',
            default=None,
            help='Database user'
        ),
        make_option('--password',
            action='store',
            dest='password',
            default=None,
            help='Database password'
        ),
        make_option('--prefix',
            action='store',
            dest='prefix',
            default='',
            help='Joomla\'s tables prefix'
        ),
    )
    
    # class constants
    table_prefix = None
    
    def handle(self, *args, **options):
        """Joomla to Cyclope database migration logic"""
        
        self.table_prefix = options['prefix']
        
        # MySQL connection
        cnx = self._mysql_connection(options['server'], options['db'], options['user'], options['password'])
        print "connected to Joomla's MySQL database..."
        
        self._fetch_content(cnx)
        
        #close mysql connection
        cnx.close()
        
    def _mysql_connection(self, host, database, user, password):
        """Establish a MySQL connection to the given option params and return it"""
        config = {
            'host': host,
            'database': database,
            'user': user
        }
        if password:
            config['password']=password
        try:
            cnx = mysql.connector.connect(**config)
            return cnx
        except mysql.connector.Error as err:
            print err
            raise
        else:
            return cnx
    
    def _fetch_content(self, mysql_cnx):
        """Queries Joomla's _content table to populate Articles."""
        fields = ('title', 'alias', 'introtext', 'created', 'modified', 'state')
        # TODO fulltext, cat_id, images, created_by
        query = re.sub("[()']", '', "SELECT {} FROM ".format(fields))+self.table_prefix+"content"
        cursor = mysql_cnx.cursor()
        cursor.execute(query)

        for content in cursor:
            content_hash = self._tuples_to_dict(fields, content)
            article = self._content_to_article(content_hash)
            article.save()
        
        cursor.close()
        
    def _tuples_to_dict(self, fields, results):
        return dict(zip(fields, results))

    def _content_to_article(self, content):
        """Instances an Article object from a Content hash."""
        return Article(
            name = content['title'],
            slug = content['alias'], # or AutoSlug?

            creation_date = content['created'],
            modification_date = content['modified'],
            date = content['created'], # redundant?

            # 0=unpublished, 1=published, -1=archived, -2=marked for deletion
            published = content['state']==1,
            
            # TODO introtext-fulltext logic
            summary = content['introtext'],
            text = content['introtext'],
            pretitle = content['introtext']
                        
            #TODO import Users before
            #user_id = content['created_by']
        )
