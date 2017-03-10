from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
import mysql.connector
import re
from cyclope.apps.articles.models import Article
from cyclope.core.collections.models import Collection, Category
from django.contrib.contenttypes.models import ContentType

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
        
        self._fetch_collections(cnx)

        self._fetch_categories(cnx)
        
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
    
    # QUERIES
    
    def _fetch_content(self, mysql_cnx):
        """Queries Joomla's _content table to populate Articles."""
        # TODO cat_id, images, created_by
        fields = ('title', 'alias', 'introtext', 'fulltext', 'created', 'modified', 'state')
        # we need to quote field names because fulltext is a reserved mysql keyword
        quoted_fields = ["`{}`".format(field) for field in fields]
        query = "SELECT {} FROM {}content".format(quoted_fields, self.table_prefix)
        query = re.sub("[\[\]']", '', query) # clean list and quotes syntax
        cursor = mysql_cnx.cursor()
        cursor.execute(query)

        for content in cursor:
            content_hash = self._tuples_to_dict(fields, content)
            article = self._content_to_article(content_hash)
            article.save()
        
        cursor.close()

    def _fetch_collections(self, mysql_cnx):
        """Creates Collections infering them from Categories extensions."""
        query = "SELECT DISTINCT(extension) FROM {}categories".format(self.table_prefix)
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        for extension in cursor:
            collection = self._category_extension_to_collection(extension[0])
            if collection:
                collection.save()
        cursor.close()

    def _fetch_categories(self, mysql_cnx):
        """Queries Joomla's categories table to populate Categories."""
        fields = ('id', 'path', 'title', 'alias', 'description', 'published', 'parent_id', 'lft', 'rgt', 'level', 'extension')
        query = "SELECT {} FROM {}categories".format(fields, self.table_prefix)
        query = re.sub("[\(\)']", '', query) # clean tuple and quotes syntax
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        categories = []
        for content in cursor:
            category_hash = self._tuples_to_dict(fields, content)
            counter = 1
            category = self._category_to_category(category_hash, counter)
            if category:
                categories.append(category)
                counter += 1
        # save categorties in bulk so it doesn't call custom Category save, which doesn't allow custom ids
        Category.objects.bulk_create(categories)
        # set MPTT fields using django-mptt's own method
        #Category.tree.rebuild()
    
    # HELPERS
    
    def _tuples_to_dict(self, fields, results):
        return dict(zip(fields, results))

    def _extension_to_collection(self, extension):
        """Single mapping from Joomla extension to Cyclope collection."""
        if extension == 'com_content':
            return (1, 'Contenidos', ['article',])
        else: # We might want to create other collections for newsfeeds, etc.
            return (None, None, None)

    # MODELS CONVERSION

    def _content_to_article(self, content):
        """Instances an Article object from a Content hash."""
        
        # Joomla's Read More feature
        article_content = content['introtext']
        if content['fulltext']:
            article_content += content['fulltext']
        
        return Article(
            name = content['title'],
            slug = content['alias'], # TODO or AutoSlug?
            creation_date = content['created'],
            modification_date = content['modified'],
            date = content['created'],
            published = content['state']==1, # 0=unpublished, 1=published, -1=archived, -2=marked for deletion
            text = article_content,
            #TODO redeco logic
            #summary = content['introtext'],
            #pretitle = content['introtext']
            #TODO import Users before
            #user_id = content['created_by']
        )

    def _category_extension_to_collection(self, extension):
        """Instances a Collection from a Category extension."""
        id, name, types = self._extension_to_collection(extension)
        if id != None:
            collection = Collection.objects.create(id=id, name=name)
            collection.content_types = [ContentType.objects.get(model=content_type) for content_type in types]
            return collection

    def _category_to_category(self, category_hash, counter):
        """Instances a Category in Cyclope from Joomla's Categories table fields."""
        collection_id, name, types = self._extension_to_collection(category_hash['extension'])
        if collection_id: # bring categories for content only
            return Category(
                id = category_hash['id'], # keep ids for foreign keys
                collection_id = collection_id,
                name = category_hash['title'],
                slug = category_hash['path'], # TODO or alias?
                active = category_hash['published']==1,
                parent_id = category_hash['parent_id'] if category_hash['parent_id'] != 0 else None,
                # Cyclope and Joomla use the same tree algorithm
                lft = category_hash['lft'],
                rght = category_hash['rgt'],
                level = category_hash['level'],
                tree_id = counter, # TODO is this right?
            )
