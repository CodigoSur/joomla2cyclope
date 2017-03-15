from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
import mysql.connector
import re
from cyclope.apps.articles.models import Article
from cyclope.core.collections.models import Collection, Category, Categorization
from django.contrib.contenttypes.models import ContentType
from django.db import IntegrityError
import operator
from autoslug.settings import slugify
from datetime import datetime
from django.contrib.auth.models import User
from lxml import html
from lxml.cssselect import CSSSelector # REQUIRES cssselect

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
        make_option('--default_password',
            action='store',
            dest='joomla_password',
            default=None,
            help='Default password for ALL users. Optional, otherwise usernames will be used.'
        ),
    )
    
    # class constants
    table_prefix = None
    joomla_password = None
    
    def handle(self, *args, **options):
        """Joomla to Cyclope database migration logic"""
        
        self.table_prefix = options['prefix']
        self.joomla_password = options['joomla_password']
        
        # MySQL connection
        cnx = self._mysql_connection(options['server'], options['db'], options['user'], options['password'])
        print "connected to Joomla's MySQL database..."
        
        user_count = self._fetch_users(cnx)
        print "-> {} Usuarios migrados".format(user_count)
        
        collections_count = self._fetch_collections(cnx)
        print "-> {} Colecciones creadas".format(collections_count)

        categories_count = self._fetch_categories(cnx)
        print "-> {} Categorias migradas".format(categories_count)
        
        articles_count = self._fetch_content(cnx)
        print "-> {} Articulos migrados".format(articles_count)
        
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
    
    def _fetch_users(self, mysql_cnx):
        """Joomla Users to Cyclope
           Are users treated as authors in Joomla?"""
        fields = ('id', 'username', 'name', 'email', 'registerDate', 'lastvisitDate') # userType
        query = "SELECT {} FROM {}users".format(fields, self.table_prefix)
        query = self._clean_tuple(query)
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        for user_cursor in cursor:
            user_hash = self._tuples_to_dict(fields, user_cursor)
            user = self._user_to_user(user_hash)
            user.save()
        return User.objects.count()
    
    def _fetch_content(self, mysql_cnx):
        """Queries Joomla's _content table to populate Articles."""
        # TODO images
        fields = ('title', 'alias', 'introtext', 'fulltext', 'created', 'modified', 'state', 'catid', 'created_by')
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
            self._categorize_object(article, content_hash['catid'], 'article')
        cursor.close()
        return Article.objects.count()

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
        return Collection.objects.count()

    def _fetch_categories(self, mysql_cnx):
        """Queries Joomla's categories table to populate Categories."""
        fields = ('id', 'path', 'title', 'alias', 'description', 'published', 'parent_id', 'lft', 'rgt', 'level', 'extension')
        query = "SELECT {} FROM {}categories".format(fields, self.table_prefix)
        query = self._clean_tuple(query)
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        # save categorties in bulk so it doesn't call custom Category save, which doesn't allow custom ids
        categories = []
        for content in cursor:
            category_hash = self._tuples_to_dict(fields, content)
            counter = 1
            category = self._category_to_category(category_hash, counter)
            if category:
                categories.append(category)
                counter += 1
        cursor.close()
        # find duplicate names, since AutoSlugField doesn't properly preserve uniqueness in bulk.
        try: # duplicate query is expensive, we try not to perform it if we can
            Category.objects.bulk_create(categories)
        except IntegrityError:
            cursor = mysql_cnx.cursor()
            query = "SELECT id FROM {}categories WHERE title IN (SELECT title FROM {}categories GROUP BY title HAVING COUNT(title) > 1)".format(self.table_prefix, self.table_prefix)
            cursor.execute(query)
            result = [x[0] for x in cursor.fetchall()]
            cursor.close()
            duplicates = [cat for cat in categories if cat.id in result]
            for dup in duplicates: categories.remove(dup)
            # sort duplicate categories by name ignoring case
            duplicates.sort(key = lambda cat: operator.attrgetter('name')(cat).lower(), reverse=False)
            # categories can have the same name if they're different collections, but not the same slug
            duplicates = self._dup_categories_slugs(duplicates)
            # categories with the same collection cannot have the same name
            duplicates = self._dup_categories_collections(duplicates)
            categories += duplicates
            Category.objects.bulk_create(categories)
        # set MPTT fields using django-mptt's own method TODO
        #Category.tree.rebuild()
        return Category.objects.count()
    # HELPERS
    
    def _clean_tuple(self, query):
        """clean tuple and quotes syntax"""
        return re.sub("[\(\)']", '', query)
    
    def _tuples_to_dict(self, fields, results):
        return dict(zip(fields, results))

    def _extension_to_collection(self, extension):
        """Single mapping from Joomla extension to Cyclope collection."""
        if extension == 'com_content':
            return (1, 'Contenidos', ['article',])
        else: # We might want to create other collections for newsfeeds, etc.
            return (None, None, None)

    def _dup_categories_slugs(self, categories):
        #use a counter to differentiate them
        counter = 2
        for idx, category in enumerate(categories):
            if idx == 0 :
                category.slug = slugify(category.name)
            else:
                if categories[idx-1].name.lower() == category.name.lower() :
                    category.slug = slugify(category.name) + '-' + str(counter)
                    counter += 1
                else:
                    counter = 2
                    category.slug = slugify(category.name)
        return categories

    def _dup_categories_collections(self, categories):
        counter = 1
        for idx, category in enumerate(categories):
            if idx != 0 :
                if categories[idx-1].name.lower() == category.name.lower() :
                    if categories[idx-1].collection == category.collection :
                        category.name = category.name + " (" + str(counter) + ")"
                else : counter = 1
        return categories

    # MODELS CONVERSION

    def _content_to_article(self, content):
        """Instances an Article object from a Content hash."""
        
        # Joomla's Read More feature
        article_content = content['introtext']
        if content['fulltext']:
            article_content += content['fulltext']
            
        # especifico redecom.com.ar
        summary, text = self._redeco_text_logic(article_content)
        
        return Article(
            name = content['title'],
            slug = content['alias'], # TODO or AutoSlug?
            creation_date = content['created'] if content['created'] else datetime.now(),
            modification_date = content['modified'],
            date = content['created'],
            published = content['state']==1, # 0=unpublished, 1=published, -1=archived, -2=marked for deletion
            summary = summary,
            text = text,
            # TODO pretitle
            user_id = content['created_by']
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
                tree_id = counter, # TODO
            )
    
    def _categorize_object(self, objeto, cat_id, model):
        Categorization.objects.create(
            category_id = cat_id,
            content_type_id = ContentType.objects.get(model=model).pk,
            object_id = objeto.pk
        )
        
    def _user_to_user(self, user_hash):
        user = User(
            id = user_hash['id'],
            username = user_hash['username'],
            first_name = user_hash['name'],
            email = user_hash['email'],
            is_staff=True,
            is_active=True,
            is_superuser=True, # else doesn't have any permissions
            last_login = user_hash['lastvisitDate'] if user_hash['lastvisitDate'] else datetime.now(),
            date_joined = user_hash['registerDate'],
        )
        password = self.joomla_password if self.joomla_password else user.username
        user.set_password(password)
        return user

    def _redeco_text_logic(self, content):
        """Logica de redecom.com.ar en html traducida a columnas Cyclope.
           61% de articulos se divien en bajada y cuerpo nota,
           se trata ya sea de parrafos o spans con estas clases."""
        summary, text = None, None
        tree = html.fromstring(content)
        bajada = tree.xpath("*[@class='bajada']")
        if re.search('class="bajada"', content):
            sel = CSSSelector('.bajada')
            bajada = sel(tree)[0]
            summary = bajada.text
            try:
                tree.remove(bajada)
            except ValueError:
                pass # it does work
            text = tree.text_content()
        if summary:
            return summary, text
        else:
            return "", content    

