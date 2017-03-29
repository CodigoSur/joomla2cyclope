from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
import pymysql
import re
from cyclope.models import SiteSettings, RelatedContent, Menu, MenuItem
from cyclope.apps.articles.models import Article
from cyclope.core.collections.models import Collection, Category, Categorization
from django.contrib.contenttypes.models import ContentType
from cyclope.apps.medialibrary.models import Picture
from django.contrib.contenttypes.models import ContentType
from django.db import IntegrityError
import operator
from autoslug.settings import slugify
from datetime import datetime
from django.contrib.auth.models import User
from lxml import html, etree
from lxml.cssselect import CSSSelector
import json
from django.db import transaction
from io import BytesIO
import time


class Command(BaseCommand):
    help = """
    Migrates a site in Joomla to CyclopeCMS.

    Usage: (cyclope_workenv)$~ python manage.py joomla2cyclope --server localhost --database REDECO_JOOMLA --user root --password NEW_PASSWORD --prefix wiphala_

    Required params are server host name, database name and database user and password.
    Optional params are joomla's table prefix.
    
    FIXME(doc) REQUIRES cssselect, PyMySQL
    
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
        make_option('--devel',
            action='store_true',
            dest='devel',
            help='Use http://localhost:8000 as site url (development)'
        ),
    )
    
    # class constants
    table_prefix = None
    joomla_password = None
    devel_url = False
    
    def handle(self, *args, **options):
        """Joomla to Cyclope database migration logic"""
        
        self.table_prefix = options['prefix']
        self.joomla_password = options['joomla_password']
        self.devel_url = options['devel']

        # MySQL connection
        cnx = self._mysql_connection(options['server'], options['db'], options['user'], options['password'])
        print "connected to Joomla's MySQL database..."
        
        start = time.time() # T

        self._site_settings_setter()

        user_count = self._fetch_users(cnx)
        print "-> {} Usuarios migrados".format(user_count)
        self._time_from(start)

        menus_count, menu_types = self._fetch_menus(cnx)
        menuitem_count = self._fetch_menuitems(cnx, menu_types)
        print "-> {} Menus migrados.".format(menus_count)
        print "-> {} Items de Menu migrados.".format(menuitem_count)
        self._time_from(start)
        
        collections_count = self._fetch_collections(cnx)
        print "-> {} Colecciones creadas".format(collections_count)
        self._time_from(start)

        categories_count = self._fetch_categories(cnx)
        print "-> {} Categorias migradas".format(categories_count)
        self._time_from(start)
        
        articles_count, articles_images, articles_categorizations = self._fetch_content(cnx)
        print "-> {} Articulos migrados".format(articles_count)
        self._time_from(start)
        
        categorizations_count = self._categorize_articles(articles_categorizations)
        print "-> {} Articulos categorizados".format(categorizations_count)
        self._time_from(start)
        
        images_count, related_count, article_images_count = self._create_images(articles_images)
        print "-> {} Imagenes migradas".format(images_count)
        print "-> {} Imagenes de articulos".format(article_images_count)
        print "-> {} Imagenes como contenido relacionado".format(related_count)
        self._time_from(start)
        
        #close mysql connection
        cnx.close()
        
    def _mysql_connection(self, host, database, user, password):
        """Establish a MySQL connection to the given option params and return it"""
        password = password if password else ""
        cnx = pymysql.connect(
            host='localhost',
            user=user,
            password=password,
            db=database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
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
        for user_hash in cursor:
            user = self._user_to_user(user_hash)
            user.save()
        cursor.close()
        return User.objects.count()

    def _fetch_content(self, mysql_cnx):
        """Queries Joomla's _content table to populate Articles."""
        articles_images = []
        articles_categorizations = []
        fields = ('title', 'alias', 'introtext', 'fulltext', 'created', 'modified', 'state', 'catid', 'created_by', 'images')
        # we need to quote field names because fulltext is a reserved mysql keyword
        quoted_fields = ["`{}`".format(field) for field in fields]
        query = "SELECT {} FROM {}content".format(quoted_fields, self.table_prefix)
        query = re.sub("[\[\]']", '', query) # clean list and quotes syntax
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        #single transaction for all articles
        transaction.enter_transaction_management()
        transaction.managed(True)
        for content_hash in cursor:
            article = self._content_to_article(content_hash)
            article.save()
            # this is here to have a single query to the largest table
            articles_categorizations.append( self._categorize_object(article, content_hash['catid'], 'article') )
            articles_images.append( self._content_to_images(content_hash, article.pk) )
        cursor.close()
        transaction.commit()
        transaction.leave_transaction_management()
        return Article.objects.count(), articles_images, articles_categorizations

    def _fetch_collections(self, mysql_cnx):
        """Creates Collections infering them from Categories extensions."""
        query = "SELECT DISTINCT(extension) FROM {}categories".format(self.table_prefix)
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        for extension in cursor:
            collection = self._category_extension_to_collection(extension['extension'])
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
        for category_hash in cursor:
            category = self._category_to_category(category_hash)
            if category:
                categories.append(category)
        cursor.close()
        # find duplicate names, since AutoSlugField doesn't properly preserve uniqueness in bulk.
        try: # duplicate query is expensive, we try not to perform it if we can
            Category.objects.bulk_create(categories)
        except IntegrityError:
            cursor = mysql_cnx.cursor()
            query = "SELECT id FROM {}categories WHERE title IN (SELECT title FROM {}categories GROUP BY title HAVING COUNT(title) > 1)".format(self.table_prefix, self.table_prefix)
            cursor.execute(query)
            result = [x['id'] for x in cursor.fetchall()]
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
        # set MPTT fields using django-mptt's own method
        Category.tree.rebuild()
        return Category.objects.count()

    def _create_images(self, images):
        images = [image for image in images if image]
        for image_hash in images:
            image_hash = image_hash[0] # flatten
            picture = self._image_to_picture(image_hash)
            self._image_article_relation(image_hash, picture)
        return Picture.objects.count(), RelatedContent.objects.count(), Article.objects.exclude(pictures=None).count()

    def _categorize_articles(self, categorizations):
        Categorization.objects.bulk_create(categorizations)
        return Categorization.objects.count()

    def _fetch_menus(self, cnx):
        """migrate joomla menu_types to cyclope menus
           they have a similar tree algorithm so hierarchy is preserved."""
        fields = ('id', 'menutype', 'title', 'description')
        query = "SELECT {} FROM {}menu_types".format(fields, self.table_prefix)
        query = self._clean_tuple(query)
        cursor = cnx.cursor()
        cursor.execute(query)
        menu_types = {}
        for menu_type_hash in cursor:
            menu = self._menu_type_to_menu(menu_type_hash)
            menu.save()
            menu_types[menu_type_hash['menutype']] = menu.pk
        cursor.close()
        return Menu.objects.count(), menu_types

    def _fetch_menuitems(self, cnx, menu_types):
        """migrate joomla menus to cyclope menuitems.
           they have a similar tree algorithm so hierarchy is preserved.
           menu_types is a dict mapping the FK to the menu_types menutype field."""
        fields = ('id', 'menutype', 'title', 'alias', 'path', 'link', 'published', 'parent_id', 'level', 'lft', 'rgt', 'home')
        query = "SELECT {} FROM {}menu".format(fields, self.table_prefix)
        query = self._clean_tuple(query)
        cursor = cnx.cursor()
        cursor.execute(query)
        menuitems = []
        for menu_hash in cursor:
            if menu_types.has_key(menu_hash['menutype']):
                menuitem = self._menu_to_menuitem(menu_hash, menu_types)
                menuitems.append(menuitem)
        cursor.close()
        # delete pre existent menuitem 1 because of id collision
        MenuItem.objects.all().delete()
        # skip custom save method
        MenuItem.objects.bulk_create(menuitems)
        # hierarchy ordering
        MenuItem.tree.rebuild()
        return MenuItem.objects.count()

    # HELPERS

    def _clean_tuple(self, query):
        """clean tuple and quotes syntax"""
        return re.sub("[\(\)']", '', query)
    
    def _tuples_to_dict(self, fields, results):
        return dict(zip(fields, results))

    def _time_from(self, start):
        now = time.time()
        ellapsed = now - start 
        print( "%.2f s" % ellapsed )

    # CYCLOPE'S LOGIC

    def _site_settings_setter(self):
        settings = SiteSettings.objects.all()[0]
        site = settings.site
        if not self.devel_url:
            site.domain = "www.redecom.com.ar" 
        else:
            site.domain = "localhost:8000"

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

    # JOOMLA'S LOGIC

    def _joomla_content(self, content):
        """Joomla's Read More feature separates content in two columns: introtext and fulltext,
           Most of the time all of the content sits at introtext, but when Read More is activated,
           it is spwaned through both introtext and fulltext.
           Receives the context hash."""
        article_content = content['introtext']
        if content['fulltext']:
            article_content += content['fulltext']
        return article_content

    def _content_to_images(self, content_hash, article_id):
        """Instances images from content table's images column or HTML img tags in content.
           Images column has the following JSON '{"image_intro":"","float_intro":"","image_intro_alt":"","image_intro_caption":"","image_fulltext":"","float_fulltext":"","image_fulltext_alt":"","image_fulltext_caption":""}'
           """
        imagenes = []
        # instances images from column
        images = content_hash['images']
        images = json.loads(images)
        # NOTE we could also use captions & insert image_fulltext inside text
        if images['image_intro']:
            image_hash = {'src': images['image_intro'], 'alt': images['image_intro_alt'], 'article_id': article_id, 'image_type': 'article' }
            imagenes.append(image_hash)
        if images['image_fulltext']:
            image_hash = {'src': images['image_fulltext'], 'alt': images['image_fulltext_alt'], 'article_id': article_id, 'image_type': 'article' }
            imagenes.append(image_hash)
            
        # instances images from content
        full_content = self._joomla_content(content_hash)
        try: # x-treme hack! html.fromstring having ID collisions, collect_ids is not an option...
            context = etree.iterparse(BytesIO(full_content.encode('utf-8')), huge_tree=True, html=True)
            for action, elem in context: pass # just read it
            tree = context.root
            sel = CSSSelector('img')
            imgs = sel(tree)
            for img in imgs:
                src = img.get('src')
                alt = img.get('alt')
                image_hash = {'src': src, 'alt': alt, 'article_id': article_id, 'image_type': 'related'}
                imagenes.append(image_hash)
        except:
            pass
        return imagenes

    def _image_article_relation(self, image_hash, picture):
        """images comming from content's image column will be article images,
           images comming from within the article's content will be just related contents."""
        article_id = image_hash['article_id']
        if image_hash['image_type'] == 'article':
            picture.pictures.add(article_id)
            picture.save()
        elif image_hash['image_type'] == 'related':
            other_type_id=ContentType.objects.get(name='picture').id
            article=Article.objects.get(pk=article_id)
            RelatedContent.objects.create(self_object=article, other_type_id=other_type_id, other_id = picture.pk)

    def _menu_type_id(self, menu_types, menutype):
        if menu_types.has_key(menutype):
            return menu_types[menutype]

    # MODELS CONVERSION

    def _content_to_article(self, content):
        """Instances an Article object from a Content hash."""
        article = Article(
            name = content['title'],
            creation_date = content['created'] if content['created'] else datetime.now(),
            modification_date = content['modified'],
            date = content['created'],
            published = content['state']==1, # 0=unpublished, 1=published, -1=archived, -2=marked for deletion
            text =  self._joomla_content(content),
            user_id = content['created_by']
        )
        return article

    def _image_to_picture(self, image_hash):
        src = image_hash['src']
        alt = image_hash['alt'] if image_hash['alt'] else ""
        # TODO devel URL 
        name = slugify(src)
        # needs to be created in order to build relations
        picture = Picture.objects.create(
            image = src,
            description = alt,
            name = name,
            # creation_date = post['post_date'], article
        )
        return picture

    def _category_extension_to_collection(self, extension):
        """Instances a Collection from a Category extension."""
        id, name, types = self._extension_to_collection(extension)
        if id != None:
            collection = Collection.objects.create(id=id, name=name)
            collection.content_types = [ContentType.objects.get(model=content_type) for content_type in types]
            return collection

    def _category_to_category(self, category_hash):
        """Instances a Category in Cyclope from Joomla's Categories table fields."""
        collection_id, name, types = self._extension_to_collection(category_hash['extension'])
        if collection_id: # bring categories for content only
            return Category(
                id = category_hash['id'], # keep ids for foreign keys
                collection_id = collection_id,
                name = category_hash['title'],
                active = category_hash['published']==1,
                parent_id = category_hash['parent_id'] if category_hash['parent_id'] != 0 else None,
                # Cyclope and Joomla use the same tree algorithm
                lft = category_hash['lft'],
                rght = category_hash['rgt'],
                level = category_hash['level'],
                tree_id = category_hash['id'] # any value, overwritten by tree rebuild
            )
    
    def _categorize_object(self, objeto, cat_id, model):
        categorization = Categorization(
            category_id = cat_id,
            content_type_id = ContentType.objects.get(model=model).pk,
            object_id = objeto.pk
        )
        return categorization
        
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

    def _menu_type_to_menu(self, menu_type_hash):
        menu = Menu(
            id = menu_type_hash['id'],
            name = menu_type_hash['title'],
            main_menu = False,
        )
        return menu

    def _menu_to_menuitem(self, menu_hash, menu_types):
        menu_id = self._menu_type_id(menu_types, menu_hash['menutype'])
        parent_id = self._menu_hierarchy(menu_hash['parent_id'])
        menuitem = MenuItem(
            id = menu_hash['id'],
            menu_id = menu_id,
            name = menu_hash['title'],
            parent_id = parent_id,
            site_home = menu_hash['home']==1,
            url = menu_hash['path'],
            active = menu_hash['published']==1,
            persistent_layout = False,
            lft = menu_hash['lft'],
            rght = menu_hash['rgt'],
            level = menu_hash['level'],
            tree_id = menu_hash['id'] # any value, overwritten by tree rebuild
        )
        return menuitem

    def _menu_hierarchy(self, parent_id):
        """0 is default value, and 1 is Menu Item Root, a Menu with no menutype"""
        if parent_id != 0 and parent_id != 1:
            return parent_id
        return None
