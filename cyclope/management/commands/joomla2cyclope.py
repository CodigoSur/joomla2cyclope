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
from django.db import IntegrityError, transaction, connection
import operator
from autoslug.settings import slugify
from datetime import datetime
from django.contrib.auth.models import User
from lxml import html, etree
from lxml.cssselect import CSSSelector
import json
from io import BytesIO
import time
from collections import Counter

class Command(BaseCommand):
    help = """
    Migrates a site in Joomla to CyclopeCMS.

    Usage: (cyclope_workenv)$~ python manage.py joomla2cyclope --server localhost --database REDECO_JOOMLA --user root --password NEW_PASSWORD --prefix wiphala_

    Required params are server host name, database name and database user and password.
    Optional params are joomla's table prefix.
    
    This script makes use of libraries not included in Cyclope that need to be installed through pip

    PyMySQL: a Python driver for MySQL database connection
    $ pip install pymysql

    Lxml depends on libxml2 headers
    $ sudo apt-get install libxml2-dev libxslt1-dev
    
    Then:
    $ pip install lxml

    Cssselect: a library for analyzing HTML
    $ pip install cssselect

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
        make_option('--limit',
            action='store',
            dest='limit',
            default=None,
            help='Limit Content query result to this ammount of rows.'
        ),
        make_option('--offset',
            action='store',
            dest='offset',
            default=None,
            help='Make Content query return results starting from this row number.'
        ),
    )
    
    # CLASS CONSTANTS
    table_prefix = None
    joomla_password = None
    devel_url = False
    # menus
    _menu_category_view = 'teaser_list'
    _menu_category_view_options = '{"sort_by": "DATE+", "show_title": false, "show_description": false, "show_image": false, "items_per_page": 10, "limit_to_n_items": 0, "simplified": false, "traverse_children": true, "navigation": "DISABLED"}'
    _category_content_type = None
    _article_content_type = None
    # categories
    _categories_collection = 1
    _tags_collection = 2   
    
    def handle(self, *args, **options):
        """Joomla to Cyclope database migration logic"""
        
        self.table_prefix = options['prefix']
        self.joomla_password = options['joomla_password']
        self.devel_url = options['devel'] # FIXME

        nlimit = options['limit']
        offset = options['offset']
        if offset and not nlimit:
            raise Exception("To specify an offset, the nlimit must be supplied too.")

        self._category_content_type = ContentType.objects.get(name='category').pk
        self._article_content_type = ContentType.objects.get(name='article').pk

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
        
        self._create_collections()
        print "-> Colecciones creadas"

        categories_count = self._fetch_categories(cnx)
        print "-> {} Categorias migradas de Categorias Joomla".format(categories_count)
        self._time_from(start)
        
        min_tag_id = self._fetch_min_id(cnx)
        tags_count = self._fetch_categories_from_tags(cnx, min_tag_id)
        print "-> {} Categorias migradas de Tags Joomla".format(tags_count)
        self._time_from(start)

        print "-> Migrando articulos... (toma tiempo, con cafe)"
        articles_count, articles_images, articles_categorizations, img_success = self._fetch_content(cnx, nlimit, offset)
        print "-> {} Articulos migrados".format(articles_count)
        self._time_from(start)
        print "-> {}% Imgs ok".format(img_success)
        
        categorizations_count = self._mass_categorization(articles_categorizations)
        print "-> {} Articulos categorizados".format(categorizations_count)
        self._time_from(start)

        tag_categorizations_count = self._fetch_categorizations_from_tag_map(cnx, min_tag_id)
        tag_categorizations_count -= categorizations_count
        print "-> {} Tags como categorizaciones".format(tag_categorizations_count)
        
        images_count, related_count, article_images_count = self._create_images(articles_images)
        print "-> {} Imagenes migradas".format(images_count)
        print "-> {} Imagenes de articulos".format(article_images_count)
        print "-> {} Imagenes como contenido relacionado".format(related_count)
        self._time_from(start)
        self._fix_img_src()
        print "-> src de <img/>s ajustados"
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

    def _fetch_content(self, mysql_cnx, nlimit, offset):
        """Queries Joomla's _content table to populate Articles."""
        articles = []
        articles_images = []
        articles_categorizations = []
        # a counter to know in which proportion are we retrieving html images
        error_counter = 0
        fields = ('id', 'title', 'alias', 'introtext', 'fulltext', 'created', 'modified', 'state', 'catid', 'created_by', 'images')
        # we need to quote field names because fulltext is a reserved mysql keyword
        quoted_fields = ["`{}`".format(field) for field in fields]
        query = "SELECT {} FROM {}content".format(quoted_fields, self.table_prefix)
        query = self._clean_list(query)
        query = self._limit_query(query, nlimit, offset)
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        for content_hash in cursor:
            article = self._content_to_article(content_hash)
            if not article:
                continue
            articles.append(article)
            # this is here to have a single query to the largest table
            articles_categorizations.append( self._categorize_object(article.pk, content_hash['catid'], self._article_content_type) )
            articles_images.append( self._content_to_images(content_hash, article.pk) )
            related_images, error_counter = self._parse_html_images(content_hash, article.pk, error_counter)
            articles_images.append(related_images)
        cursor.close()
        Article.objects.bulk_create(articles)
        article_count = Article.objects.count()
        img_success_percent = 100 - (error_counter * 100 / article_count)
        return article_count, articles_images, articles_categorizations, img_success_percent

    def _create_collections(self):
        """Creates Collections infering them from Categories extensions."""
        contenidos = Collection.objects.create(id=1, name='Contenidos')
        contenidos.content_types = [ContentType.objects.get(model='article')]
        contenidos.save()
        tags = Collection.objects.create(id=2, name='Tags')
        tags.content_types = [ContentType.objects.get(model='article')] # TODO other types?
        tags.save()

    def _fetch_categories(self, mysql_cnx):
        """Queries Joomla's categories table to populate Categories."""
        fields = ('id', 'path', 'title', 'alias', 'description', 'published', 'parent_id', 'lft', 'rgt', 'level', 'extension')
        query = "SELECT {} FROM {}categories".format(fields, self.table_prefix)
        query = self._clean_tuple(query)
        # we are considering only categories for the Contents collection.
        query += " WHERE extension = 'com_content'"
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        categories = []
        for category_hash in cursor:
            category = self._category_to_category(category_hash)
            if category:
                categories.append(category)
        cursor.close()
        try:
            # save categorties in bulk so it doesn't call custom Category save, which doesn't allow custom ids
            Category.objects.bulk_create(categories)
        except IntegrityError:
            # duplicate query is expensive, we try not to perform it if we can
            categories = self._category_duplicates_uniqueness(mysql_cnx, categories)
            Category.objects.bulk_create(categories)
        Category.tree.rebuild()
        category_count = Category.objects.filter(collection_id=self._categories_collection).count()
        return category_count

    def _category_duplicates_uniqueness(self, mysql_cnx, categories):
        """find duplicate names, since AutoSlugField doesn't properly preserve uniqueness in bulk."""
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
        return categories

    def _fetch_min_id(self, mysql_cnx):
        """we need this datum so that categories and tags ids don't collide"""
        cursor = mysql_cnx.cursor()
        query = "select max(id) as min_id from {}categories".format(self.table_prefix)
        cursor.execute(query)
        min_id = cursor.fetchone()['min_id']
        return min_id

    def _fetch_categories_from_tags(self, mysql_cnx, min_id):
        """Migrate Joomla's Tags as Cyclopes Categories in a separate Collection.
           Table content_item_tags_map is the equivalent of Categorizations."""
        fields = ('id', 'parent_id', 'lft', 'rgt', 'level', 'title', 'published') # note, description, urls, path, alias, created_time
        query = "SELECT {} FROM {}tags".format(fields, self.table_prefix)
        query = self._clean_tuple(query)
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        categories = []
        for tag_hash in cursor:
            category = self._tag_to_category(tag_hash, min_id)
            categories.append(category)
        cursor.close()
        Category.objects.bulk_create(categories)
        Category.tree.rebuild()
        tag_count = Category.objects.filter(collection_id=self._tags_collection).count()
        return tag_count

    def _fetch_categorizations_from_tag_map(self, mysql_cnx, min_id):
        fields = ('type_alias', 'content_item_id', 'tag_id') # core_content_id (PK?), type_id (==type_alias), tag_date
        query = "SELECT {} FROM {}contentitem_tag_map".format(fields, self.table_prefix)
        query = self._clean_tuple(query)
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        categorizations = []
        for map_hash in cursor:
            categorization = self._tag_map_to_categorization(map_hash, min_id)
            categorizations.append(categorization)
        cursor.close()
        categorizations = [cat for cat in categorizations if cat] # clean nulls
        categorization_count = self._mass_categorization(categorizations)
        return categorization_count

    def _create_images(self, images):
        images = [image[0] for image in images if image] # FIXME
        # massive picture creation
        pictures = []
        for image_hash in images:
            picture = self._image_to_picture(image_hash)
            if not picture:
                continue
            picture.description = self._pic_info_to_description(image_hash['article_id'], image_hash['image_type'])
            pictures.append(picture)
        # clean duplicate slugs
        pictures = self._duplicate_pictures_removal(pictures)
        # bulk insert
        Picture.objects.bulk_create(pictures)
        # retrieve relation from description
        pic_relations = []
        for pic in Picture.objects.all():
            article_id, image_type = self._pic_info_from_description(pic.description)
            relation = {'picture_id': pic.pk, 'article_id': article_id, 'image_type': image_type}
            pic_relations.append(relation)
        # pass relations to queries
        self._bulk_relate_images(pic_relations)
        # clean descriptions
        Picture.objects.all().update(description='')
        return Picture.objects.count(), RelatedContent.objects.count(), Article.objects.exclude(pictures=None).count()

    def _duplicate_pictures_removal(self, pictures):
        """for bulk picture creation we treat here duplicate pictures slugs.
           since we are using article id and img src for slugs, duplicate slugs are really duplicate pictures,
           so we just remove them. there could be other strategies whenever it makes sense.
           therefore we groupi pictures indexes by slug (there is no pk yet), 
           removing all but the first of each group (original one).
           using collections.Counter is supposed to perform O(n).
           """
        # count slugs appearing more than once
        duplicate_slugs = [slug for slug, count in Counter([pic.slug for pic in pictures]).items() if count > 1]
        # find the index in this list for pictures with duplicate slugs, grouped by slug
        slush = {}
        slush = slush.fromkeys(duplicate_slugs)
        for key in slush.iterkeys(): slush[key]=[]
        for i, pic in enumerate(pictures):
            if pic.slug in duplicate_slugs:
                # remove from list immediately, before list indexes are updated FIXME
                if slush[pic.slug]:
                    pictures.pop(i)
                else:
                    slush[pic.slug].append(i)
        return pictures

    def _pic_info_to_description(self, article_id, image_type):
        to_json = {'article_id': article_id, 'image_type': image_type}
        return json.dumps(to_json)

    def _pic_info_from_description(self, description):
        info_hash = json.loads(description)
        article_id = info_hash['article_id']
        image_type = info_hash['image_type']
        return article_id, image_type

    def _mass_categorization(self, categorizations):
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
        # delete pre existent menuitem 1 because of id collision
        MenuItem.objects.all().delete()
        for menu_hash in cursor:
            if menu_types.has_key(menu_hash['menutype']):
                menuitem = self._menu_to_menuitem(menu_hash, menu_types)
                menuitems.append(menuitem)
        # skip custom save method
        MenuItem.objects.bulk_create(menuitems)
        # because of MenuItem's uniqueness constraint with parent, we can't associate parent_ids at bulk creation time
        cursor.execute(query)
        for menu_hash in cursor:
            if menu_types.has_key(menu_hash['menutype']):
                menuitem = self._menu_to_menuitem_tree(menu_hash)
                menuitem.save()
        cursor.close()
        # resetear tree ids
        MenuItem.tree.rebuild()
        return MenuItem.objects.count()

    # HELPERS

    def _clean_tuple(self, query):
        """clean tuple and quotes syntax"""
        return re.sub("[\(\)']", '', query)

    def _clean_list(self, query):
        """clean list and quotes syntax"""
        return re.sub("[\[\]']", '', query)
 
    def _tuples_to_dict(self, fields, results):
        return dict(zip(fields, results))

    def _time_from(self, start):
        now = time.time()
        ellapsed = now - start 
        print( "%.2f s" % ellapsed )

    def _limit_query(self, query, nlimit, offset):
        """Adds SQL Limit/Offset syntax to limit queries to return only nlimit rows starting from offset."""
        if nlimit:
            query +=  " LIMIT {}".format(nlimit)
        if offset:
            query += " OFFSET {}".format(offset)
        return query

    # CYCLOPE'S LOGIC

    def _site_settings_setter(self):
        settings = SiteSettings.objects.all()[0]
        site = settings.site
        if not self.devel_url:
            site.domain = "www.redecom.com.ar" 
        else:
            site.domain = "localhost:8000"

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

    def _shift_min_id(self, cat_id, min_id):
        """this method is necessary because both Joomla's Categories and Tags are Categories in Cyclope.
        we want to keep ids and their hierarchy, but we don't want them to collide, 
        so tag ids start from the greatest of categories ids."""
        result_id = cat_id + min_id
        return result_id

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
        return imagenes

    def _parse_html_images(self, content_hash, article_id, error_counter):
        """instances images from content's embedded <img> HTML tags."""
        imagenes = []
        full_content = self._joomla_content(content_hash)
        try: # FIXME x-treme hack! html.fromstring having ID collisions, collect_ids is not an option...
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
            error_counter += 1
        return imagenes, error_counter

    def _joomla_slugify(self, pk, alias):
        """joomla's URLs consist of the primary-key followed by a hyphen and the alias"""
        pk_str = str(pk)
        slug = '-'.join((pk_str, alias))
        return slug

    def _bulk_relate_images(self, images):
        """images comming from content's image column will be article images,
           images comming from within the article's content will be just related contents."""
        article_images = []
        related_images = []
        picture_type_id = ContentType.objects.get(name='picture').pk
        article_type_id = ContentType.objects.get(name='article').pk
        for image_hash in images:
            article_id = image_hash['article_id']
            picture_id = image_hash['picture_id']
            article_image_pair = (article_id, picture_id)
            if image_hash['image_type'] == 'article':
                article_images.append(article_image_pair)
            elif image_hash['image_type'] == 'related':
                related_tuple = (article_type_id, article_id, picture_type_id, picture_id)
                related_images.append(related_tuple)
        if article_images:
            article_images_chunks = self._split_large_inserts(article_images)
            for article_images_chunk in article_images_chunks:
                article_images_query = "INSERT INTO articles_article_pictures ('article_id', 'picture_id') VALUES {}".format(article_images_chunk)
                article_images_query =  self._clean_list(article_images_query)
                self._raw_sqlite_execute(article_images_query)
        if related_images:
            related_images_chunks = list(self._split_large_inserts(related_images))
            for related_images_chunk in related_images_chunks:
                related_content_query = "INSERT INTO cyclope_relatedcontent ('self_type_id', 'self_id', 'other_type_id', 'other_id') VALUES {}".format(related_images_chunk)
                related_content_query = self._clean_list(related_content_query)
                self._raw_sqlite_execute(related_content_query)

    def _raw_sqlite_execute(self, query):
        sqlite = connection.cursor()
        try:
            sqlite.execute(query)
            connection.commit()
        finally:
            sqlite.close()

    def _split_large_inserts(self, dataset):
        """split INSERT VALUES into chunks of 500, that is SQLite's SQLITE_MAX_COMPOUND_SELECT limit in v.3.7
           returns a generator, which must be instanced for ex. by the list() function"""
        n = 500
        for i in xrange(0, len(dataset), n):
            yield dataset[i:i+n]

    def _menu_type_id(self, menu_types, menutype):
        if menu_types.has_key(menutype):
            return menu_types[menutype]

    # MODELS CONVERSION

    def _content_to_article(self, content):
        """Instances an Article object from a Content hash."""
        summary, text = self._redeco_text_logic(content)
        if not text:
            return None
        slug = self._joomla_slugify(content['id'], content['alias'])
        article = Article(
            id = content['id'],
            slug = slug,
            name = content['title'],
            creation_date = content['created'] if content['created'] else datetime.now(),
            modification_date = content['modified'],
            date = content['created'],
            published = content['state']==1, # 0=unpublished, 1=published, -1=archived, -2=marked for deletion
            summary = summary,
            text = text,
            user_id = content['created_by']
        )
        return article

    def _image_to_picture(self, image_hash):
        src = self._sanitize_img_src(image_hash['src'])
        if not src: # NOTE img con src externo podrian ser ExternalContent?
            return None
        alt = image_hash['alt'] if image_hash['alt'] else ""
        name = src.split('/')[-1].split('.')[0] # get rid of path and extension
        name = slugify(name) # TODO name, alt
        # we don't really care about image's slugs, and article_id can be useful
        slug = self._joomla_slugify(image_hash['article_id'], name)
        picture = Picture(
            image = src,
            name = name,
            slug = slug,
            # creation_date = post['post_date'] TODO
        )
        return picture

    def _category_to_category(self, category_hash):
        """Instances a Category in Cyclope from Joomla's Categories table fields."""
        category = Category(
            id = category_hash['id'], # keep ids for foreign keys
            collection_id = self._categories_collection, # Contenidos
            name = category_hash['title'],
            active = category_hash['published']==1,
            parent_id = category_hash['parent_id'] if category_hash['parent_id'] != 0 else None,
            # Cyclope and Joomla use the same tree algorithm
            lft = category_hash['lft'],
            rght = category_hash['rgt'],
            level = category_hash['level'],
            tree_id = category_hash['id'] # any value, overwritten by tree rebuild
        )
        return category

    def _tag_to_category(self, tag_hash, min_id):
        category_id = self._shift_min_id(tag_hash['id'], min_id)
        parent_id = self._tree_hierarchy(tag_hash['parent_id'])
        if parent_id:
            parent_id = self._shift_min_id(parent_id, min_id)
        category = Category(
            name = tag_hash['title'],
            active = tag_hash['published']==1,
            id = category_id,
            parent_id = parent_id,
            collection_id = self._tags_collection, # Tags
            lft = tag_hash['lft'],
            rght = tag_hash['rgt'],
            level = tag_hash['level'],
            tree_id = 0, # any value, overwritten by tree rebuild
        )
        return category

    def _tag_map_to_categorization(self, map_hash, min_id):
        objeto = map_hash['content_item_id']
        cat_id = self._shift_min_id(map_hash['tag_id'], min_id)
        type_alias = map_hash['type_alias']
        if re.search('com_content.article', type_alias):
            content_type_id = self._article_content_type
            return self._categorize_object(objeto, cat_id, content_type_id)

    def _categorize_object(self, objeto, cat_id, content_type_id):
        categorization = Categorization(
            category_id = cat_id,
            content_type_id = content_type_id,
            object_id = objeto
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

    # BEGIN redeco.com.ar

    def _redeco_text_logic(self, content_hash):
        """Logica de redecom.com.ar respecto a la separacion del copete y el cuerpo del texto.
           90% de las notas en Joomla consta de ambas columnas introtext & fulltext, estas corresponden a summary & text en Cyclope
           En el 10% restante intentamos identificar las clases CSS bajada y cuerponota.
           Este diez se reparte en un 7% que solo tienen introtext (de estos 65% aprox tiene la clase CSS 'bajada'),
           y en un 3% que solo tienen fulltext (de estos casi ninguno tiene la clase CSS 'bajada'). 
           La clase CSS 'cuerponota' se repite y mas que sematico probablemente tenga un significado estetico.
           Asi estamos separando el 96% de los articulos aprox en summary y text.
           NOTE:
           - Que sentido en la funcionalidad Read More de Joomla tiene que solo haya texto en fulltext?
           - Cuando se puede separar por ambos introtext/fulltext, en el 70% de los casos coincide con la clase bajada en el introtext
        """
        summary, text = None, None
        introtext = content_hash['introtext']
        fulltext = content_hash['fulltext']
        if introtext and fulltext:
            summary = introtext
            text = fulltext
            # correccion de cierres en bd redeco, muchos cierran los tags en la columna fulltext
            text, apendix = self._correccion_cierre(text)
            if apendix:
                summary += apendix 
        elif introtext:
            summary, text = self._redeco_css_bajada(introtext)
        elif fulltext:
            summary, text = self._redeco_css_bajada(fulltext)
        # limpiar breaks al inicio y al final
        if summary:
            summary = self._clean_html_breaks(summary)
        if text:
            text = self._clean_html_breaks(text)
        return summary, text

    def _redeco_css_bajada(self, content):
        """interpreta el contenido de un articulo como html.
           si encuentra una etiqueta con la clase bajada, la extrae como el summary, separandolo del text."""
        summary, text = None, None
        tree = html.fromstring(content)
        if re.search('class="bajada"', content):
            sel = CSSSelector('.bajada')
            bajada = sel(tree)[0]
            summary = bajada.text
            try:
                tree.remove(bajada)
            except ValueError:
                pass # it does work
            text = html.tostring(tree)
        if summary:
            return summary, text
        else:
            return "", content    

    def _correccion_cierre(self, text):
        """especifico para la bd de redeco.com.ar
           1183 registros cierran en fulltext las etiquetas que abren en introtext
           retornamos el texto limpio, y tambien las etiquetas de cierre, si se hallan,
           estas son 'devueltas' a la columna introtext."""
        regex = "^(\s*<\s*/[a-zA-Z]+\s*>)+"
        match = re.search(regex, text)
        if match:
            apendix = match.group()
            end = match.end()
            clean_text = text[match.end():]
            return clean_text, apendix
        return text, None

    def _clean_html_breaks(self, txt):
        """limpia las etiquetas <br /> del principio y del final"""
        regex_start = "^(\s*<br\s?/>)+"
        match_start = re.search(regex_start, txt)
        regex_end = "(<br\s?/>\s*)+$"
        match_end = re.search(regex_end, txt)
        if match_start:
            txt = txt[match_start.end():]
        if match_end:
            txt = txt[:match_end.start()]
        return txt

    def _sanitize_img_src(self, src):
        """sanitize img URLs in redeco.com.ar, most of the corrections needed are relativizing canonical URLs
           75,5% de los src estan bien, 23,5% tienen URL canonica, la cual relativizamos.
           1% son src a sitios externos, los cuales descartamos.
           tambien descartamos 12 excepciones en www.redeco.com.ar/ana y www.redeco.com.ar/opinion"""
        regex_ok = "^images/"
        regex_canonical = "^(http://)?(www.redeco.com.ar/)"
        if re.search(regex_ok, src): # images/etc/img.png -> /media/images/etc/img.png
            return src # 75,5%
        match_canonical = re.search(regex_canonical, src)
        if match_canonical: # 23,5%
            regex_nv = regex_canonical + "nv/"
            match_nv = re.search(regex_nv, src)
            if match_nv: # (http://)www.redeco.com.ar/nv/images/etc/img.png -> /media/images/etc/img.png 
                 src = src[match_nv.end():]
                 return src
            else: # www.redeco.com.ar/ana/ www.redeco.com.ar/opinion/ se descartan
                return None
        return None # 1%, src externos

    def _fix_img_src(self):
        """Con queries actualiza las fuentes (src) de las imagenes en los campos summary y text de Article."""
        queries = []
        fields = ['summary', 'text']
        # 2734 / 2805 (97,5%)
        nv_query = "UPDATE articles_article SET {} = REPLACE({}, '<img src=\"http://www.redeco.com.ar/nv/', '<img src=\"/media/');"
        # 2805 (2,4%) son img con otros atributos antes del src, se reemplaza solo el src. # 2734 + 67 + 4(descarte)=28015
        nv_query_2 = "UPDATE articles_article SET {} = REPLACE({}, 'src=\"http://www.redeco.com.ar/nv/', 'src=\"/media/') WHERE {} LIKE '%%<img%%src=\"http://www.redeco.com.ar/nv/%%';"
        # 3988 imgs tienen src="images/...", (de las cuales 132 tienen atributos antes del src)
        image_query = "UPDATE articles_article SET {} = REPLACE({}, 'src=\"images/', 'src=\"/media/images/');"
        # execute for summary and text
        for field in fields:
            queries.append(nv_query.format(field, field))
            queries.append(nv_query_2.format(field, field, field))
            queries.append(image_query.format(field, field))
        for query in queries:
            self._raw_sqlite_execute(query)

    # END redeco.com.ar

    def _menu_type_to_menu(self, menu_type_hash):
        menu = Menu(
            id = menu_type_hash['id'],
            name = menu_type_hash['title'],
            main_menu = False,
        )
        return menu

    def _menu_to_menuitem(self, menu_hash, menu_types):
        menu_id = self._menu_type_id(menu_types, menu_hash['menutype'])
        parent_id = self._tree_hierarchy(menu_hash['parent_id'])
        content_object_type, object_id = self._menu_content_object(menu_hash['link'])
        menuitem = MenuItem(
            id = menu_hash['id'],
            menu_id = menu_id,
            name = menu_hash['title'],
            site_home = menu_hash['home']==1,
            url = menu_hash['path'], # TODO slugify(path), alias not unique
            active = menu_hash['published']==1,
            persistent_layout = False,
            lft = menu_hash['lft'],
            rght = menu_hash['rgt'],
            level = menu_hash['level'],
            tree_id = menu_hash['id'], # any value, overwritten by tree rebuild
            content_type_id = content_object_type,
            object_id = object_id,
            content_view = self._menu_category_view,
            view_options = self._menu_category_view_options
        )
        return menuitem

    def _menu_to_menuitem_tree(self, menu_hash):
        menuitem = MenuItem.objects.get(pk=menu_hash['id'])
        menuitem.parent_id = self._tree_hierarchy(menu_hash['parent_id'])
        return menuitem

    def _tree_hierarchy(self, parent_id):
        """0 is default value, and 1 is Menu Item Root, a Menu with no menutype
           the same logic is valid for Tags tree Root ids"""
        if parent_id != 0 and parent_id != 1:
            return parent_id
        return None

    def _menu_content_object(self, link):
        """inferr a menu's content object from its joomla link
           for now we treat just categories, other types might need urls instead of ids"""
        if re.search('category', link):
            link_category_id = link.split('&')[-1] # &id=123
            link_category_id = link_category_id.split("=")[-1] # 123
            category_id = int(link_category_id)
            return self._category_content_type, category_id
        return None, None
