from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
import mysql.connector

class Command(BaseCommand):
    help = """
    Migrates a site in Joomla to CyclopeCMS.
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
        
        # TODO todo...
        
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
    
    def _fetch_content(self, mysql_cnx, site):
    """Queries Joomla's _content table to populate Articles."""
        fields = ('',)
        query = re.sub("[()']", '', "SELECT {} FROM ".format(fields))+self.table_prefix+"content"
        cursor = mysql_cnx.cursor()
        cursor.execute(query)
        # STUB
        cursor.close()
        
