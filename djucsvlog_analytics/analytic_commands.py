from datetime import datetime
from optparse import make_option
from django.core.management.base import BaseCommand
import djucsvlog_analytics.settings as S
import os
import codecs
import sqlite3
from djucsvlog_analytics.readers import BaseStreamReader, BaseCollectReader


class BaseSimpleAnalyticCommand(BaseCommand):
    args = '<logfile1 logfile2 ...>'
    
    cls_reader = BaseCollectReader
    
    option_list = BaseCommand.option_list + (
        make_option(
            '--from',
            dest='from_datetime',
            default=None,
            help='From Datetime'
        ),
        make_option(
            '--to',
            dest='to_datetime',
            default=None,
            help='To Datetime'
        ),
    )

    def initial_options(self, options):
        convert_datetime_str = lambda s: datetime.strptime(s, '%Y-%m-%dT%H:%M:%S')
        from_datetime = (
            convert_datetime_str(options['from_datetime'])
            if options['from_datetime']
            else datetime(1, 1, 1, 0, 0)
        )
        self.max_datetime = self.from_datetime = from_datetime
        to_datetime = (
            convert_datetime_str(options['to_datetime'])
            if options['to_datetime']
            else datetime.now()
        )
        self.min_datetime = self.to_datetime = to_datetime

    def filter_row(self, row):
        return self.from_datetime < row.index_datetime < self.to_datetime 
    
    def collect_row(self, row):
        cur_datetime = row.index_datetime
        self.min_datetime = min(cur_datetime, self.min_datetime)
        self.max_datetime = max(cur_datetime, self.max_datetime)
    
    def handle_file(self, file_name):
        anal = self.cls_reader(file_name)
        anal.set_command(self)
        anal.import_all()
    
    def handle(self, *args, **options):
        self.initial_options(options)
        for file_name in args:
            self.handle_file(file_name)
        self.output_results()
    
    def output_results(self):
        pass
        

class GeoIPDB(object):
    def __init__(self,filename, *args, **kwargs):
        self.filename = filename
        self.c = sqlite3.connect(filename)
        self.c.row_factory = sqlite3.Row
        self._cache_country_code = {}
        object.__init__(self, *args, **kwargs)
    
    def ip2long(self,ip):
        "Converts an IP address into a long suitable for querying."
        w, x, y, z = map(int,ip.split('.'))
        return 16777216*w + 65536*x + 256*y + z
    
    def get_city_info(self,ip):
        long_ip = self.ip2long(ip)
        cur = self.c.cursor()
        
        cur.execute('''select net_country.name country_name, net_country.code country_code, net_city.* from net_city_ip
         join net_city on net_city.id=net_city_ip.city_id 
         join net_country on net_city.country_id=net_country.id where begin_ip < ? order by begin_ip desc''',[long_ip])
        return cur.fetchone()
    
    def get_name_by_code(self,code):
        if code in self._cache_country_code:
            return self._cache_country_code[code]
        cur = self.c.cursor()
        cur.execute('select name from net_country where code=?',[code])
        ret = self._cache_country_code[code] = cur.fetchone()[0]
        return ret

    def get_code_by_name(self, name):
        cur = self.c.cursor()
        cur.execute('select code from net_country where name=?', [name])
        country_code = cur.fetchone()
        if not country_code:
            print "Country: %s not found" % name
            return
        return country_code[0]
        




class BaseAnalyticCommand(BaseSimpleAnalyticCommand):
    
    option_list = BaseSimpleAnalyticCommand.option_list + (
        make_option('--out',
            dest='out_file',
            default=None,
            help='out convertion file'),
        make_option('--force-new',
            dest='force_new',
            default=False,
            action = 'store_true',
            help='Remove old file, if it exists'),
        make_option('--geoip-db',
            dest='geoip_db',
            default=None,
            help='sqlite3 file with geoip data'),
        )
    
    
    def initial_options(self,options):
        super(BaseAnalyticCommand,self).initial_options(options)
        if options['force_new']:
            try:
                os.remove(options['out_file'])
            except OSError:
                pass
            
        self.c = sqlite3.connect(options['out_file'])
        self.files = []
        self.out_file = options['out_file']
        self.geoip_db = GeoIPDB(options['geoip_db']) if options['geoip_db'] else None
        self.initial_tables()
        
    def initial_tables(self):
        cur = self.c.cursor()
        cur.execute('''
            create table  if not exists "file" (
                id integer primary key autoincrement,
                "name" text,
                "from_datetime" text,
                "to_datetime" text
            )
        ''')
        
        cur.execute('''
            create table  if not exists "info" (
                "name" text primary key,
                "value" text
            )
        ''')
        
        return cur

    def get_info(self,name):
        cur = self.c.cursor()
        cur.execute('select value from "info" where name=?', name)
        return cur.fetchone()[0]
    
    def set_info(self,name,value):
        cur = self.c.cursor()
        cur.execute('update "info" set value=? where name=?',[value,name])
        if cur.rowcount:
            return
        cur.execute('insert into "info" (name,value) values (?,?)',[name,value])
    
    def add_file(self,name, from_datetime, to_datetime):
        cur = self.c.cursor()
        cur.execute('insert into "file" (name,from_datetime,to_datetime) values(?,?,?)' , (name, from_datetime.isoformat(), to_datetime.isoformat()))
    
    def set_cur_file(self,file_name):
        self.cur_file = os.path.abspath(file_name)
        self.cur_min_datetime = self.to_datetime
        self.cur_max_datetime = self.from_datetime
        
    def add_cur_file(self):
        self.add_file(self.cur_file, self.cur_min_datetime, self.cur_max_datetime)
        
    def filter_row(self,row):
        return self.from_datetime < row.index_datetime < self.to_datetime 
    
    def output_results(self):
        print 'From : %s' % self.min_datetime.isoformat()
        print 'To: %s' % self.max_datetime.isoformat()
        
    
    def handle_file(self,file_name):
        self.set_cur_file(file_name)
        super(BaseAnalyticCommand,self).handle_file(file_name)
        self.add_cur_file()

    def handle(self, *args, **options):
        super(BaseAnalyticCommand,self).handle(*args, **options)
        self.c.commit()

class BaseAnalyticElement(object):
    result = None

    def command_analyse(self,command):
        self.command = command
        self.result = self.all_analyse()
    
    def each_connection(self,func,func_kwargs=None,zip_func=None,zip_func_kwargs=None):
        if func_kwargs is None:
            func_kwargs = {}
        ret = []
        for item in self.command.connections:
            self.c = item
            ret.append(func(**func_kwargs))
        
        if not zip_func:
            return ret
        
        if zip_func_kwargs is None:
            zip_func_kwargs = {}
            
        return zip_func(ret,**zip_func_kwargs)
    
    def all_analyse(self):
        return self.each_connection(self.analyse,None,self.zip_analyse_results)
    
    def analyse(self):
        raise ImplementationError('analyse')
    
    def zip_analyse_results(self):
        raise ImpplementationError('zip_analyse_results')

    def output_results(self):
        print self.__class__, 'Done'


class BaseAnalyticReadCommand(BaseCommand):
    
    option_list = BaseCommand.option_list + (
        make_option('--ipy',
            dest='ipy',
            default=None,
            action = 'store_true',
            help='Open IPython console'),
        make_option('--config',
            dest='config',
            default=None,
            help='PyConfig File'),
         make_option('--output-limit',
            dest='output_limit',
            default=None,
            help='Output limit'), 
        make_option('--geoip-db',
            dest='geoip_db',
            default=None,
            help='sqlite3 file with geoip data'),
        )
    
    def eval_config(self,file,options):
        ev_globals = {}
        exec(codecs.open(file,'r','utf-8').read(),ev_globals)
        if 'OPTIONS' not in ev_globals:
            raise ValueError('PyConfig file has to contain OPTIONS dict')
        
        for name,value in ev_globals['OPTIONS'].items():
            if name  in options and options[name] is not None:
                continue
            options[name] = value
            
    
    def initial_options(self,options):
        if options['config'] is not None:
            self.eval_config(options.pop('config'),options)
            
        self.ipy = options['ipy']
        self.output_limit = int(options['output_limit'] or 0)
        self.geoip_db = GeoIPDB(options['geoip_db']) if options['geoip_db'] else None
        self.analyse_elements = []
        
    
    def add_analyse_element(self,el):
        self.analyse_elements.append(el)
    
    def initial_connections(self,args):
        self.connections = []
        for item in args:
            con = sqlite3.connect(item)
            con.row_factory = sqlite3.Row
            self.connections.append(con)
        self.c = self.connections[0]
        
    def close_connections(self):
        for item in self.connections:
            item.close()
        
    
    def handle(self, *args, **options):
        self.initial_options(options)
        self.initial_connections(args)
        
        self.analyse()
        
        if self.ipy:
            self.exec_ipy()
        else:
            self.output_results()
        
        self.close_connections()
    
    
    
    def exec_ipy(self):
        from IPython import embed
        embed()
        
    
    
    def main_output_results(self):
        'main analyse results'
        pass
    
    def output_results(self):
        self.main_output_results()
        for item in self.analyse_elements:
            item.output_results()
            
    def analyse(self):
        for item in self.analyse_elements:
            item.command_analyse(self)
        


        

class BaseSimpleStreamCommand(BaseCommand):
    cls_reader = BaseStreamReader
    collected_rows = 0

    def __init__(self,*args,**kwargs):
        super(BaseSimpleStreamCommand,self).__init__(*args,**kwargs)
        self.i = {}
        self.streams = []
        self.readers = []
    
    def handle(self, *args, **options):

        self.init_options(*args, **options)

        self.collect_streams()

        if not self.streams:
            return
        
        self.handle_streams()

            
        self.clear_too_old_data()
        self.update_collected_info()
    

    
    def read_stream_folder(self, folder, matches):
        for item in os.listdir(folder):
            if not self.match_file(item,matches):
                continue
            self.add_stream_reader(os.path.join(folder,item))
    
    def match_file(self,filename,matches):
        #analyse only filename without 
        for re_filename in matches:
            if re_filename.match(filename):
                return True
        return False

    def get_analyse_position(self,filename):
        return 0
        
    
    def add_stream_reader(self, filename):
        read_position = self.get_analyse_position(filename)
        if read_position is None:
            return
        reader = self.cls_reader(filename,seek=read_position)
        self.readers.append(reader)
        self.streams.append( reader.all_records())
    


    def handle_streams(self):
        streams = []
        last_rows = []
        for stream in self.streams:
            try:
                last_rows.append(stream.next())
            except StopIteration:
                continue
            streams.append(stream)
            
        last_rows_index = map(lambda row: row.index_datetime, last_rows)
        if not last_rows_index:
            return
        while True:
            row_index = last_rows_index.index(min(last_rows_index))
            row = last_rows[row_index]
            self.base_collect_row(row)
            try:
                new_row = last_rows[row_index] = streams[row_index].next()
                last_rows_index[row_index] = new_row.index_datetime
            except StopIteration:
                last_rows.pop(row_index)
                streams.pop(row_index)
                last_rows_index.pop(row_index)
            if not streams:
                break
        
    
    def base_collect_row(self, row):
        if not self.filter_row(row):
            return
        
        self.collected_rows += 1
        self.collect_row(row)
        
        if not self.collected_rows % S.STREAM_CLEAR_EVERY:
            self.clear_too_old_data()
            
        
    
    def filter_row(self,row):
        return True
    
    def collect_row(self,row):
        pass
    
    def update_collected_info(self):
        pass

    def clear_too_old_data(self):
        pass

    def collect_streams(self):
        pass

    def init_options(self,*args,**kwargs):
        pass


class BaseFileStreamCommand(BaseSimpleStreamCommand):
    def init_options(self,*args,**kwargs):
        self.files = args
    def collect_streams(self):
        for filename in self.files:
            self.add_stream_reader(filename)

class BaseStreamCommand(BaseSimpleStreamCommand):

    def get_stream_index(self):
        return S.STREAM_INDEX

    def handle(self, *args, **options):
        if not os.path.exists(self.get_stream_index()):
            self.create_stream_index()
        else:
            self.init_stream_index()


        super(BaseStreamCommand,self).handle(*args, **options)

        for item in self.readers:
            self.db_update_file(item)

        self.c.commit()


    def init_stream_index(self):
        self.c = sqlite3.connect(self.get_stream_index())
        self.c.row_factory = sqlite3.Row

    def create_stream_index(self):
        self.init_stream_index()
        cur = self.c.cursor()
        cur.execute('''
            create table if not exists "file" (
                id integer primary key autoincrement,
                "name" text UNIQUE,
                "last_read_position" int,
                "last_size" int
            )
        ''')

    def get_analyse_position(self,filename):
        cur = self.c.cursor()
        cur.execute('select * from "file" where "name"=?',[filename])
        db_info = cur.fetchone()
        if db_info is None:
            cur.execute('insert into "file"("name","last_read_position", "last_size") values (?,0,0)', [filename])
            return 0 #read from begining
        if db_info['last_size'] < os.path.getsize(filename):
            return db_info['last_read_position'] # read from last position
        return None # no need to read

    def db_update_file(self,reader):
        filename = reader.filename
        cur = self.c.cursor()
        cur.execute('update "file" set "last_read_position"=?, "last_size"=? where "name"=?',\
            [reader.tell(), os.path.getsize(filename),filename])

    def collect_streams(self):
        for folder, matches in S.STREAM_FOLDERS_MATCHES_RE.items():
            self.read_stream_folder(folder, matches)


class BaseStreamBlockCommand(BaseStreamCommand):
    def __init__(self,*args,**kwargs):
        super(BaseStreamBlockCommand,self).__init__(*args,**kwargs)
        self.a_rows = {}
        self.lost_rows = 0
    
    def collect_row(self,row):
        #import ipdb; ipdb.set_trace();
        if row.parent_index:
            if row.parent_index in self.a_rows:
                self.a_rows[row.parent_index].add_child(row)
            else:
                self.lost_rows += 1
                return

        if row.is_a_log:
            self.a_rows[row.index] = row
            return

        
        if row.is_c_req:
            complete_block  = self.a_rows[row.parent_index]
            self.remove_children(complete_block)
            self.collect_block(complete_block)
    
    def remove_children(self,row):
        try:
            del self.a_rows[row.index]
        except KeyError:
            return
        for item in row.children:
            if item.is_a_log:
                self.remove_children(item)
                
            
        
        
        
        
        
        