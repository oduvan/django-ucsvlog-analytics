from optparse import make_option
from djucsvlog_analytics.analytic_commands import BaseAnalyticCommand
from djucsvlog import settings as LS

browser_ids = set()

class Command(BaseAnalyticCommand):
    help = 'calculate all uniques request for special url'
    
    option_list = BaseAnalyticCommand.option_list + (
        make_option('--host-level',
            dest='host_level',
            default='2',
            help='level of domain to collect (Need a "http_host" is UCSVLOG_REQUEST_FIELDS)'),
        make_option('--null-host-name',
            dest='null_host_name',
            default='nohost',
            help='If you do not have "http_host" in  UCSVLOG_REQUEST_FIELDS than we replace host name by this value'),
        make_option('--test',
            dest='test',
            default=None,
            action='store_true',
            help='integrity test'),
        )

        
    
    def initial_options(self,options):
        super(Command,self).initial_options(options)
        self.collect_host = 'http_host' in LS.REQUEST_FIELDS
        self.host_level = int(options['host_level'])
        self.null_host_name = options['null_host_name']
        self.test = options['test']
        self.a_req = {}
        
    
    def get_info_data(self):
        ret = super(Command,self).get_info_data()
        ret.update({
            'with_hosts':self.collect_host,
            'host_level':self.host_level
            })
        return ret
            
    
    def initial_tables(self):
        cur = super(Command,self).initial_tables()
        
        cur.execute('''
            create table  if not exists "host" (
                id integer primary key autoincrement,
                name text unique
            )
        ''')
        
        cur.execute('''
            create table  if not exists "useruuid" (
                id integer primary key autoincrement,
                host_id integer,
                uuid text unique,
                http_user_agent text,
                remote_addr text,
                req_http_accept_language text
            )
        ''')
        
        cur.execute('''
            create index if not exists "useruuid_host_id"  on "useruuid" (host_id)
        ''')
        
        cur.execute('''
            create table if not exists "request" (
                id integer primary key autoincrement,
                useruuid_id integer,
                req_get text,
                req_post text,
                req_path text,
                req_http_referer text,
                req_remote_addr text,
                in_userid text,
                close_ctype text,
                close_content text,
                close_status text,
                close_headers text,
                req_time integer,
                req_index text,
                in_index text,
                close_index text
            )
        ''')
        
        
        cur.execute('''
            create index if not exists "request_useruuid_id"  on "request" (useruuid_id,req_time)
        ''')
        
        if self.geoip_db:
            cur.execute('alter table "useruuid" add column "geoip_country_code" text')
            cur.execute('alter table "request" add column "geoip_country_code" text')
            self.set_info('geoip_db',self.geoip_db.filename)
        
        cur.execute('''
            create table if not exists "request_log" (
                id integer primary key autoincrement,
                request_id integer,
                i_time integer,
                i_index text,
                a_index text,
                d1 text,
                d2 text,
                d3 text,
                d4 text,
                d5 text,
                d6 text,
                d7 text,
                d8 text,
                d9 text,
                d0 text
            )
        ''')
        
        cur.execute('''
            create table if not exists "request_link" (
                id integer primary key autoincrement,
                request_id integer,
                a_index text
            )
        
        ''')
        
        cur.execute('''
            create index if not exists "request_links_link"  on "request_link" (a_index,request_id)
        ''')
        
        cur.execute('''
            create index if not exists "request_links_request_id"  on "request_link" (request_id)
        ''')
    
    def create_req(self,row,uuid_id):
        cur = self.c.cursor()
        cur.execute('insert into "request" (useruuid_id, req_get, req_post, req_path, req_http_referer, req_remote_addr, req_index,req_time) values (?,?,?,?,?,?,?,?)',\
                    [uuid_id, row.get_raw_data('get'),row.get_raw_data('post'), row.get_raw_data('path'), row.get_raw_data('http_referer'), row.get_raw_data('remote_addr'),\
                     row.raw[0], row.index_time])
        req_uuid = cur.lastrowid
        if self.geoip_db:
            geoip = self.geoip_db.get_city_info(row.get_raw_data('remote_addr'))
            if geoip:
                cur.execute('update "request" set geoip_country_code=? where id=?',[geoip['country_code'],req_uuid])
        return req_uuid
    
    def remove_request_links(self,request_id):
        cur = self.c.cursor()
        cur.execute('delete from "request_link" where request_id=?',[request_id])
    
    def update_request_in(self,row,request_id):
        cur = self.c.cursor()
        cur.execute('update "request" set in_userid=?, in_index=? where id=?',[row.get_raw_data('userid'),row.raw[0],request_id])
        
    def update_request_close(self,row,request_id):
        cur = self.c.cursor()
        cur.execute('update "request" set close_ctype=?, close_content=?, close_status=?,close_headers=?,close_index=? where id=?',\
                    [row.get_raw_data('ctype'),row.get_raw_data('content'),row.get_raw_data('status'),row.get_raw_data('headers'),row.raw[0],request_id])
    
    def insert_request_log(self,row,request_id):
        cur = self.c.cursor()
        data_10 = row.raw_data + [None]*(10 - len(row.raw_data))
        
        cur.execute('''
            insert into "request_log" (request_id, i_index, i_time, a_index , d1, d2, d3, d4, d5, d6, d7, d8, d9, d0) 
                            values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''',[request_id,row.raw[0], row.index_time, row.raw[1]] + data_10)
    
    def create_request_link(self,request_id, a_index):
        cur = self.c.cursor()
        cur.execute('insert into "request_link" (request_id,a_index) values (?,?)',\
                    [request_id, a_index])
    
    def get_request_by_index(self,a_index):
        cur = self.c.cursor()
        cur.execute('select request.id from "request_link" join "request" on request_link.request_id=request.id where request_link.a_index=?',[a_index])
        try:
            return cur.fetchone()[0]
        except TypeError:
            return None
    
    def get_host_name(self,row):
        if hasattr(row,'data_http_host'):
            return '.'.join(row.data_http_host.split('.')[(-1)*self.host_level:])
        else:
            return self.null_host_name
    
    def get_host_id(self, row):
        cur = self.c.cursor()
        host_name = self.get_host_name(row)
        cur.execute('select id from host  where name=?', [host_name])
        try:
            return cur.fetchone()[0]
        except TypeError:
            cur.execute('insert into "host" ( name ) values (?)', [host_name])
            return cur.lastrowid
    
    def get_uuid_id(self, row):
        cur = self.c.cursor()
        cur.execute('select id from useruuid  where uuid=?',[row.data_browser_uuid])
        try:
            return cur.fetchone()[0]
        except TypeError:
            cur.execute('insert into "useruuid" (host_id,uuid,http_user_agent,remote_addr,req_http_accept_language) values (?,?,?,?,?)',\
                        [self.get_host_id(row), row.data_browser_uuid, row.get_raw_data('http_user_agent'), row.get_raw_data('remote_addr'),\
                                                row.get_raw_data('http_accept_language')])
            useruuid_id = cur.lastrowid
            if self.geoip_db:
                geoip = self.geoip_db.get_city_info(row.get_raw_data('remote_addr'))
                if geoip:
                    cur.execute('update "useruuid" set geoip_country_code=? where id=?',[geoip['country_code'],useruuid_id])
                    
            return useruuid_id
    
    def collect_row(self,row):
        super(Command,self).collect_row(row)
        cur = self.c.cursor()
        if row.is_a_req:
            uuid_id = self.get_uuid_id(row)
            
            request_id = self.create_req(row,uuid_id)
            
            self.create_request_link(request_id, row.raw[0])
            
            
            
        else:
            
            request_id = self.get_request_by_index(row.raw[1])
            if request_id is None:
                if self.test:
                    print 'Lost row {},{}'.format(row.raw[0],row.raw[1])
                return
            
            if row.is_a_log:
                self.create_request_link(request_id, row.raw[0])
            
            if row.is_a_in:
                self.update_request_in(row, request_id)
            elif row.is_c_req:
                self.update_request_close(row, request_id)
                self.remove_request_links(request_id)
            else:
                self.insert_request_log(row,request_id)

    def output_results(self):
        super(Command,self).output_results()
        if not self.test:
            return

        cur = self.c.cursor()
        cur.execute('select req_index from request where close_index is null')
        print 'Broken requests:'
        for row in cur:
            print row[0]
    
