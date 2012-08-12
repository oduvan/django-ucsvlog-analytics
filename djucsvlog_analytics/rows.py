from djucsvlog import settings as LS
from datetime import datetime
import time
def list_settings_names(names):
    '''
        return list of stings for name of fields. Cuz field can by a function
    '''
    for item in names:
        if isinstance(item, (str,unicode)):
            yield item
            continue
        assert callable(item)
        
        if item.func_name == '<lambda>':
            yield str(names.index(item))
        else:
            yield item.func_name

def parse_index_field(data):
    try:
        return datetime.strptime(data.split(';')[0],'%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        return datetime.strptime(data.split(';')[0],'%Y-%m-%dT%H:%M:%S')

class Row(object):
    def __init__(self,row, reader=None):
        self.reader = reader
        self.raw = row
        self.children = []
        
        self.index = row[0]
        self.parent_index = row[1]
        self.raw_base = self.raw[2:len(LS.LOG_BASE)+2]
        
        for prop_name,prop_val in zip(list_settings_names(LS.LOG_BASE),self.raw_base):
            setattr(self,'base_'+prop_name,prop_val)

        self.log_name = self.raw[len(self.raw_base) + 2]
        
        self.is_a_log = self.log_name == 'a_log'
        self.is_c_log = self.log_name == 'c_log'
        
        self.raw_data = self.raw[len(self.raw_base) + 3:]
        
        self.a_log = self.raw_data.pop(0) if self.is_a_log or self.is_c_log else None
        
        
        self.is_a_req = self.a_log == LS.REQ_LOG_NAME and self.is_a_log
        
        self.is_a_in = self.a_log == LS.VIEW_LOG_NAME and self.is_a_log
        
        self.is_c_req = self.a_log == LS.REQ_LOG_NAME and self.is_c_log
        
        
        if self.is_a_req:
            data_fields = list_settings_names(LS.REQUEST_FIELDS)
        elif self.is_c_req:
            data_fields = list_settings_names(LS.RESPONSE_FIELDS)
        elif self.is_a_in:
            data_fields = list_settings_names(LS.VIEW_OPEN_FIELDS)
        else:
            data_fields = map(str,range(1,len(self.raw_data)+1))
            
        for prop_name, prop_val in zip(data_fields,self.raw_data):
            setattr(self, 'data_'+prop_name, prop_val)
        
         
    def get_raw_data(self,name,default_value=None):
        return getattr(self,'data_'+name,default_value)
    
    def get_index_data(self):
        return self.raw[:(-1)*len(self.raw_data)]
    
    
    
    
    __index_datetime = None
    @property
    def index_datetime(self):
        if self.__index_datetime is not None:
            return self.__index_datetime
        self.__index_datetime = parse_index_field(self.index)
        return self.__index_datetime
        
    __index_time = None    
    @property
    def index_time(self):
        if self.__index_time is not None:
            return self.__index_time
        self.__index_time = time.mktime(self.index_datetime.timetuple())
        return self.__index_time
    
    
    def add_child(self,row):
        self.children.append(row)