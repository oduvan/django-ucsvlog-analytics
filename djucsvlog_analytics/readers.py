from djucsvlog import settings as LS
from ucsvlog.Reader import Reader as BaseReader

from djucsvlog_analytics.rows import Row

class Reader(BaseReader):
    def __init__(self,*args,**kwargs):
        kwargs['close_row'] = LS.LOG_CLOSE_ROW
        return super(Reader,self).__init__(*args,**kwargs)

class BaseStreamReader(Reader):
    cls_row = Row
    
    def write_row(self,arr_row):
        return self.cls_row(arr_row, reader=self)
    
class BaseCollectReader(BaseStreamReader):
    
    def set_command(self,command):
        self.command = command
    
    def write_row(self,arr_row):
        row = super(BaseCollectReader,self).write_row(arr_row)
        
        if self.command.filter_row(row):
            self.command.collect_row(row)
            

