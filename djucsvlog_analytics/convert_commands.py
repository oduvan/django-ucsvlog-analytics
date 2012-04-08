from djucsvlog_analytics.analytic_commands import BaseStreamCommand
from djucsvlog_analytics.rows import parse_index_field

import djucsvlog_analytics.settings as S
from djucsvlog import settings as LS
import codecs
from datetime import datetime
import os



class BaseConvertBlockCommand(BaseStreamCommand):
    def get_stream_index(self):
        return S.CONVERT_INDEX
    def __init__(self,*args,**kwargs):
        super(BaseConvertBlockCommand,self).__init__(*args,**kwargs)
        self.a_indexes = {}
        self.log_fh = {}
    def get_analyse_position(self,filename):
        dt_file = datetime.fromtimestamp(os.path.getmtime(filename))
        if dt_file + S.CONVERT_OLD_TIME < datetime.now():
            return super(BaseConvertBlockCommand,self).get_analyse_position(filename)
    
    def filter_row(self,row):
        if row.is_a_req:
            return True
        return row.parent_index in self.a_indexes
    
    def collect_row(self, row):
        if not  self.filter_convert_row(row):
            return
        converted_raw_data = self.convert_row(row)
        if converted_raw_data is None:
            return
        
        converted_data = [row.index,row.parent_index] + self.convert_call_data(row) + converted_raw_data
        
        if row.is_a_log:
            self.a_indexes[row.index] = row.parent_index
        
        if converted_data[1]:
            first_parent_index = self.get_first_parent_index(converted_data[1])
        else:
            first_parent_index = converted_data[0]
            
        
        
        log_filename = self.get_log_filename(parse_index_field(first_parent_index), row = row)
        
        if log_filename in self.log_fh:
            fh = self.log_fh[log_filename]
        else:
            fh = self.log_fh[log_filename] = codecs.open(log_filename,'a','utf8')
        
        fh.write('\n"' + ',"'.join(map(lambda a:a.replace('"','""'),converted_data)))
    
    def get_first_parent_index(self, row_index):
        if row_index  in self.a_indexes and self.a_indexes[row_index]:
            return self.get_first_parent_index(self.a_indexes[row_index])
        return row_index
    
    def get_log_filename_replace_dict(self, now, row):
        return   {
            'year':now.year,
            'syear':unicode(now.year)[2:],
            'month':now.month,
            'day':now.day,
            'hour':now.hour,
            '2_hour':(now.hour/2)*2,
            '3_hour':(now.hour/3)*3,
            '5_hour':(now.hour/5)*5,
            'minute':now.minute,
            '0month': '%0.2d' % now.month,
            '0day': '%0.2d' % now.day,
            '0hour': '%0.2d' % now.hour,
        }
    
    def get_log_filename(self,now, row):
        return S.CONVERT_FILE % self.get_log_filename_replace_dict(now, row)
    
        
    
            
    def convert_row(self,row):
        if row.is_a_req:
            data = self.convert_a_req_row(row)
            if data is None:
                return
            return ['a_log', LS.REQ_LOG_NAME] + data
        if row.is_c_req:
            data = self.convert_c_req_row(row)
            if data is None:
                return
            return ['c_log', LS.REQ_LOG_NAME] + data
        if row.is_a_in:
            data = self.convert_a_in_row(row)
            if data is None:
                return
            return ['a_log', LS.VIEW_LOG_NAME] + data
        return self.convert_other_row(row)
    
    def convert_call_data(self, row):
        return []
        
    
    def convert_a_req_row(self,row):
        return map(lambda a,row=row: row.get_raw_data(a,''),S.CONVERT_REQUEST_FIELDS)
    
    def convert_c_req_row(self,row):
        return map(lambda a,row=row: row.get_raw_data(a,''),S.CONVERT_RESPONSE_FIELDS)
    
    def convert_a_in_row(self,row):
        return map(lambda a,row=row: row.get_raw_data(a,''),S.CONVERT_VIEW_OPEN_FIELDS)
    
    def convert_other_row(self,row):
        return row.raw_data
    
    def filter_convert_row(self,row):
        return False
    
        
    