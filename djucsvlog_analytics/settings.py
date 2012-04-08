import os
import re
from django.conf import settings
from djucsvlog import settings as LS
from datetime import timedelta

set_prefix = 'UCSVLOG_A_' 
def get(key, default):
    globals()[key] = getattr(settings, set_prefix+key, default)
get('STREAM_FOLDERS_MATCHES',{os.path.dirname(LS.FILE):[re.compile('.*')]}) #keys is folder values is list of matches
get('STREAM_INDEX', 'stream_index.db')
get('STREAM_MATCH_D',['year','syear','month','day','hour','2_hour','3_hour','5_hour','minute','0month','0day','0hour'])
get('STREAM_CLEAR_EVERY', 10000)

import re
STREAM_FOLDERS_MATCHES_RE = {}
for folder, matches in STREAM_FOLDERS_MATCHES.items():
    re_matches = STREAM_FOLDERS_MATCHES_RE[folder] = []
    for item in matches:
        if not hasattr(item,'match'):
            for d_replace in STREAM_MATCH_D:
                item = item.replace('%('+d_replace+')s','\d+')
            item = re.compile('^'+item+'$')
            
        re_matches.append(item)

get('CONVERT_OLD_TIME',timedelta(days=30)) #convert only 2 weeks older files .
get('CONVERT_FILE','%(year)s-%(month)s.ucsv') #file for saving

get('CONVERT_REQUEST_FIELDS',['remote_addr','path','request_form_data']) # request logged fields
get('CONVERT_RESPONSE_FIELDS',['status']) #response logged fields
get('CONVERT_VIEW_OPEN_FIELDS',['userid'])  
get('CONVERT_INDEX','convert_index.db')