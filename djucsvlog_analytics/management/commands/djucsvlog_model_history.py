import json
import sys
import operator
from django.contrib.auth.models import User
from djucsvlog_analytics.analytic_commands import BaseStreamCommand
from djucsvlog import settings as djucsvlog_settings
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from djucsvlog_analytics.models import ModelHistory


class Command(BaseStreamCommand):

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        self.request_data = {}
        # for binding "in requests" with "a_log" requests
        self.in_indices = {}

    def filter_row(self, row):
        if row.is_a_req:
            self.request_data[row.index] = {
                'request_data': row.raw_data,
                'request_id': row.data_browser_uuid
            }
            return False
        if row.is_a_in:
            try:
                user = User.objects.get(pk=row.data_userid)
            except User.DoesNotExist:
                user = None
            self.in_indices[row.index] = row.parent_index
            self.request_data[row.parent_index].update({
                'user': user
            })
            return False
        if djucsvlog_settings.CHANGE_MODEL_LOG_START != row.raw_data[0]:
            return False
        return self.filter_model(row)

    def __get_model_name(self, row):
        return row.raw_data[2]

    def __get_action_name(self, row):
        return row.raw_data[1]

    def __get_change_data(self, row):
        return row.raw_data[3:]

    def get_model_fields(self, row):
        model_name = self.__get_model_name(row)
        return [model['props'] for model in settings.UCSVLOG_CHANGE_MODEL
                 if model['model'] == model_name][0]

    def filter_model(self, row):
        model_name = self.__get_model_name(row)
        return model_name in map(operator.itemgetter('model'),
                                 settings.UCSVLOG_CHANGE_MODEL)

    def get_model(self, row):
        model_name = self.__get_model_name(row)
        app_name, model_name = model_name.split('.')
        models_module_name = '.'.join((app_name, 'models'))
        models_module = sys.modules[models_module_name]
        return getattr(models_module, model_name)

    def collect_row(self, row):
        model = self.get_model(row)
        content_type = ContentType.objects.get_for_model(model)
        change_data = self.__get_change_data(row)
        change_fields = self.get_model_fields(row)
        if change_fields[0] in ('id', 'pk'):
            object_id = change_data[0]
        else:
            object_id = None
        change_data = dict(zip(change_fields, change_data))
        if row.parent_index:
            all_request_data = self.request_data[self.in_indices[row.parent_index]]
            request_data = json.dumps(all_request_data['request_data'])
            user = all_request_data['user']
        else:
            request_data = None
            user = None
        ModelHistory.objects.create(
            content_type=content_type,
            object_id=object_id,
            request_data=request_data,
            user=user,
            action=self.__get_action_name(row),
            change_data=json.dumps(change_data),
            row_index=row.index
        )