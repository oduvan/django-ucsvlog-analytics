from django.contrib import admin
from djucsvlog_analytics.models import ModelHistory


class ModelHistoryAdmin(admin.ModelAdmin):
    list_display = ('__unicode__', 'user', 'action', 'row_index',
                    'change_data')
    list_filter = ('user', 'content_type', 'object_id')
    readonly_fields = ('content_type', 'object_id', 'action', 'user',
                       'request_data', 'change_data', 'created_at', 'row_index')

    def has_add_permission(self, request):
        return False


admin.site.register(ModelHistory, ModelHistoryAdmin)