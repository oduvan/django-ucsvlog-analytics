import datetime
from django.utils.translation import ugettext_lazy as _
from django.db import models
from django.contrib.auth.models import User
from django.contrib.contenttypes import generic
from django.contrib.contenttypes.models import ContentType


class ModelHistory(models.Model):
    content_type = models.ForeignKey(ContentType)
    object_id = models.PositiveIntegerField(null=True, blank=True)
    content_object = generic.GenericForeignKey()
    action = models.CharField(max_length=20)
    user = models.ForeignKey(User, related_name='model_history', null=True,
                             blank=True)
    request_data = models.TextField(null=True, blank=True)
    change_data = models.TextField()
    created_at = models.DateTimeField(default=datetime.datetime.now)
    row_index = models.CharField(max_length=100)

    def __unicode__(self):
        return unicode(self.content_object)

    class Meta:
        verbose_name = _("Model history item")
        verbose_name_plural = _("Model history")