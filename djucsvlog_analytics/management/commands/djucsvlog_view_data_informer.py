from cStringIO import StringIO
from datetime import datetime, timedelta
from django.core.mail import mail_admins
from django.core.urlresolvers import resolve, Resolver404, reverse, NoReverseMatch
from djucsvlog_analytics.analytic_commands import BaseSimpleAnalyticCommand
from optparse import make_option
from djucsvlog_analytics.management.commands.djucsvlog_view_runs_informer import Command as ViewRunsCommand

import sys

data = {
    'total_files': 0,
    'total_records': 0,
    'unresolved': 0
}
view_row_list = []


class Command(ViewRunsCommand):
    option_list = ViewRunsCommand.option_list + (
        make_option(
            '--view-namespace',
            action='append',
            help="View's name"
        ),
    )

    def initial_options(self, options):
        self.view_namespace = (options['view_namespace'][0]
                               if options['view_namespace'] else None)
        try:
            self.view_path = reverse(self.view_namespace)
        except NoReverseMatch:
            raise NotImplementedError("Bad view's namespace")
        self.row_time_string = None

        return super(Command, self).initial_options(options)


    def collect_row(self, row):
        super(ViewRunsCommand, self).collect_row(row)
        data['total_records'] += 1

        if row.get_raw_data('path') == self.view_path:
            if row.is_a_log:
                self.row_time_string = row.index
            view_row_list.append(row.raw)
            return
        if self.row_time_string:
            if row.parent_index != self.row_time_string:
                return
            view_row_list.append(row.raw)

    def handle(self, *args, **options):
        super(ViewRunsCommand, self).handle(*args, **options)
        format_dtime = lambda dt: dt.strftime('%H:%M of %d.%m.%Y')
        data.update({
            'from_date': format_dtime(self.min_datetime),
            'to_date': format_dtime(self.max_datetime)
        })
        self.output('''
        ============ Analyse View Log Data ============
            Period: from %(from_date)s to %(to_date)s
            Total files: %(total_files)s
            Total records: %(total_records)s
            Unresolved urls: %(unresolved)s
            ''' % data
        )
        self.output('\t\tView namespace: %s'
                    % self.view_namespace)
        for row in view_row_list:
            self.output('\n%s:' % row)

        if self.mail_admins:
            subject = (["View_Data_Report"])
            mail_admins(subject, self.ostream.getvalue())
