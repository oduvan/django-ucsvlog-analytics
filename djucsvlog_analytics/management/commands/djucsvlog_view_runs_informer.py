from cStringIO import StringIO
from datetime import datetime, timedelta
from django.core.mail import mail_admins
from django.core.urlresolvers import resolve, Resolver404
from djucsvlog_analytics.analytic_commands import BaseSimpleAnalyticCommand
from optparse import make_option

import sys

data = {
    'total_files': 0,
    'total_records': 0,
    'unresolved': 0
}
opened = {}
views_stats = {}


class Command(BaseSimpleAnalyticCommand):
    option_list = BaseSimpleAnalyticCommand.option_list + (
        make_option(
            '--mail-admins',
            action='store_true',
            default=False,
            help="Mail admins"
        ),
    )

    def initial_options(self, options):
        self.mail_admins = options.get('mail_admins')
        if self.mail_admins:
            self.ostream = StringIO()
        else:
            self.ostream = sys.stdout
        return super(Command, self).initial_options(options)

    def output(self, s=None):
        print >> self.ostream, "%s" % s

    def handle_file(self, file_name):
        data['total_files'] += 1
        return super(Command, self).handle_file(file_name)

    def analyse(self, request_row, result_row):
        cur_path = request_row.get_raw_data('path')
        try:
            view_resolve = resolve(cur_path)
        except Resolver404:
            data['unresolved'] += 1
            return

        view_function = view_resolve[0]
        view_namespace = view_resolve.view_name
        view_function_name = getattr(view_function, '__name__',
            view_function.__class__.__name__)
        view_function_module = view_function.__module__

        key_view = '.'.join([view_function_module, view_function_name])

        if key_view not in views_stats:
            views_stats[key_view] = dict(load_time=timedelta(),
                                         load_count=0,
                                         namespace=view_namespace)

        convert_datetime_str = lambda s: (
            datetime.strptime(s.split(';')[0], '%Y-%m-%dT%H:%M:%S.%f')
        )
        load_time = (convert_datetime_str(result_row.index)
                     - convert_datetime_str(result_row.parent_index))
        views_stats[key_view]['load_time'] += load_time
        views_stats[key_view]['load_count'] += 1

    def collect_row(self, row):
        super(Command, self).collect_row(row)

        data['total_records'] += 1

        if row.is_a_log:
            opened[row.index] = row
            return

        if not row.is_c_log:
            return

        request_row_index = row.parent_index
        if request_row_index in opened:
            self.analyse(opened[request_row_index], row)
            del opened[request_row_index]
        return

    def handle(self, *args, **options):
        super(Command, self).handle(*args, **options)
        format_dtime = lambda dt: dt.strftime('%H:%M of %d.%m.%Y')
        data.update({
            'from_date': format_dtime(self.min_datetime),
            'to_date': format_dtime(self.max_datetime)
        })
        self.output('''
        ============ Analyse View Runs ============
            Period: from %(from_date)s to %(to_date)s
            Total files: %(total_files)s
            Total records: %(total_records)s
            Unresolved urls: %(unresolved)s
            ''' % data
        )
        self.output('\t\t================== Views ==================')
        for key, info_dict in views_stats.items():
            self.output(
                '\n%s (%s):\n\tload time: %s \tload count: %s\taverage load time: %s' % (
                    key,
                    info_dict['namespace'],
                    info_dict['load_time'],
                    info_dict['load_count'],
                    info_dict['load_time'] / info_dict['load_count']
                )
            )
        if self.mail_admins:
            subject = (["View_Runs_Report"])
            mail_admins(subject, self.ostream.getvalue())
