from optparse import make_option
import string
from djucsvlog_analytics.analytic_commands import (BaseAnalyticReadCommand,
                                                       BaseAnalyticElement)
from djucsvlog_analytics.uasparser import UASparser
uas_parser = UASparser('/tmp/')


class TopHostsEntryPointsElement(BaseAnalyticElement):
    display_name = 'Entry Points'

    def __init__(self, hosts=None, except_paths=None, output_limit=None,
                 after_path=None, country_ips=None, paths=None,
                 after_steps=None, log_goals=None, from_browsers_family=None,
                 from_os_family=None, after_time=None, exclude_paths=None):
        self.hosts = hosts
        self.except_paths = except_paths
        self.output_limit = output_limit
        self.after_path = after_path
        self.country_ips = country_ips
        self.paths = paths
        self.after_steps = after_steps
        self.log_goals = log_goals
        self.from_browsers_family = from_browsers_family
        self.from_os_family = from_os_family
        self.after_time = after_time
        self.count_steps = True
        self.exclude_paths = exclude_paths

    def in_except_path(self, path):
        if self.except_paths is None:
            return False

        return path in self.except_paths

    def req_status(self, status):
        return status == '200'

    def get_all_hosts(self):
        ret = []
        cur = self.c.cursor()
        cur.execute('select name from host')
        for row in cur:
            ret.append(row[0])
        return ret

    def analyse(self):
        if self.hosts is None:
            hosts = self.get_all_hosts()
        else:
            hosts = self.hosts
        ret = {}
        for host in hosts:
            ret[host] = self.a_host(host)
        return ret

    def a_host(self, host):
        ret = {}
        cur = self.c.cursor()
        cur.execute('select useruuid.*, host.name host_name from useruuid '
                    'join host on host.id=useruuid.host_id where host.name=? ',
                    [host])
        for uuid_row in cur:
            path = self.a_uuid(uuid_row)
            if path is None:
                continue

            if path in ret:
                ret[path] += 1
            else:
                ret[path] = 1
        return ret

    def get_goal(self, row, log_goals):
        cur_goals = self.c.cursor()
        gals_list = map(lambda a: 'request_log.d%s=?' % a,
                        range(1, len(log_goals) + 1))

        cur_goals.execute('select count(*) from request_log '
                          'join request on request.id = request_log.id '
                          'where request.useruuid_id=? and '
                          + ' and '.join(gals_list),
                          [row['id']] + log_goals)
        return int(cur_goals.fetchone()[0])

    def get_have_exclude_path(self, row):
        cur_req = self.c.cursor()
        rq_list = ['req_path=?' for i in range(len(self.exclude_paths))]
        cur_req.execute('select count(*) from request where useruuid_id=? and '
                        + ' or '.join(rq_list),
                        [row['id']] + self.exclude_paths)
        return int(cur_req.fetchone()[0])

    def a_uuid(self, row):
        if self.country_ips:
            if row['geoip_country_code'] not in self.country_ips:
                return

        if self.from_browsers_family or self.from_os_family:
            parsed_user_agent_data = uas_parser.parse(row['http_user_agent'])

            if self.from_browsers_family:
                if (parsed_user_agent_data['ua_family']
                    in self.from_browsers_family):
                    return

            if self.from_os_family:
                if (parsed_user_agent_data['os_family']
                    in self.from_os_family):
                    return

        if self.log_goals:
            if not self.get_goal(row, self.log_goals):
                return

        if self.exclude_paths:
            if self.get_have_exclude_path(row):
                return

        cur_req = self.c.cursor()
        cur_req.execute('select req_path, close_status, req_time from request '
                        'where useruuid_id=?',
                        [row['id']])

        after_path_finded = False
        after_steps = self.after_steps
        first_req_time = 0

        for req_row in cur_req:
            if self.after_time and not first_req_time:
                first_req_time = req_row['req_time']

            if self.after_time:
                delta_time = req_row['req_time'] - first_req_time
                if int(self.after_time) > delta_time:
                    continue

            if not self.req_status(req_row['close_status']):
                continue

            if self.in_except_path(req_row['req_path']):
                continue

            if self.after_path:
                if req_row['req_path'] == self.after_path:
                    after_path_finded = True
                    continue

                if not after_path_finded:
                    continue

            if self.paths and req_row['req_path'] not in self.paths:
                continue

            if after_steps:
                after_steps -= 1
                continue

            return self.a_uuid_collect_data(req_row, row)

    def a_uuid_collect_data(self, req_row, uuid_row):
        return req_row['req_path']

    def zip_analyse_results(self, res):
        ret = {}

        for item in res:
            for host, paths in item.items():
                ret_paths = ret.setdefault(host, {})
                for path, hits in paths.items():
                    if path in ret_paths:
                        ret_paths[path] += hits
                    else:
                        ret_paths[path] = hits

        return ret

    def out_filter_paths(self, paths):
        sorted_paths = sorted(paths.items(), key=lambda a: a[1], reverse=True)
        return sorted_paths[:self.output_limit] if self.output_limit else sorted_paths

    def out_total_path_hits(self, paths):
        return sum(paths.values())

    def output_results(self):
        print '\n=====' + self.display_name + '====='
        for domain, paths in self.result.items():
            print '%s (%s)' % (domain.upper(), self.out_total_path_hits(paths))
            for path_data in self.out_filter_paths(paths):
                self.out_path_data(path_data)

    def out_path_data(self, path_data):
        path, hits = path_data
        print str(path).rjust(30), str(hits).rjust(30)


class TopHostsCountryIpElement(TopHostsEntryPointsElement):
    display_name = 'Country IP'

    def __init__(self, *args, **kwargs):
        self.geoip_db = kwargs.pop('geoip_db', None)
        super(TopHostsCountryIpElement, self).__init__(*args, **kwargs)

    def a_uuid_collect_data(self, req_row, uuid_row):
        return uuid_row['geoip_country_code']

    def out_path_data(self, path_data):
        path, hits = path_data
        if self.geoip_db:
            path = self.geoip_db.get_name_by_code(path)
        print str(path).rjust(30), str(hits).rjust(30)


class TopHostsFirstAcceptedLanguageElement(TopHostsEntryPointsElement):
    display_name = 'Accepted Language'

    def a_uuid_collect_data(self, req_row, uuid_row):
        split_lang_row = lambda s: s.split(',')[0].split('-')[0].split(';')[0]
        lang = split_lang_row(uuid_row['req_http_accept_language']).strip()
        if not lang:
            # do not count empty accept language
            return None
        return lang.lower()


class TopHostsOSElement(TopHostsEntryPointsElement):
    display_name = 'OS'

    def a_uuid_collect_data(self, req_row, uuid_row):
        result = uas_parser.parse(uuid_row['http_user_agent'])
        return result['os_name']


class TopHostsOSFamilyElement(TopHostsEntryPointsElement):
    display_name = 'OS Family'

    def a_uuid_collect_data(self, req_row, uuid_row):
        result = uas_parser.parse(uuid_row['http_user_agent'])
        return result['os_family']


class TopHostsBrowsersElement(TopHostsEntryPointsElement):
    display_name = 'Browser'

    def a_uuid_collect_data(self, req_row, uuid_row):
        result = uas_parser.parse(uuid_row['http_user_agent'])
        return result['ua_name']


class TopHostsBrowsersFamilyElement(TopHostsEntryPointsElement):
    display_name = 'Browser Family'

    def a_uuid_collect_data(self, req_row, uuid_row):
        result = uas_parser.parse(uuid_row['http_user_agent'])
        return result['ua_family']


class TopHostsExitPointsElement(TopHostsEntryPointsElement):
    display_name = 'Exit Point'

    def a_uuid_collect_data(self, req_row, uuid_row):
        cur_req = self.c.cursor()
        cur_req.execute('select req_path, close_status from request '
                        'where useruuid_id=? order by req_index desc',
                        [uuid_row['id']])
        for req_row in cur_req:
            if req_row['close_status'] != '200':
                continue

            if self.in_except_path(req_row['req_path']):
                continue

            return req_row['req_path']


class TopHostsMoreRequestElement(TopHostsEntryPointsElement):
    display_name = "UUID's After Steps"

    def a_uuid_collect_data(self, req_row, uuid_row):
        return uuid_row['id']


class TopHostsAfterTimeElement(TopHostsEntryPointsElement):
    display_name = "UUID's After Time"

    def a_uuid(self, row):
        if self.get_goal(row, ["OrderSave"]):
            return
        return super(TopHostsAfterTimeElement, self).a_uuid(row)

    def a_uuid_collect_data(self, req_row, uuid_row):
        return uuid_row['id']


class TopHostsResponseNotFoundElement(TopHostsEntryPointsElement):
    display_name = 'Top 404'

    def req_status(self, status):
        return status == '404'


class TopHostsCountStepsElement(TopHostsEntryPointsElement):
    display_name = 'Count Steps'

    def a_uuid_collect_data(self, req_row, uuid_row):
        cur_req = self.c.cursor()
        cur_req.execute('select count(*) from request where useruuid_id=?',
        [uuid_row['id']])
        count_steps = cur_req.fetchone()[0]
        return count_steps


class Command(BaseAnalyticReadCommand):
    option_list = BaseAnalyticReadCommand.option_list + (
        make_option('--get-entry-points',
            dest='get_entry_points',
            default=None,
            action='store_true',
            help='Top Entry Points'),

        make_option('--get-os-family',
            dest='get_os_family',
            default=None,
            action='store_true',
            help='Top Entry Points'),

        make_option('--get-os',
            dest='get_os',
            default=None,
            action='store_true',
            help='Top Entry Points'),

        make_option('--get-browsers',
            dest='get_browsers',
            default=None,
            action='store_true',
            help='Top Entry Points'),

        make_option('--get-browsers-family',
            dest='get_browsers_family',
            default=None,
            action='store_true',
            help='Top Entry Points'),

        make_option('--get-exit-points',
            dest='get_exit_points',
            default=None,
            action='store_true',
            help='Top Exit Points'),

        make_option('--get-country-ip',
            dest='get_country_ip',
            default=None,
            action='store_true',
            help='Top Entry Points'),

        make_option('--get-first-accept-language',
            dest='get_first_accept_language',
            default=None,
            action='store_true',
            help='Top Entry Points'),

        make_option('--except-paths',
            dest='except_paths',
            default=None,
            help='Paths that no needs to count'),

        make_option('--top-hosts',
            dest='top_hosts',
            default=None,
            help='Paths that no needs to count'),

        make_option('--after-path',
            dest='after_path',
            default=None,
            help='Paths that no needs to count'),

        make_option('--country-ips',
            dest='country_ips',
            default=None,
            help='List of countries'),

        make_option('--paths',
            dest='paths',
            default=None,
            help='List of countries'),

        make_option('--after-steps',
            dest='after_steps',
            default=None,
            help='count a real steps which need to do'),

        make_option('--log-goals',
            dest='log_goals',
            default=None,
            help='count a real steps which need to do'),

        make_option('--top-404',
            dest='top_404',
            default=None,
            action='store_true',
            help='Top paths with 404'),

        make_option('--from-countries',
            dest='from_countries',
            default=None,
            help='Only from selected countries'),

        make_option('--from-os-family',
            dest='from_os_family',
            default=None,
            help='Top Entry Points'),

        make_option('--from-browsers-family',
            dest='from_browsers_family',
            default=None,
            help='Top Entry Points'),

        make_option('--after-time',
            dest='after_time',
            default=None,
            help='Top Entry Points'),

        make_option('--exclude-paths',
            dest='exclude_paths',
            default=None,
            help='Top Entry Points'),

        make_option('--steps-count',
            dest='steps_count',
            default=None,
            action='store_true',
            help='Top Entry Points'),
    )

    def initial_options(self, options):
        super(Command, self).initial_options(options)
        self.top_hosts = (None if options['top_hosts'] is None
                                    or options['top_hosts'].lower() == 'all'
                               else options['top_hosts'].split(','))

        if options['except_paths']:
            if isinstance(options['except_paths'], (list, tuple)):
                self.except_paths = list(options['except_paths'])
            self.except_paths = options['except_paths'].split(',')
        else:
            self.except_paths = None

        self.after_path = options['after_path']

        self.country_ips = (options['country_ips'].upper().split(',')
                            if options['country_ips'] else None)

        self.paths = options['paths'].split(',') if options['paths'] else None

        self.after_steps = (int(options['after_steps'])
                            if options['after_steps'] else 0)

        self.log_goals = (map(string.strip, options['log_goals'].split(','))
                          if options['log_goals'] else None)

        self.after_time = (options['after_time']
                           if options['after_time'] else 0)

        split_country_string = lambda s: s.replace('_', ' ').split(',')

        from_countries = (split_country_string(options['from_countries'])
                          if self.geoip_db and options['from_countries']
                          else None)

        if from_countries:
            country_code_list = ([self.geoip_db.get_code_by_name(name)
                                  for name in from_countries])
            self.country_ips = (country_code_list.extend(self.country_ips)
                                if self.country_ips else country_code_list)

        self.from_os_family = options['from_os_family']

        self.from_browsers_family = options['from_browsers_family']

        self.exclude_paths = (options['exclude_paths'].split(',')
                              if options['exclude_paths'] else None)

        global_params = dict(except_paths=self.except_paths,
                             output_limit=self.output_limit,
                             after_path=self.after_path,
                             country_ips=self.country_ips,
                             paths=self.paths,
                             after_steps=self.after_steps,
                             log_goals=self.log_goals,
                             from_os_family=self.from_os_family,
                             from_browsers_family=self.from_browsers_family,
                             after_time=self.after_time,
                             exclude_paths=self.exclude_paths)

        if options['get_entry_points']:
            self.add_analyse_element(TopHostsEntryPointsElement(
                                        self.top_hosts, **global_params))

        if options['get_country_ip']:
            self.add_analyse_element(TopHostsCountryIpElement(
                hosts=self.top_hosts,
                geoip_db=self.geoip_db,
                **global_params)
            )

        if options['get_first_accept_language']:
            self.add_analyse_element(TopHostsFirstAcceptedLanguageElement(
                                        self.top_hosts, **global_params))

        if options['get_exit_points']:
            self.add_analyse_element(TopHostsExitPointsElement(
                                        self.top_hosts, **global_params))

        if options['get_os']:
            self.add_analyse_element(TopHostsOSElement(
                                        self.top_hosts, **global_params))

        if options['get_browsers']:
            self.add_analyse_element(TopHostsBrowsersElement(
                                        self.top_hosts, **global_params))

        if options['get_os_family']:
            self.add_analyse_element(TopHostsOSFamilyElement(
                                        self.top_hosts, **global_params))

        if options['get_browsers_family']:
            self.add_analyse_element(TopHostsBrowsersFamilyElement(
                                        self.top_hosts, **global_params))

        if options['top_404']:
            self.add_analyse_element(TopHostsResponseNotFoundElement(
                                        self.top_hosts, **global_params))

        if self.after_steps:
            self.add_analyse_element(TopHostsMoreRequestElement(
                                        self.top_hosts, **global_params))

        if self.after_time:
            self.add_analyse_element(TopHostsAfterTimeElement(
                self.top_hosts, **global_params))


        if options['steps_count']:
            self.add_analyse_element(TopHostsCountStepsElement(
                                            self.top_hosts, **global_params))
