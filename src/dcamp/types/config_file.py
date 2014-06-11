import logging
from configparser import ConfigParser, Error as ConfigParserError

from dcamp.types.specs import EndpntSpec, FilterSpec, GroupSpec, MetricSpec, ThreshSpec
from dcamp.util.decorators import prefixable
import dcamp.util.functions as util


class ParsingError(ConfigParserError):
    pass


@prefixable
class ConfigFileMixin(ConfigParser):
    def __init__(self):
        self.logger = logging.getLogger('dcamp.types.config')
        ConfigParser.__init__(self, allow_no_value=True, delimiters='=')

        self.isvalid = False
        self.__num_errors = 0
        self.__num_warns = 0

        self.global_cfg = {}
        self.metrics = {}
        self.groups = {}

        self.kvdict = {}

        self.metric_sections = {}
        self.group_sections = {}

    @staticmethod
    def validate(file):
        config = ConfigFileMixin()
        config.read_file(file)

    def read_file(self, f, source=None):
        ConfigParser.read_file(self, f, source)

        sections = list(self)
        sections.remove('DEFAULT')
        if 'global' in self:  # not validated yet
            sections.remove('global')

        # find all metric specifications
        self.metric_sections = {}
        for s in sections:
            section = dict(self[s])
            if 'rate' in section or 'metric' in section:
                self.metric_sections[s] = section

        # find all group specifications
        self.group_sections = {}
        for s in sections:
            if s not in self.metric_sections:
                self.group_sections[s] = dict(self[s])

        # the order of these calls matters

        self.isvalid = False
        self.__num_errors = 0
        self.__num_warns = 0

        self.__validate()

        if self.isvalid:
            self.__create_global()
            self.__create_metrics()
            self.__create_groups()

        if self.__num_errors > 0:
            raise ParsingError('%d parsing errors in dcamp config file; '
                               'see above error messages for details' %
                               self.__num_errors)

        self.__create_kvdict()

    def __create_global(self):
        assert self.isvalid

        result = {
            'heartbeat': util.str_to_seconds(self['global']['heartbeat'])
        }
        self.global_cfg = result

    def __create_metrics(self):
        assert self.isvalid

        result = {}

        # process all metric specifications
        for name in self.metric_sections:

            rate = util.str_to_seconds(self[name]['rate'])

            threshold = None
            if 'threshold' in self[name]:
                threshold = ThreshSpec.from_str(self[name]['threshold'])

                if threshold.is_timed:
                    if threshold.value < rate:
                        self.__eprint('time-based threshold shorter than sample rate: %s' % name)
                    elif threshold.value % rate != 0:
                        self.__eprint('time-based threshold indivisible by sample rate: %s' % name)

            detail = self[name]['metric']

            param = None
            if 'param' in self[name]:
                param = self[name]['param']

            aggr = None
            if 'aggregate' in self[name]:
                aggr = self[name]['aggregate']

            valid_aggr = (None, 'max', 'min', 'avg', 'sum')
            if aggr not in valid_aggr:
                self.__eprint('aggregation value "%s" not valid for "%s" metric; choose: %s' %
                              (aggr, name, valid_aggr))

            if aggr is not None and threshold is not None:
                self.__eprint('aggregation cannot be configured along with threshold: %s' % name)

            result[name] = MetricSpec(name, rate, threshold, detail, param, aggr)

        self.metrics = result

    def __create_groups(self):
        assert self.isvalid
        assert len(self.metrics) > 0

        result = {}

        # process all group specifications
        for name in self.group_sections:
            endpoints = []
            metrics = []
            filters = []

            for key in self[name]:
                if key in self.metrics:
                    # add metric spec
                    metrics.append(self.metrics[key])
                elif key.startswith(('+', '-')):
                    # create/add filter spec
                    filters.append(FilterSpec(key[0], key[1:]))
                else:
                    # create endpoint spec
                    endpoints.append(EndpntSpec.from_str(key))

            result[name] = GroupSpec(endpoints, filters, metrics)

        self.groups = result

    def __create_kvdict(self):
        assert self.isvalid
        assert len(self.global_cfg) > 0
        assert len(self.metrics) > 0

        result = {}

        self._push_prefix('CONFIG')

        # add global_cfg specs
        prefix = self._push_prefix('global')
        result[prefix + 'heartbeat'] = self.global_cfg['heartbeat']
        self._pop_prefix()

        for (group, spec) in self.groups.items():
            # add group name to prefix
            prefix = self._push_prefix(group)

            result[prefix + 'endpoints'] = spec.endpoints
            result[prefix + 'filters'] = spec.filters
            result[prefix + 'metrics'] = spec.metrics

            # remove name from prefix
            self._pop_prefix()

        # remove "config" prefix
        self._pop_prefix()

        # verify we popped as many times as we pushed
        assert len(self._get_prefix()) == 1

        self.kvdict = result

    #####
    # validation methods

    def __eprint(self, *objects):
        import sys

        self.__num_errors += 1
        print('Error:', *objects, file=sys.stderr)

    def __wprint(self, *objects):
        import sys

        self.__num_warns += 1
        print('Warning:', *objects, file=sys.stderr)

    def __validate(self):
        """
        @todo turn each of these checks into a test routine / issue #24
        """
        # { host : [ port ] }
        endpoints = {}

        # check global_cfg specification
        if 'global' not in self:
            self.__eprint("missing [global] section")
        else:
            if 'heartbeat' not in self['global']:
                self.__eprint("missing 'heartbeat' option in [global] section")

            if len(self['global']) > 1:
                self.__eprint("extraneous values in [global] section")

        # check for at least one group and one metric
        if len(self.metric_sections) < 1:
            self.__eprint("must specify at least one metric")
        if len(self.group_sections) < 1:
            self.__eprint("must specify at least one group")

        # find all used metric specs and validate group endpoint definitions
        used_metrics = set()
        for group in self.group_sections:
            nodecnt = 0
            for key in self[group]:
                if key in self.metric_sections:
                    # add metric spec name to used list and continue
                    used_metrics.add(key)
                    continue
                elif key.startswith(('+', '-')):
                    # skip filter specs
                    continue
                try:
                    nodecnt += 1
                    ep = EndpntSpec.from_str(key)
                    if ep.host in endpoints:
                        endpoints[ep.host].append(ep.port)
                    else:
                        endpoints[ep.host] = [ep.port]
                except ValueError:
                    self.__eprint('invalid endpoint or undefined metric in %s: "%s"' % (group, key))

            # verify group has at least one endpoint
            if nodecnt == 0:
                self.__eprint('[%s] section contains no nodes or subnets' % group)

        # ensure no overlapping ports
        for (host, ports) in endpoints.items():
            ports = sorted(ports)
            prev = ports[0]
            for p in ports[1:]:
                if (prev + EndpntSpec.MAX_OFFSET) > p:
                    self.__eprint('endpoint port overlap on host %s: %d and %d; must be %d or more apart' %
                                  (host, prev, p, EndpntSpec.MAX_OFFSET))
                prev = p

        # warn if some metric specs not used
        unused_metrics = set(self.metric_sections.keys()) - used_metrics
        if len(unused_metrics) > 0:
            self.__wprint('unused metric specification:', unused_metrics)

        if 0 == self.__num_errors:
            self.isvalid = True
