#!/usr/bin/env python3
import logging
from argparse import ArgumentParser, ArgumentTypeError, FileType

from dcamp.app import App
from dcamp.types.config_file import ConfigFileMixin, ParsingError
from dcamp.types.specs import EndpntSpec


def address(string):
    try:
        return EndpntSpec.from_str(string)
    except ParsingError as e:
        raise ArgumentTypeError(e)


def main():
    # setup CLI parser and parse arguments
    parser = ArgumentParser(prog='dcamp', description='the %(prog)s cli')
    parser.add_argument('--version', action='version', version='%(prog)s 0.1', help='show %(prog)s version and exit')

    # logging output arguments
    parser.add_argument("-v", "--verbose",
                        dest="verbose",
                        help="make %(prog)s verbose",
                        action="store_true")
    parser.add_argument("-d", "--debug",
                        dest="debug",
                        help="make all %(prog)s modules uber verbose",
                        action="store_true")
    parser.add_argument("--debug-module",
                        dest="debug_mods",
                        help="make given %(prog)s module uber verbose",
                        action="append")
    parser.set_defaults(verbose=False, debug=False, debug_mods=[])

    # configuration file
    subparsers = parser.add_subparsers(title='dcamp commands', dest='command')

    # root command
    parser_root = subparsers.add_parser('root', help='run root command')
    parser_root.add_argument("-f", "--file", dest="configfile",
                             help="use FILE as configuration",
                             type=FileType(),
                             metavar="FILE",
                             required=True)
    parser_root.add_argument('--start', dest='action', action='store_const', const='start')
    parser_root.add_argument('--stop', dest='action', action='store_const', const='stop')
    parser_root.add_argument('-a', '--address', dest='address', type=address, required=True)
    parser_root.set_defaults(func=do_app, cmd='root', action='start')

    # base command
    parser_base = subparsers.add_parser('base', parents=[],
                                        help='run base command')
    parser_base.add_argument('-a', '--address', dest='address', type=address, required=True)
    parser_base.set_defaults(func=do_app, cmd='base')

    # config command
    parser_config = subparsers.add_parser('config',  help='run actions on the given %(prog)s config file')
    parser_config.add_argument("-f", "--file", dest="configfile",
                               help="use FILE as configuration",
                               type=FileType(),
                               metavar="FILE",
                               required=True)
    config_actions = parser_config.add_mutually_exclusive_group(required=True)
    config_actions.add_argument('--validate', dest='validate', action='store_true')
    config_actions.add_argument('--print', dest='print', action='store_true')

    parser_config.set_defaults(func=do_config)

    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(name)-27s %(levelname)-8s %(message)s')
    logger = logging.getLogger('dcamp')
    if args.verbose:
        logger.setLevel(logging.INFO)
        logger.debug('set logging level to verbose')
    if args.debug:  # not an elif--always set debug level if given
        logger.setLevel(logging.DEBUG)
        logger.debug('set logging level to debug')
    for mod in args.debug_mods:
        mod_logger = logging.getLogger(mod)
        mod_logger.setLevel(logging.DEBUG)
        mod_logger.debug('set "%s" logging level to debug' % mod)

    if args.command is None:
        parser.print_usage()
        exit(1)

    exit(args.func(args))


def do_app(args):
    dapp = App(args)
    return dapp.exec()


def do_config(args):
    config = ConfigFileMixin()

    if args.validate:
        try:
            config.validate(args.configfile)
        except ParsingError as e:
            print('\n' + e.message + '\n')
            return -1
        return 0
    elif args.print:
        config.read_file(args.configfile)
        for (k, v) in sorted(config.kvdict.items()):
            line = k + ' = '
            if type(v) == list:
                for i in v:
                    line += str(i) + ', '
                if len(v) == 0:
                    print(line + 'None')
                else:
                    print(line[:-2])
            else:
                print(line + str(v))
        return 0
    else:
        raise RuntimeError('unknown config action given')


if __name__ == '__main__':
    main()
