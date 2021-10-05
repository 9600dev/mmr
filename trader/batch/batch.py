import click
import os
import sys

from click_help_colors import HelpColorsGroup, HelpColorsCommand

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from trader.common.command_line import common_options, default_config_provider, configuration_option

@click.command()
@common_options()
@configuration_option(provider=default_config_provider)
def main(
    ib_server_address,
    testing,
    **args
):
    print(ib_server_address)
    print(testing)


if __name__ == '__main__':
    main()
