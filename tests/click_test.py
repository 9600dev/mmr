from click_repl import register_repl
from cloup import command, Context, group, HelpFormatter, HelpTheme, option, option_group
from cloup.constraints import RequireAtLeast
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.cursor_shapes import CursorShape
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import FileHistory

import click
import click_repl
import cloup
import os


global invoke_context

def invoke_context_wrapper(ctx):
    global invoke_context
    invoke_context = ctx


CONTEXT_SETTINGS = Context.settings(
    # parameters of Command:
    align_option_groups=False,
    align_sections=True,
    show_constraints=True,
    # parameters of HelpFormatter:
    formatter_settings=HelpFormatter.settings(
        max_width=120,
        col1_max_width=25,
        col2_min_width=30,
        indent_increment=3,
        col_spacing=3,
        theme=HelpTheme.light(),
    )
)


@cloup.group(
    'cli',
    invoke_without_command=True,
    context_settings=CONTEXT_SETTINGS,
)
@cloup.pass_context
def cli(ctx):
    click.echo('cli')

    if ctx.invoked_subcommand is None:
        ctx.invoke(invoke_context)

@cli.command('exit')
def def_exit():
    os._exit(os.EX_OK)

@cli.command()
@option_group(
    "Input options",
    option("--one", help="1st input option"),
    option("--two", help="2nd input option"),
    option("--three", help="3rd input option"),
)
@option_group(
    "Output options",
    "This is a an optional description of the option group.",
    option("--four / --no-four", help="1st output option"),
    option("--five", help="2nd output option"),
    option("--six", help="3rd output option"),
    constraint=RequireAtLeast(1),
)
# The following will be shown (with --help) under "Other options"
@option("--seven", help="1st uncategorized option")
@option("--height", help="2nd uncategorized option")
def hello(**kwargs):
    print(kwargs)

@cli.command()
def repl():
    global is_repl

    prompt_kwargs = {
        'history': FileHistory(os.path.expanduser('.trader.history')),
        'vi_mode': True,
        'message': 'mmr> ',
        'cursor': CursorShape.BLINKING_BLOCK,
        'auto_suggest': AutoSuggestFromHistory(),
    }

    is_repl = True
    click.echo(click.get_current_context().find_root().get_help())
    click.echo()
    click.echo('Ctrl-D or \'exit\' to exit')
    click_repl.repl(click.get_current_context(), prompt_kwargs=prompt_kwargs)

# repl()

if __name__ == '__main__':
    invoke_context_wrapper(repl)
    cli(prog_name='cli')


# cli()
# def cli(**kwargs):
#     """ A CLI that does nothing. """
#     print(kwargs)

