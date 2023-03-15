from __future__ import annotations

from collections import defaultdict
from rich.console import RenderableType
from rich.text import Text
from textual.message import Message
from textual.reactive import Reactive
from textual.widget import Widget
from typing import ClassVar, Optional

import rich.repr
import textual.events as events


@rich.repr.auto
class CustomFooter(Widget):
    """A simple footer widget which docks itself to the bottom of the parent container."""

    COMPONENT_CLASSES: ClassVar[set[str]] = {
        "footer--description",
        "footer--key",
        "footer--highlight",
        "footer--highlight-key",
    }
    """
    | Class | Description |
    | :- | :- |
    | `footer--description` | Targets the descriptions of the key bindings. |
    | `footer--highlight` | Targets the highlighted key binding. |
    | `footer--highlight-key` | Targets the key portion of the highlighted key binding. |
    | `footer--key` | Targets the key portions of the key bindings. |
    """

    DEFAULT_CSS = """
    CustomFooter {
        background: $accent;
        color: $text;
        dock: bottom;
        height: 1;
    }
    CustomFooter > .footer--highlight {
        background: $accent-darken-1;
    }

    CustomFooter > .footer--highlight-key {
        background: $secondary;
        text-style: bold;
    }

    CustomFooter > .footer--key {
        text-style: bold;
        background: $accent-darken-2;
    }
    """

    highlight_key: Reactive[str | None] = Reactive(None)
    daily_pnl: Reactive[float | None] = Reactive(None)
    unrealized_pnl: Reactive[float | None] = Reactive(None)
    realized_pnl: Reactive[float | None] = Reactive(None)

    class FooterMessage(Message, bubble=True):
        def __init__(
            self,
            daily_pnl: Optional[float],
            unrealized_pnl: Optional[float],
            realized_pnl: Optional[float],
        ) -> None:
            super().__init__()
            self.daily_pnl = daily_pnl
            self.unrealized_pnl = unrealized_pnl
            self.realized_pnl = realized_pnl

    def __init__(self) -> None:
        super().__init__()
        self._key_text: Text | None = None
        self.auto_links = False

    async def watch_highlight_key(self, value) -> None:
        """If highlight key changes we need to regenerate the text."""
        self._key_text = None
        self.refresh()

    async def watch_daily_pnl(self, value) -> None:
        self._key_text = None
        self.refresh()

    async def watch_unrealized_pnl(self, value) -> None:
        self._key_text = None
        self.refresh()

    async def watch_realized_pnl(self, value) -> None:
        self._key_text = None
        self.refresh()

    def update_daily_pnl(self, value: float) -> None:
        self.daily_pnl = value  # Text(value, style="bold")

    def update_unrealized_pnl(self, value: float) -> None:
        self.unrealized_pnl = value  # Text(value, style="bold")

    def update_realized_pnl(self, value: float) -> None:
        self.realized_pnl = value

    def on_mount(self) -> None:
        self.watch(self.screen, "focused", self._focus_changed)

    def _focus_changed(self, focused: Widget | None) -> None:
        self._key_text = None
        self.refresh()

    def on_custom_footer_footer_message(self, event: CustomFooter.FooterMessage) -> None:
        if event.daily_pnl is not None:
            self.update_daily_pnl(event.daily_pnl)
        if event.unrealized_pnl is not None:
            self.update_unrealized_pnl(event.unrealized_pnl)
        if event.realized_pnl is not None:
            self.update_realized_pnl(event.realized_pnl)

    async def on_mouse_move(self, event: events.MouseMove) -> None:
        """Store any key we are moving over."""
        self.highlight_key = event.style.meta.get("key")

    async def on_leave(self, event: events.Leave) -> None:
        """Clear any highlight when the mouse leaves the widget"""
        self.highlight_key = None

    def __rich_repr__(self) -> rich.repr.Result:
        yield from super().__rich_repr__()

    def make_pnl_text(self) -> Text:
        base_style = self.rich_style
        text = Text(
            style=self.rich_style,
            no_wrap=True,
            overflow="ellipsis",
            justify="left",
            end="",
        )
        if self.daily_pnl is not None:
            text.append_text(Text(f' Daily: ${self.daily_pnl:,.2f}'))
        else:
            text.append_text(Text('  Daily: $0.00', style=base_style))

        if self.unrealized_pnl is not None:
            text.append_text(Text(f' Unrlzd: ${self.unrealized_pnl:,.2f}'))
        else:
            text.append_text(Text(' Unrlzd: $0.00', style=base_style))

        if self.realized_pnl is not None:
            text.append_text(Text(f' Realized: ${self.realized_pnl:,.2f} '))
        else:
            text.append_text(Text(' Realized: $0.00 ', style=base_style))


        return text

    def make_key_text(self) -> Text:
        """Create text containing all the keys."""
        base_style = self.rich_style
        text = Text(
            style=self.rich_style,
            no_wrap=True,
            overflow="ellipsis",
            justify="left",
            end="",
        )
        highlight_style = self.get_component_rich_style("footer--highlight")
        highlight_key_style = self.get_component_rich_style("footer--highlight-key")
        key_style = self.get_component_rich_style("footer--key")

        bindings = [
            binding
            for (_namespace, binding) in self.app.namespace_bindings.values()
            if binding.show
        ]

        action_to_bindings = defaultdict(list)
        for binding in bindings:
            action_to_bindings[binding.action].append(binding)

        for action, bindings in action_to_bindings.items():
            binding = bindings[0]
            if binding.key_display is None:
                key_display = self.app.get_key_display(binding.key)
                if key_display is None:
                    key_display = binding.key.upper()
            else:
                key_display = binding.key_display
            hovered = self.highlight_key == binding.key
            key_text = Text.assemble(
                (f" {key_display} ", highlight_key_style if hovered else key_style),
                (
                    f" {binding.description} ",
                    highlight_style if hovered else base_style,
                ),
                meta={
                    "@click": f"app.check_bindings('{binding.key}')",
                    "key": binding.key,
                },
            )
            text.append_text(key_text)
        return text

    def notify_style_update(self) -> None:
        self._key_text = None

    def post_render(self, renderable):
        return renderable

    def render(self) -> RenderableType:
        if self._key_text is None:
            self._key_text = self.make_pnl_text()
            self._key_text.append(self.make_key_text())
        return self._key_text
