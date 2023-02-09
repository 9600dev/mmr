from textual.app import ComposeResult
from textual.binding import Binding, Bindings
from textual.containers import Container, Horizontal
from textual.message import Message, MessageTarget
from textual.reactive import Reactive
from textual.widgets import Button, Static


class Dialog(Static):
    """Display a modal dialog"""

    _show_dialog = Reactive(False)
    # DEFAULT_CSS = """

    # /* The top level dialog (a Container) */

    # """

    def __init__(
        self,
        confirm_action=None,
        noconfirm_action=None,
        name=None,
        id=None,
        classes=None,
    ):
        self.confirm_action = confirm_action  # type: ignore
        self.noconfirm_action = noconfirm_action  # type: ignore
        # actual message will be set using set_message()
        self.message = ""

        # list to save and restore focus for modal dialogs
        self._focuslist = []
        self._focus_save = None
        self._bindings_stack = []

        # Allow the application to access actions in this namespace
        self.app._action_targets.add("dialog")

        super().__init__(name=name, id=id, classes=classes)

    class FocusMessage(Message):
        """Message to inform the app that Focus has been taken"""

        def __init__(self, sender: MessageTarget, focustaken=True) -> None:
            self.focustaken = focustaken
            super().__init__(sender)

    def compose(self) -> ComposeResult:
        yield Container(
            Static(self.message, id="dialog_question"),
            Horizontal(
                Button("Yes", variant="success", id="dialog_y"),
                Button("No", variant="error", id="dialog_n"),
                classes="dialog_buttons",
            ),
            id="dialog",
        )

    def watch__show_dialog(self, show_dialog: bool) -> None:
        """Called when _show_dialog is modified"""
        self.app.set_class(show_dialog, "-show-dialog")

    def set_message(self, message: str) -> None:
        """Update the dialgo message"""
        self.query_one("#dialog_question", Static).update(message)

    def show_dialog(self) -> None:
        self._override_bindings()
        self._override_focus()
        self._show_dialog = True

    def action_close_dialog(self) -> None:
        """Close the dialog and return bindings"""
        self._restore_bindings()
        self._restore_focus()
        self._show_dialog = False

    @property
    def confirm_action(self):
        return self._confirm_action

    @confirm_action.setter
    def confirm_action(self, value: str):
        self._confirm_action = value

    @property
    def noconfirm_action(self):
        return self._noconfirm_action

    @noconfirm_action.setter
    def noconfirm_action(self, value: str):
        self._noconfirm_action = value

    def _override_bindings(self):
        """Force bindings for the dialog"""
        self._bindings_stack.append(self.app._bindings)
        newbindings = [
            Binding(
                key="ctrl+c",
                action="quit",
                description="",
                show=False,
                key_display=None,
                priority=True,
            ),
            ("y", "dialog.run_confirm_binding('dialog_y')", "Yes"),
            ("n", "dialog.run_confirm_binding('dialog_n')", "No"),
        ]
        self.app._bindings = Bindings(newbindings)

    async def _action_run_confirm_binding(self, answer: str):
        """When someone presses a button, directly run the associated binding"""
        if answer == "dialog_y":
            await self.action(self._confirm_action)
        elif answer == "dialog_n":
            await self.action(self._noconfirm_action)
        else:
            raise ValueError

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        assert button_id is not None
        await self._action_run_confirm_binding(button_id)

    def _restore_bindings(self):
        if len(self._bindings_stack) > 0:
            self.app._bindings = self._bindings_stack.pop()

    def _override_focus(self):
        """remove focus for everything, force it to the dialog"""
        self._focus_save = self.app.focused
        for widget in self.app.screen.focus_chain:
            self._focuslist.append(widget)
            widget.can_focus = False
        self.can_focus = True
        self.focus()
        self.emit_no_wait(self.FocusMessage(self, focustaken=True))

    def _restore_focus(self):
        """restore focus to what it was before we stole it"""
        while len(self._focuslist) > 0:
            self._focuslist.pop().can_focus = True
        if self._focus_save is not None:
            self.app.set_focus(self._focus_save)
        self.emit_no_wait(self.FocusMessage(self, focustaken=False))
