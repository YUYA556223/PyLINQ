import asyncio
import sys
import threading
import time
from typing import Union


class Loader(object):
    def __init__(self):
        self.format: str = "[{SYMBOL}] {STATUS}"
        self.is_loading: bool = False
        self.delay: float = 0.1
        # self.symbols: list[str] = ['⠁', '⠃', '⠇', '⠧', '⠷', '⠿', '⠾', '⠾']
        self.symbols: list[str] = ['⠁', '⠂', '⠄', '⠠', '⠐', '⠈']
        self.finish_symbol = '\033[32m✓\033[0m'
        self.fail_symbol = '\033[31mx\033[0m'
        self.current_status = None
        self.current_status_output = None

    @staticmethod
    def FormatStatus(format: str, symbol: str, status: str, padding: int = 0, padding_char: str = ' '):
        return format.replace('{SYMBOL}', symbol).replace('{STATUS}', status).ljust(padding, padding_char)

    @staticmethod
    def HideCursor():
        sys.stdout.write('\x1b[?25l')

    @staticmethod
    def ShowCursor():
        sys.stdout.write('\x1b[?25h')

    def __show_loader(self):
        while self.is_loading:
            for s in self.symbols:
                self.current_status_output = '\r' + \
                    Loader.FormatStatus(self.format, s, self.current_status)

                sys.stdout.write(self.current_status_output)
                sys.stdout.flush()

                time.sleep(self.delay)

    def start_loading(self, status: str, hide_cursor=True):
        self.is_loading = True
        self.current_status = status
        self.current_status_output = '\r' + \
            Loader.FormatStatus(
                self.format, self.symbols[0], self.current_status)

        if hide_cursor:
            Loader.HideCursor()

        threading.Thread(target=self.__show_loader).start()

    def update_status(self, new_status: str):
        self.current_status = new_status

    def update_progress(self, message: str):
        sys.stdout.write(
            '\r' + message.ljust(len(str(self.current_status_output)), ' ') + '\n')
        sys.stdout.flush()

    def stop_loading(self, failed: bool = False, status: Union[str, None] = None):
        self.is_loading = False

        time.sleep(self.delay * len(self.symbols))

        sys.stdout.write('\r' + Loader.FormatStatus(self.format, (self.finish_symbol if not failed else self.fail_symbol),
                         (status if status is not None else self.current_status), padding=len(str(self.current_status_output))) + '\n')
        Loader.ShowCursor()
        sys.stdout.flush()


class LoaderAsync(Loader):
    def __init__(self):
        super().__init__()

    async def start_loading_async(self, status: str, hide_cursor=True):
        self.is_loading = True
        self.current_status = status

        if hide_cursor:
            Loader.HideCursor()

        while self.is_loading:
            for s in self.symbols:
                self.current_status_output = '\r' + \
                    Loader.FormatStatus(self.format, s, self.current_status)

                sys.stdout.write(self.current_status_output)
                sys.stdout.flush()

                await asyncio.sleep(self.delay)

    async def update_status_async(self, new_status: str):
        super().update_status(new_status)
        await asyncio.sleep(self.delay)

    async def update_progress_async(self, message: str):
        super().update_progress(message)
        await asyncio.sleep(self.delay)

    async def stop_loading_async(self, failed: bool = False, status: str = None):
        self.is_loading = False

        sys.stdout.write('\r' + Loader.FormatStatus(self.format, (self.finish_symbol if not failed else self.fail_symbol),
                         (status if status is not None else self.current_status), padding=len(self.current_status_output)) + '\n')
        Loader.ShowCursor()
        sys.stdout.flush()

        await asyncio.sleep(self.delay)

    def start_loading_asyn(self, status: str):
        asyncio.create_task(self.start_loading_async(status))

    def update_status(self, new_status: str):
        asyncio.create_task(self.update_status_async(new_status))

    def update_progress(self, message: str):
        asyncio.create_task(self.update_progress_async(message))

    def stop_loading(self, failed: bool = False, status: str = None):
        asyncio.create_task(self.stop_loading_async(
            failed=failed, status=status))

    def abort(self, status: str = None):
        asyncio.run(self.stop_loading_async(failed=True, status=status))
