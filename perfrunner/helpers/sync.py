import threading


class SyncHotWorkload:

    def __init__(self, current_hot_load_start, timer_elapse):
        self.timer = None
        self.current_hot_load_start = current_hot_load_start
        self.timer_elapse = timer_elapse

    def start_timer(self, ws):
        self.timer_elapse.value = 1
        if ws.working_set_move_time:
            self.timer = threading.Timer(ws.working_set_move_time, self.start_timer, args=[ws])
            self.timer.start()

    def stop_timer(self):
        if self.timer:
            self.timer.cancel()
            self.timer = None
