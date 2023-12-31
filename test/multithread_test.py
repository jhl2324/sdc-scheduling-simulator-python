import threading
import time
from background_task import background_task

shared_list = [0]
lock = threading.Lock()

background_thread = threading.Thread(target=background_task, args=(shared_list, lock))
background_thread.start()

for i in range(5):
    with lock:
        shared_list[0] += 1
        print(f"Main thread increment: {shared_list[0]}")

    time.sleep(0.5)

background_thread.join()

print(f"Final value in shared list: {shared_list[0]}")