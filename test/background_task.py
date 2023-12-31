import time

def background_task(shared_list, lock):
    for i in range(5):
        with lock:
            shared_list[0] += 1
            print(f"Background task increment: {shared_list[0]}")

        time.sleep(1)