from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from data_validation_check import validate_data
import time
import pandas as pd

class FileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.src_path.endswith('.csv'):
            print(f"New file detected: {event.src_path}")
            process_file(event.src_path)

def process_file(file_path):
    # Call validation and processing logic
    validate_data(pd.read_csv(file_path))
    print(f"Processing: {file_path}")

if __name__ == "__main__":
    path = "D:\job\Data_Pipeline\data"
    observer = Observer()
    observer.schedule(FileHandler(), path=path, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
