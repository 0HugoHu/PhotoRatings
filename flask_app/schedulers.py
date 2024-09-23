from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from config import *
from utils import *
import zipfile
import threading
import json

job_lock = threading.Lock()


def start_scheduler():
    scheduler = BackgroundScheduler()
    job1 = scheduler.add_job(move_images_to_unrated, 'interval', seconds=SCHEDULER_INTERVAL_MOVE_TO_UNRATED)
    job2 = scheduler.add_job(archive_log_file, 'interval', seconds=SCHEDULER_INTERVAL_CHECK_LOG_SIZE) 
    job1.func()
    job2.func()
    scheduler.start()


def move_images_to_unrated():
    with job_lock:
        start_time = time.time()
        log_operation("Starting to move images from raw to unrated.")

        with open(LOG_FILE, 'r') as log_file:
            log_data = json.load(log_file)

        for root, dirs, files in os.walk(RAW_FOLDER):
            for image_file in files:
                if image_file.lower().endswith(('.jpg', '.jpeg', '.png')):
                    source_path = os.path.join(root, image_file) 
                    image_id = get_image_identifier(source_path)

                    if image_id in log_data:
                        log_operation(f"Removing duplicate image: {image_file} from {root}")
                        os.remove(source_path)
                        continue

                    available_folder = get_available_folder(UNRATED_FOLDER)
                    dest_path = os.path.join(UNRATED_FOLDER, available_folder, image_file)
                    shutil.move(source_path, dest_path)
                    log_operation(f"Moved image: {image_file} from {root} to {dest_path}")

                    log_data[image_id] = {'status': 'unrated', 'path': dest_path}

        with open(LOG_FILE, 'w') as log_file:
            json.dump(log_data, log_file, indent=4)

        log_operation(f"Finished moving images. Time taken: {time.time() - start_time:.2f} seconds.")


def archive_log_file():
    """Archives the log file if it exceeds the size limit."""
    log_size = os.path.getsize(OPERATION_LOG_FILE)

    if log_size > MAX_LOG_FILE_SIZE_BYTES:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_name = os.path.join(LOG_ARCHIVE_FOLDER, f"log_{timestamp}.zip")

        with zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(OPERATION_LOG_FILE, os.path.basename(OPERATION_LOG_FILE))
        
        log_operation(f"Archived log file to {archive_name}")

        with open(OPERATION_LOG_FILE, 'w') as log_operation_file:
            log_operation_file.write("") 

        log_operation("Created a new log file after archiving.")
