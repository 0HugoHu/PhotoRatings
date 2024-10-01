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
    job3 = scheduler.add_job(create_thumbnail, 'interval', seconds=SCHEDULER_INTERVAL_CREATE_THUMBNAILS) 
    job4 = scheduler.add_job(preprocess_image_size, 'interval', seconds=SCHEDULER_INTERVAL_PREPROCESSING_IMAGE)
    job1.func()
    job2.func()
    job3.func()
    job4.func()
    scheduler.start()


def move_images_to_unrated():
    with job_lock:
        start_time = time.time()
        log_operation("Starting to move images from raw to unrated.")

        with open(LOG_FILE, 'r') as log_file:
            log_data = json.load(log_file)

        for root, dirs, files in os.walk(RAW_FOLDER):
            for image_file in files:
                if image_file.lower().endswith(('.jpg', '.jpeg', '.png') ):
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


def create_thumbnail():
    start_time = time.time()
    log_operation("Starting to create thumbnails.")
    
    partition_folders = [f for f in os.listdir(UNRATED_FOLDER) 
                         if os.path.isdir(os.path.join(UNRATED_FOLDER, f)) 
                         and not THUMBNAIL in f]
    
    partition_folders = sorted(partition_folders, key=int, reverse=True)
    
    for partition in partition_folders:
        partition_folder = os.path.join(UNRATED_FOLDER, partition)
        thumbnail_folder = os.path.join(UNRATED_FOLDER, partition + THUMBNAIL)
        os.makedirs(thumbnail_folder, exist_ok=True)

        for file in os.listdir(partition_folder):
            if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                source_path = os.path.join(partition_folder, file)
                thumbnail_path = os.path.join(thumbnail_folder, file)
                
                if not os.path.isfile(thumbnail_path):
                    try:
                        with Image.open(source_path) as img:
                            img.thumbnail((IMAGE_THUMBNAIL_SIZE, IMAGE_THUMBNAIL_SIZE))
                            img.save(thumbnail_path)
                            log_operation(f"Created thumbnail for {file} in {thumbnail_path}")
                    except Exception as e:
                        log_operation(f"Failed to create thumbnail for {file}: {str(e)}")
    
    log_operation(f"Finished creating thumbnails. Time taken: {time.time() - start_time:.2f} seconds.")


def preprocess_image_size():
    start_time = time.time()
    log_operation("Starting to preprocess for desired photo size.")
    
    partition_folders = [f for f in os.listdir(RATED_FOLDER)]
    output_folder = PREPROCESS_FOLDER
    file_counter = 1

    with open(METADATA_FILE, 'w') as metadata_file:
        for partition in partition_folders:
            partition_folder = os.path.join(RATED_FOLDER, partition)
            target_score = SCORE_MAP.get(str(partition), 0.0) 

            for file in os.listdir(partition_folder):
                if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                    source_path = os.path.join(partition_folder, file)
                    
                    file_extension = os.path.splitext(file)[1].lower()
                    output_filename = f"{file_counter:05d}{file_extension}"
                    preprocessing_path = os.path.join(output_folder, output_filename)
                    
                    try:
                        with Image.open(source_path) as img:
                            img.thumbnail((IMAGE_PREPROCESS_SIZE, IMAGE_PREPROCESS_SIZE))
                            img.save(preprocessing_path)
                            
                            metadata = {
                                "file_name": output_filename,
                                "target": target_score
                            }
                            metadata_file.write(json.dumps(metadata) + "\n")
                            file_counter += 1
                            log_operation(f"Preprocessed image {source_path} to {preprocessing_path}, score: {target_score}")
                        
                    except Exception as e:
                        log_operation(f"Failed to create thumbnail for {file}: {str(e)}")
    
    log_operation(f"Finished resizing images. Time taken: {time.time() - start_time:.2f} seconds.")

