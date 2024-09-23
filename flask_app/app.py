import logging
from flask import Flask, jsonify, request, send_file
import zipfile
import os
import shutil
import json
import time
import threading
from PIL import Image
from io import BytesIO
from apscheduler.schedulers.background import BackgroundScheduler
from waitress import serve
from datetime import datetime

app = Flask(__name__)

job_lock = threading.Lock()

# Folder paths
BASE_DIR = os.getcwd()
RAW_FOLDER = os.path.join(BASE_DIR, 'images_raw')
UNRATED_FOLDER = os.path.join(BASE_DIR, 'images_unrated')
RATED_FOLDER = os.path.join(BASE_DIR, 'images_rated')
DEBUG_FOLDER = os.path.join(BASE_DIR, 'images_debug')
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'images_log.json')
OPERATION_LOG_FILE = os.path.join(BASE_DIR, 'logs', 'operations.log')
LOG_ARCHIVE_FOLDER = os.path.join(BASE_DIR, 'logs_archive')

# Constants
PARTITION_SIZE = 100  # Number of images per partition
SCHEDULER_INTERVAL_MOVE_TO_UNRATED = 60 * 60  # Time interval for the scheduler (seconds)
SCHEDULER_INTERVAL_CHECK_LOG_SIZE = 60 * 30 # Time interval for the scheduler (seconds)
MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024  # in bytes
MAX_LOG_FILE_SIZE_BYTES = 20 * 1024 * 1024  # 20MB

# Configure logging
logging.basicConfig(
    filename=OPERATION_LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'w') as log_file:
        json.dump({}, log_file)

if not os.path.exists(LOG_ARCHIVE_FOLDER):
    os.makedirs(LOG_ARCHIVE_FOLDER, exist_ok=True)


def log_operation(message):
    """Log an operation message."""
    logging.info(message)


def compress_image(image_path):
    """Compress the image by resizing it while preserving the original format, to be under a certain size limit (in MB)."""
    start_time = time.time()
    log_operation(f"Compressing image: {image_path}")
    
    with Image.open(image_path) as img:
        img_format = img.format
        img_bytes = BytesIO()
        original_size = img.size
        target_size = MAX_FILE_SIZE_BYTES

        if img_format in ['JPEG', 'JPG']:
            min_quality = 50  
            max_quality = 100 
            
            for scale in [0.8, 0.6, 0.2]:
                for quality in range(max_quality, min_quality - 1, -10):
                    new_size = (int(original_size[0] * scale), int(original_size[1] * scale))
                    img_resized = img.resize(new_size, Image.LANCZOS)
                    
                    img_bytes = BytesIO()
                    img_resized.save(img_bytes, format=img_format, quality=quality)
                    img_bytes.seek(0)

                    if img_bytes.getbuffer().nbytes <= target_size:
                        log_operation(f"Compressed image: {image_path} to size: {img_bytes.getbuffer().nbytes / (1024 * 1024):.2f} MB with quality {quality} and scale {scale}. Time taken: {time.time() - start_time:.2f} seconds.")
                        return img_bytes

        elif img_format == 'PNG':
            for scale in [0.8, 0.6, 0.2]:
                new_size = (int(original_size[0] * scale), int(original_size[1] * scale))
                img_resized = img.resize(new_size, Image.LANCZOS)

                img_bytes = BytesIO()
                img_resized.save(img_bytes, format=img_format, optimize=True)
                img_bytes.seek(0)

                if img_bytes.getbuffer().nbytes <= target_size:
                    log_operation(f"Compressed PNG image: {image_path} to size: {img_bytes.getbuffer().nbytes / (1024 * 1024):.2f} MB with scale {scale}. Time taken: {time.time() - start_time:.2f} seconds.")
                    return img_bytes
        
        else:
            for scale in [0.8, 0.6, 0.2]:
                new_size = (int(original_size[0] * scale), int(original_size[1] * scale))
                img_resized = img.resize(new_size, Image.LANCZOS)

                img_bytes = BytesIO()
                img_resized.save(img_bytes, format=img_format)
                img_bytes.seek(0)

                if img_bytes.getbuffer().nbytes <= target_size:
                    log_operation(f"Compressed image ({img_format}): {image_path} to size: {img_bytes.getbuffer().nbytes / (1024 * 1024):.2f} MB with scale {scale}. Time taken: {time.time() - start_time:.2f} seconds.")
                    return img_bytes

        os.makedirs(DEBUG_FOLDER, exist_ok=True)
        destination_path = os.path.join(DEBUG_FOLDER, image_path.split('\\')[-1])
        shutil.move(image_path, destination_path)
        log_operation(f"Could not compress image: {image_path} to target size. Moved it to {destination_path}. Time taken: {time.time() - start_time:.2f} seconds.")
        return img_bytes 




def get_image_identifier(image_path):
    """Get the ique identifier for the image based on its last modified time."""
    modified_time = os.path.getmtime(image_path)
    date_str = time.strftime('%Y:%m:%d %H:%M:%S', time.localtime(modified_time))
    return f"{date_str}_{os.path.basename(image_path)}"


def update_log(image_id, status):
    """Update the log file with the image status."""
    with open(LOG_FILE, 'r') as log_file:
        log_data = json.load(log_file)

    log_data[image_id] = status

    with open(LOG_FILE, 'w') as log_file:
        json.dump(log_data, log_file, indent=4)


def get_available_folder(base_path):
    subfolders = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]
    subfolders = sorted([int(folder) for folder in subfolders])
    
    if not subfolders:
        os.makedirs(os.path.join(base_path, '1'))
        return '1'
    
    last_folder = subfolders[-1]
    last_folder_path = os.path.join(base_path, str(last_folder))
    
    if len(os.listdir(last_folder_path)) < PARTITION_SIZE:
        return str(last_folder)
    
    new_folder = str(last_folder + 1)
    os.makedirs(os.path.join(base_path, new_folder))
    return new_folder


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


@app.route('/get_unrated_images', methods=['GET'])
def get_unrated_images():
    start_time = time.time()
    log_operation("Starting to load unrated images.")
    unrated_images = []
    largest_folder = max([int(f) for f in os.listdir(UNRATED_FOLDER) if os.path.isdir(os.path.join(UNRATED_FOLDER, f))], default=0)

    for i in range(largest_folder, 0, -1):
        partition_folder = os.path.join(UNRATED_FOLDER, str(i))
        
        if os.path.isdir(partition_folder):
            unrated_images.extend({
                "partition": str(i),
                "filename": filename
            } for filename in os.listdir(partition_folder) if filename.lower().endswith(('.png', '.jpg', '.jpeg')))
        
        if len(unrated_images) >= 10:
            break
    
    unrated_images = unrated_images[:10]
    log_operation(f"Sent {len(unrated_images)} unrated images list from {UNRATED_FOLDER}")
    log_operation(f"Time taken to load images: {time.time() - start_time:.2f} seconds.")
    return jsonify(unrated_images)


@app.route('/images/<partition>/<filename>', methods=['GET'])
def serve_image(partition, filename):
    start_time = time.time()
    log_operation(f"Starting to serve image {partition}/{filename}.")
    image_path = os.path.join(UNRATED_FOLDER, partition, filename)

    if os.path.exists(image_path):
        if os.path.getsize(image_path) > MAX_FILE_SIZE_BYTES:
            compressed_image = compress_image(image_path)
            log_operation(f"Compressed image: {filename} from {image_path}, Time taken: {time.time() - start_time:.2f} seconds.")
            return send_file(compressed_image, mimetype='image/jpeg')

        log_operation(f"Original image: {filename} from {image_path}, Time taken: {time.time() - start_time:.2f} seconds.")
        return send_file(image_path)

    log_operation(f"Image not fod: {filename} in partition {partition}")
    return "Image not fod", 404


@app.route('/rate_image', methods=['POST'])
async def rate_image():
    start_time = time.time()
    log_operation("Starting to rate an image.")
    data = request.get_json()
    image_name = data.get('image_name')
    rating = data.get('rating')
    partition = data.get('partition')

    if image_name and rating:
        source_path = os.path.join(UNRATED_FOLDER, partition, image_name)
        destination_folder = os.path.join(RATED_FOLDER, str(rating))

        os.makedirs(destination_folder, exist_ok=True)
        destination_path = os.path.join(destination_folder, image_name)
        
        shutil.move(source_path, destination_path)
        image_id = get_image_identifier(destination_path)
        update_log(image_id, 'rated')

        log_operation(f"Moved rated image: {image_name} from {source_path} to {destination_path}")

        if not os.listdir(os.path.join(UNRATED_FOLDER, partition)):
            os.rmdir(os.path.join(UNRATED_FOLDER, partition))
            log_operation(f"Deleted empty partition folder: {partition}")

        log_operation(f"Time taken to rate image: {time.time() - start_time:.2f} seconds")
        return jsonify({"status": "success", "message": f"Image {image_name} moved to rated {rating} folder"}), 200

    log_operation("Invalid data received for rating an image.")
    return jsonify({"status": "error", "message": "Invalid data"}), 400


def start_scheduler():
    scheduler = BackgroundScheduler()
    job1 = scheduler.add_job(move_images_to_unrated, 'interval', seconds=SCHEDULER_INTERVAL_MOVE_TO_UNRATED)
    job2 = scheduler.add_job(archive_log_file, 'interval', seconds=SCHEDULER_INTERVAL_CHECK_LOG_SIZE) 
    job1.func()
    job2.func()
    scheduler.start()


if __name__ == '__main__':
    start_scheduler()
    serve(app, host='0.0.0.0', port=5410)
