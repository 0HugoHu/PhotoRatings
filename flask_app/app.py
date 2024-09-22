import logging
from flask import Flask, jsonify, request, send_file
import os
import shutil
import json
import time
from PIL import Image
from io import BytesIO
from apscheduler.schedulers.background import BackgroundScheduler
from waitress import serve

app = Flask(__name__)

# Folder paths
BASE_DIR = os.getcwd()
RAW_FOLDER = os.path.join(BASE_DIR, 'images_raw')
UNRATED_FOLDER = os.path.join(BASE_DIR, 'images_unrated')
RATED_FOLDER = os.path.join(BASE_DIR, 'images_rated')
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'images_log.json')
OPERATION_LOG_FILE = os.path.join(BASE_DIR, 'logs', 'operations.log')

# Constants
PARTITION_SIZE = 100  # Number of images per partition
SCHEDULER_INTERVAL = 60  # Time interval for the scheduler (seconds)
MAX_FILE_SIZE_BYTES = 1 * 1024 * 1024  # 1MB in bytes

# Configure logging
logging.basicConfig(
    filename=OPERATION_LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# Load or create the log file
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'w') as log_file:
        json.dump({}, log_file)


def log_operation(message):
    """Log an operation message."""
    logging.info(message)


def compress_image(image_path):
    """Compress the image to be under a certain size limit (in MB)."""
    with Image.open(image_path) as img:
        img_bytes = BytesIO()
        img.save(img_bytes, format=img.format, quality=85)
        
        while img_bytes.tell() > MAX_FILE_SIZE_BYTES and img_bytes.getbuffer().nbytes > 10:
            img_bytes = BytesIO()
            img.save(img_bytes, format=img.format, quality=max(0, img_bytes.getbuffer().nbytes // 1000))  # Reduce quality iteratively
            img_bytes.seek(0)

        return img_bytes


def get_image_identifier(image_path):
    """Get the unique identifier for the image based on its last modified time."""
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
    start_time = time.time()
    log_operation("Starting to move images from raw to unrated.")
    
    with open(LOG_FILE, 'r') as log_file:
        log_data = json.load(log_file)

    for image_file in os.listdir(RAW_FOLDER):
        if image_file.lower().endswith(('.jpg', '.jpeg', '.png')):
            source_path = os.path.join(RAW_FOLDER, image_file)
            image_id = get_image_identifier(source_path)

            if image_id in log_data:
                log_operation(f"Removing duplicate image: {image_file} from {RAW_FOLDER}")
                os.remove(source_path)
                continue

            available_folder = get_available_folder(UNRATED_FOLDER)
            dest_path = os.path.join(UNRATED_FOLDER, available_folder, image_file)
            shutil.move(source_path, dest_path)
            log_operation(f"Moved image: {image_file} from {RAW_FOLDER} to {dest_path}")

            log_data[image_id] = {'status': 'unrated', 'path': dest_path}

    with open(LOG_FILE, 'w') as log_file:
        json.dump(log_data, log_file, indent=4)

    log_operation(f"Finished moving images. Time taken: {time.time() - start_time:.2f} seconds.")


@app.route('/get_unrated_images', methods=['GET'])
async def get_unrated_images():
    unrated_images = []
    largest_folder = max([int(f) for f in os.listdir(UNRATED_FOLDER) if os.path.isdir(os.path.join(UNRATED_FOLDER, f))], default=0)

    for i in range(1, largest_folder + 1):
        partition_folder = os.path.join(UNRATED_FOLDER, str(i))
        if os.path.isdir(partition_folder):
            unrated_images.extend({
                "partition": str(i),
                "filename": filename
            } for filename in os.listdir(partition_folder) if filename.lower().endswith(('.png', '.jpg', '.jpeg')))

    log_operation(f"Loaded unrated images: {len(unrated_images)} from {UNRATED_FOLDER}")
    return jsonify(unrated_images)


@app.route('/images/<partition>/<filename>', methods=['GET'])
async def serve_image(partition, filename):
    image_path = os.path.join(UNRATED_FOLDER, partition, filename)

    if os.path.exists(image_path):
        if os.path.getsize(image_path) > MAX_FILE_SIZE_BYTES:
            compressed_image = compress_image(image_path)
            log_operation(f"Serving compressed image: {filename} from {image_path}")
            return send_file(compressed_image, mimetype='image/jpeg')

        log_operation(f"Serving original image: {filename} from {image_path}")
        return send_file(image_path)

    log_operation(f"Image not found: {filename} in partition {partition}")
    return "Image not found", 404


@app.route('/rate_image', methods=['POST'])
def rate_image():
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
        return jsonify({"status": "success", "message": f"Image {image_name} moved to rated {rating} folder"}), 200

    log_operation("Invalid data received for rating an image.")
    return jsonify({"status": "error", "message": "Invalid data"}), 400


def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(move_images_to_unrated, 'interval', seconds=SCHEDULER_INTERVAL)
    scheduler.start()


if __name__ == '__main__':
    start_scheduler()
    serve(app, host='0.0.0.0', port=5410)
