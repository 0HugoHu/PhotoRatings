from config import *
from io import BytesIO
import time
import shutil
import time


def get_image_identifier(image_path):
    """Get the ique identifier for the image based on its last modified time."""
    modified_time = os.path.getmtime(image_path)
    date_str = time.strftime('%Y:%m:%d %H:%M:%S', time.localtime(modified_time))
    return f"{date_str}_{os.path.basename(image_path)}"


def get_largets_partition(base_path):
    partition_folders = [f for f in os.listdir(base_path) 
                         if os.path.isdir(os.path.join(base_path, f)) 
                         and not THUMBNAIL in f]
    
    partition_folders = sorted(partition_folders, key=int, reverse=True)
    return int(partition_folders[0]) if partition_folders else 0


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


def update_log(image_id, status):
    """Update the log file with the image status."""
    with open(LOG_FILE, 'r') as log_file:
        log_data = json.load(log_file)

    log_data[image_id] = status

    with open(LOG_FILE, 'w') as log_file:
        json.dump(log_data, log_file, indent=4)


def cleanup_stale_images(served_images):
    current_time = time.time()
    for user, images in served_images.items():
        stale_images = [filename for filename, timestamp in images.items() 
                        if current_time - timestamp > SERVE_TIMEOUT_SECONDS]
        for stale_image in stale_images:
            del served_images[user][stale_image]
            log_operation(f"Removed stale image {stale_image} from user {user}'s served list.")

