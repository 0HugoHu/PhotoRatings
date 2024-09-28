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


def compress_image(image_path):
    """Compress the image by resizing it while preserving the original format, to be under a certain size limit (in MB)."""
    start_time = time.time()
    log_operation(f"Compressing image: {image_path}")
    
    with Image.open(image_path) as img:
        img_format = img.format
        img_bytes = BytesIO()
        original_size = img.size
        target_size = MAX_IMAGE_SIZE_BYTES

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
