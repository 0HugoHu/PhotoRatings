from flask import Flask, send_file, app, jsonify, request
from schedulers import *
from waitress import serve
from secrets_do_not_upload import *
import jwt
from schedulers import *
from functools import wraps
from datetime import datetime, timedelta, timezone

app = Flask(__name__)
app.config['SECRET_KEY'] = HTTP_SECRET_KEY

served_images = {}


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            token = request.headers['Authorization'].split(" ")[1]
        if not token:
            return jsonify({'message': 'Token is missing!'}), 403

        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
            current_user = data['user']
        except:
            return jsonify({'message': 'Token is invalid!'}), 403

        return f(*args, **kwargs)

    return decorated


@app.route('/photo_ratings_login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if username == HTTP_USER_NAME and password == HTTP_PASSWORD:
        token = jwt.encode({
            'user': username,
            'exp': datetime.now(timezone.utc) + timedelta(hours=12)
        }, app.config['SECRET_KEY'], algorithm="HS256")
        return jsonify({'token': token})

    return jsonify({'message': 'Invalid credentials'}), 401


@app.route('/get_unrated_images', methods=['GET'])
@token_required
def get_unrated_images():
    start_time = time.time()
    log_operation("Starting to load unrated images.")

    # Get the current user from the token
    token = request.headers['Authorization'].split(" ")[1]
    data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
    current_user = data['user']

    # Get a list of already-served images for this user (if any)
    already_served = served_images.get(current_user, [])

    # Fetch all unrated images
    unrated_images = []
    largest_folder = max([int(f) for f in os.listdir(UNRATED_FOLDER) if os.path.isdir(os.path.join(UNRATED_FOLDER, f))], default=0)

    for i in range(largest_folder, 0, -1):
        partition_folder = os.path.join(UNRATED_FOLDER, str(i))
        
        if os.path.isdir(partition_folder):
            unrated_images.extend({
                "partition": str(i),
                "filename": filename
            } for filename in os.listdir(partition_folder)
              if filename.lower().endswith(('.png', '.jpg', '.jpeg')) and filename not in already_served)

        if len(unrated_images) >= 10:
            break

    # Limit to 10 images and update the list of already-served images
    unrated_images = unrated_images[:10]
    served_images[current_user] = already_served + [image['filename'] for image in unrated_images]

    log_operation(f"Sent {len(unrated_images)} unrated images list from {UNRATED_FOLDER}")
    log_operation(f"Time taken to load images: {time.time() - start_time:.2f} seconds.")
    return jsonify(unrated_images)


@app.route('/images/<partition>/<filename>', methods=['GET'])
@token_required
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
@token_required
def rate_image():
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

        # Remove the rated image from the served_images list for the user
        token = request.headers['Authorization'].split(" ")[1]
        data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
        current_user = data['user']

        if current_user in served_images:
            served_images[current_user] = [img for img in served_images[current_user] if img != image_name]

        # Clean up empty folder
        if not os.listdir(os.path.join(UNRATED_FOLDER, partition)):
            os.rmdir(os.path.join(UNRATED_FOLDER, partition))
            log_operation(f"Deleted empty partition folder: {partition}")

        log_operation(f"Time taken to rate image: {time.time() - start_time:.2f} seconds")
        return jsonify({"status": "success", "message": f"Image {image_name} moved to rated {rating} folder"}), 200

    log_operation("Invalid data received for rating an image.")
    return jsonify({"status": "error", "message": "Invalid data"}), 400


if __name__ == '__main__':
    start_scheduler()
    serve(app, host='0.0.0.0', port=5410)
