import os
import json
import yt_dlp
import io
import tempfile
from datetime import datetime
from flask import Flask, request, jsonify, send_file, redirect
from flask_cors import CORS
from werkzeug.utils import secure_filename
from werkzeug.security import check_password_hash, generate_password_hash
from moviepy.editor import VideoFileClip
import uuid
from pathlib import Path
import subprocess
import sys
import requests
from dotenv import load_dotenv
from models import User, Session, init_db, seed_default_users, db
from cloudinary_handler import init_cloudinary, upload_video_to_cloudinary, get_video_url, delete_video_from_cloudinary

# Load environment variables
load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*", "methods": ["GET", "POST", "OPTIONS", "PUT", "DELETE"], "allow_headers": ["Content-Type", "Authorization"]}})

# Initialize Cloudinary for video storage
try:
    init_cloudinary()
    print("✓ Cloudinary initialized successfully")
except Exception as e:
    print(f"⚠ Cloudinary initialization warning: {e}")
    print("  Make sure CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET are set")

# Initialize database on startup
try:
    init_db()
    seed_default_users()
    print("✓ Database initialized successfully")
except Exception as e:
    print(f"⚠ Database initialization warning: {e}")
    print("  Make sure MongoDB is running on localhost:27017")

# Configuration
UPLOAD_FOLDER = 'uploads'
CHUNKS_FOLDER = 'chunks'
ALLOWED_EXTENSIONS = {'mp4', 'avi', 'mov', 'mkv', 'flv', 'wmv', 'webm', 'm4v'}
CHUNK_DURATION = 10  # seconds
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
UPLOADED_SESSIONS_FILE = os.path.join(DATA_DIR, 'mentor_uploaded_sessions.json')
PUBLIC_RANKINGS_FILE = os.path.join(DATA_DIR, 'public_mentor_rankings.json')

# Create necessary directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(CHUNKS_FOLDER, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def download_youtube_video(url):
    """Download video from YouTube URL"""
    unique_id = str(uuid.uuid4())
    output_path = os.path.join(UPLOAD_FOLDER, f"{unique_id}.%(ext)s")
    
    ydl_opts = {
        'format': 'best[height<=720]/best',  # Prefer 720p or lower for better compatibility
        'outtmpl': output_path,
        'merge_output_format': 'mp4',
        'quiet': False,
        'no_warnings': False,
        'extract_flat': False,
        'noplaylist': True,
        'ignoreerrors': False,
    }
    
    downloaded_filename = None
    
    def progress_hook(d):
        nonlocal downloaded_filename
        if d['status'] == 'finished':
            downloaded_filename = d.get('filename')
    
    ydl_opts['progress_hooks'] = [progress_hook]
    
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Download the video
            ydl.download([url])
            
            # Use the filename from progress hook or find it
            if downloaded_filename and os.path.exists(downloaded_filename):
                filename = downloaded_filename
            else:
                # Fallback: find the most recently created file in uploads folder
                files = [f for f in os.listdir(UPLOAD_FOLDER) if f.startswith(unique_id)]
                if files:
                    filename = os.path.join(UPLOAD_FOLDER, files[0])
                else:
                    raise Exception("Could not find downloaded file")
            
            # Ensure it has .mp4 extension
            if not filename.endswith('.mp4'):
                base_name = filename.rsplit('.', 1)[0]
                new_filename = f"{base_name}.mp4"
                if filename != new_filename and os.path.exists(filename):
                    # Rename if different extension
                    os.rename(filename, new_filename)
                filename = new_filename
            
            # Verify file exists
            if not os.path.exists(filename):
                raise Exception(f"Downloaded file not found: {filename}")
            
            return filename
    except Exception as e:
        error_msg = str(e)
        if "HTTP Error 403" in error_msg or "HTTP Error 400" in error_msg:
            raise Exception("YouTube download failed. The video may be restricted or unavailable. Please try updating yt-dlp: pip install --upgrade yt-dlp")
        raise Exception(f"Error downloading YouTube video: {error_msg}")

def split_video_into_chunks(video_path, output_folder):
    """Split video into chunks of specified duration using FFmpeg directly"""
    try:
        # First, get video duration using ffprobe
        probe_cmd = [
            'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1', video_path
        ]
        
        try:
            result = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
            duration = float(result.stdout.strip())
        except (subprocess.CalledProcessError, ValueError, FileNotFoundError):
            # Fallback to MoviePy for duration if ffprobe fails
            video = VideoFileClip(video_path)
            duration = video.duration
            video.close()
        
        chunk_count = 0
        chunks_info = []
        base_name = Path(video_path).stem
        
        # Process each chunk using FFmpeg directly (more reliable)
        for start_time in range(0, int(duration), CHUNK_DURATION):
            end_time = min(start_time + CHUNK_DURATION, duration)
            chunk_filename = f"{base_name}_chunk_{chunk_count:04d}.mp4"
            chunk_path = os.path.join(output_folder, chunk_filename)
            
            # Use FFmpeg to extract chunk directly
            ffmpeg_cmd = [
                'ffmpeg', '-i', video_path,
                '-ss', str(start_time),
                '-t', str(end_time - start_time),
                '-c:v', 'libx264',
                '-c:a', 'aac',
                '-avoid_negative_ts', 'make_zero',
                '-y',  # Overwrite output file
                chunk_path
            ]
            
            try:
                # Run FFmpeg with suppressed output
                subprocess.run(
                    ffmpeg_cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=True
                )
                
                # Verify chunk was created
                if os.path.exists(chunk_path) and os.path.getsize(chunk_path) > 0:
                    chunks_info.append({
                        'filename': chunk_filename,
                        'start_time': start_time,
                        'end_time': end_time,
                        'duration': end_time - start_time
                    })
                    chunk_count += 1
                else:
                    raise Exception(f"Chunk file was not created properly: {chunk_filename}")
                    
            except subprocess.CalledProcessError as e:
                raise Exception(f"FFmpeg error creating chunk {chunk_count}: {str(e)}")
            except FileNotFoundError:
                # FFmpeg not found, fallback to MoviePy
                return split_video_into_chunks_moviepy(video_path, output_folder)
        
        return chunks_info
    except Exception as e:
        raise Exception(f"Error splitting video: {str(e)}")

def split_video_into_chunks_moviepy(video_path, output_folder):
    """Fallback method using MoviePy - reloads video for each chunk to avoid resource issues"""
    try:
        # Get base filename without extension
        base_name = Path(video_path).stem
        chunk_count = 0
        chunks_info = []
        
        # First, get duration
        temp_video = VideoFileClip(video_path)
        duration = temp_video.duration
        temp_video.close()
        del temp_video
        
        # Process each chunk by reloading the video each time (more reliable)
        for start_time in range(0, int(duration), CHUNK_DURATION):
            end_time = min(start_time + CHUNK_DURATION, duration)
            chunk_filename = f"{base_name}_chunk_{chunk_count:04d}.mp4"
            chunk_path = os.path.join(output_folder, chunk_filename)
            
            video = None
            chunk = None
            
            try:
                # Reload video for each chunk to avoid resource issues
                video = VideoFileClip(video_path)
                chunk = video.subclip(start_time, end_time)
                
                # Write with proper error handling
                chunk.write_videofile(
                    chunk_path,
                    codec='libx264',
                    audio_codec='aac',
                    verbose=False,
                    logger=None,
                    preset='medium',
                    threads=4,
                    temp_audiofile=os.path.join(output_folder, f'temp_audio_{chunk_count}.m4a')
                )
                
                chunks_info.append({
                    'filename': chunk_filename,
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                })
                chunk_count += 1
                
            except Exception as chunk_error:
                raise Exception(f"Error creating chunk {chunk_count}: {str(chunk_error)}")
            finally:
                # Always clean up resources
                if chunk is not None:
                    try:
                        chunk.close()
                    except:
                        pass
                if video is not None:
                    try:
                        video.close()
                    except:
                        pass
                # Clean up temp audio file if it exists
                temp_audio = os.path.join(output_folder, f'temp_audio_{chunk_count}.m4a')
                if os.path.exists(temp_audio):
                    try:
                        os.remove(temp_audio)
                    except:
                        pass
        
        return chunks_info
    except Exception as e:
        raise Exception(f"Error splitting video with MoviePy: {str(e)}")

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Handle file upload"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'File type not allowed'}), 400
    
    try:
        # Save uploaded file
        unique_id = str(uuid.uuid4())
        filename = secure_filename(file.filename)
        file_ext = filename.rsplit('.', 1)[1].lower()
        saved_filename = f"{unique_id}.{file_ext}"
        file_path = os.path.join(UPLOAD_FOLDER, saved_filename)
        file.save(file_path)
        
        # Create output folder for chunks
        output_folder = os.path.join(CHUNKS_FOLDER, unique_id)
        os.makedirs(output_folder, exist_ok=True)
        
        # Split video into chunks
        chunks_info = split_video_into_chunks(file_path, output_folder)
        
        return jsonify({
            'message': 'Video processed successfully',
            'chunks_count': len(chunks_info),
            'chunks': chunks_info,
            'chunks_folder': output_folder
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/youtube', methods=['POST'])
def process_youtube_url():
    """Handle YouTube URL"""
    data = request.get_json()
    if not data or 'url' not in data:
        return jsonify({'error': 'No URL provided'}), 400
    
    url = data['url']
    
    try:
        # Download video from YouTube
        video_path = download_youtube_video(url)
        
        # Create output folder for chunks
        unique_id = Path(video_path).stem
        output_folder = os.path.join(CHUNKS_FOLDER, unique_id)
        os.makedirs(output_folder, exist_ok=True)
        
        # Split video into chunks
        chunks_info = split_video_into_chunks(video_path, output_folder)
        
        return jsonify({
            'message': 'YouTube video processed successfully',
            'chunks_count': len(chunks_info),
            'chunks': chunks_info,
            'chunks_folder': output_folder
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/cloudinary/signature', methods=['POST'])
def get_cloudinary_signature():
    """Generate a signed upload signature for Cloudinary uploads."""
    try:
        import hashlib
        data = request.get_json()
        mentor_id = data.get('mentorId')
        session_id = data.get('sessionId')
        
        if not mentor_id or not session_id:
            return jsonify({'error': 'Missing mentorId or sessionId'}), 400
        
        # Get Cloudinary credentials from environment
        api_key = os.getenv('CLOUDINARY_API_KEY')
        api_secret = os.getenv('CLOUDINARY_API_SECRET')
        
        if not api_key or not api_secret:
            return jsonify({'error': 'Cloudinary credentials not configured'}), 500
        
        # Generate timestamp
        import time
        timestamp = int(time.time())
        
        # Build the signature string
        public_id = f"mentor_videos/{mentor_id}/{session_id}"
        params = {
            'public_id': public_id,
            'folder': 'mentor_videos',
            'overwrite': True,
            'invalidate': True,
            'tags': f'mentor,session,{mentor_id}',
            'timestamp': timestamp
        }
        
        # Sort params alphabetically
        param_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        signature_string = f"{param_string}{api_secret}"
        
        # Generate SHA-1 signature
        signature = hashlib.sha1(signature_string.encode()).hexdigest()
        
        return jsonify({
            'signature': signature,
            'timestamp': timestamp,
            'api_key': api_key,
            'public_id': public_id
        }), 200
        
    except Exception as e:
        return jsonify({'error': f'Failed to generate signature: {str(e)}'}), 500


@app.route('/api/cloudinary/delete', methods=['POST'])
def delete_cloudinary_video():
    """Delete a video from Cloudinary."""
    try:
        data = request.get_json()
        public_id = data.get('publicId')
        
        if not public_id:
            return jsonify({'error': 'Missing publicId'}), 400
        
        delete_video_from_cloudinary(public_id)
        return jsonify({'message': 'Video deleted successfully'}), 200
        
    except Exception as e:
        return jsonify({'error': f'Failed to delete video: {str(e)}'}), 500

@app.route('/api/auth/login', methods=['POST'])
def login():
    """Handle user login using MongoDB"""
    data = request.get_json()
    
    if not data:
        return jsonify({'error': 'No data provided'}), 400
    
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')
    role = data.get('role', '').strip().lower()
    
    if not email or not password or not role:
        return jsonify({'error': 'Email, password, and role are required'}), 400
    
    if role not in ['student', 'mentor', 'university']:
        return jsonify({'error': 'Invalid role. Must be "student", "mentor", or "university"'}), 400
    
    # Verify user credentials using MongoDB
    user = User.verify_password(email, password)
    
    if not user:
        return jsonify({'error': 'Invalid email or password'}), 401
    
    # Verify role matches
    if user['role'] != role:
        return jsonify({'error': f'Invalid role. This account is registered as {user["role"]}'}), 403
    
    return jsonify({
        'message': 'Login successful',
        'email': email,
        'role': user['role'],
        'id': user.get('_id')
    }), 200

@app.route('/api/auth/register', methods=['POST'])
def register():
    """Handle user registration (disabled - use predefined users only)"""
    return jsonify({
        'error': 'User registration is disabled. Only predefined users can log in.'
    }), 403

@app.route('/api/mentors', methods=['GET'])
def get_mentors():
    """Get list of all mentors"""
    try:
        data_path = os.path.join(os.path.dirname(__file__), 'data', 'mentors.json')
        with open(data_path, 'r') as f:
            data = json.load(f)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({'error': f'Failed to load mentors: {str(e)}'}), 500

@app.route('/api/mentors/search', methods=['GET'])
def search_mentors():
    """Search mentors by name or specialization"""
    try:
        query = request.args.get('q', '').lower()
        data_path = os.path.join(os.path.dirname(__file__), 'data', 'mentors.json')
        with open(data_path, 'r') as f:
            data = json.load(f)
        
        mentors = data.get('mentors', [])
        
        if query:
            filtered_mentors = [
                mentor for mentor in mentors
                if query in mentor.get('name', '').lower() or
                   query in mentor.get('specialization', '').lower() or
                   query in mentor.get('bio', '').lower()
            ]
        else:
            filtered_mentors = mentors
        
        return jsonify({'mentors': filtered_mentors}), 200
    except Exception as e:
        return jsonify({'error': f'Failed to search mentors: {str(e)}'}), 500

@app.route('/api/audio/<video_id>', methods=['GET'])
def get_audio_for_video(video_id):
    """Get audio metadata for a specific video"""
    try:
        data_path = os.path.join(os.path.dirname(__file__), 'data', 'audio_metadata.json')
        with open(data_path, 'r') as f:
            data = json.load(f)
        
        # Find audio for the video_id or return first available as dummy
        audio_files = data.get('audioFiles', [])
        audio = next((a for a in audio_files if a.get('videoId') == video_id), None)
        
        if not audio:
            # Return first audio as dummy data if no match found
            audio = audio_files[0] if audio_files else None
        
        if not audio:
            return jsonify({'error': 'No audio found'}), 404
        
        return jsonify(audio), 200
    except Exception as e:
        return jsonify({'error': f'Failed to load audio: {str(e)}'}), 500

@app.route('/api/audio/create', methods=['POST'])
def create_audio():
    """Create audio explanation for a video (dummy endpoint)"""
    try:
        data = request.get_json()
        video_id = data.get('videoId')
        mentor_id = data.get('mentorId')
        
        if not video_id or not mentor_id:
            return jsonify({'error': 'videoId and mentorId are required'}), 400
        
        # Load mentors to get mentor name
        mentors_path = os.path.join(os.path.dirname(__file__), 'data', 'mentors.json')
        with open(mentors_path, 'r') as f:
            mentors_data = json.load(f)
        
        mentor = next((m for m in mentors_data.get('mentors', []) if m.get('id') == mentor_id), None)
        mentor_name = mentor.get('name', 'Unknown Mentor') if mentor else 'Unknown Mentor'
        
        # Load audio metadata
        audio_path = os.path.join(os.path.dirname(__file__), 'data', 'audio_metadata.json')
        with open(audio_path, 'r') as f:
            audio_data = json.load(f)
        
        # Get first available audio as dummy (or match by mentor if available)
        audio_files = audio_data.get('audioFiles', [])
        dummy_audio = next((a for a in audio_files if a.get('mentorId') == mentor_id), None)
        if not dummy_audio:
            dummy_audio = audio_files[0] if audio_files else None
        
        # Use existing audio ID so transcription matches, or create new one
        audio_id = dummy_audio.get('id', f'audio_{uuid.uuid4().hex[:8]}') if dummy_audio else f'audio_{uuid.uuid4().hex[:8]}'
        
        # Create new audio entry (dummy)
        new_audio = {
            'id': audio_id,
            'videoId': video_id,
            'mentorId': mentor_id,
            'mentorName': mentor_name,
            'title': f'Explanation by {mentor_name}',
            'description': f'Audio explanation for video {video_id}',
            'duration': dummy_audio.get('duration', 180) if dummy_audio else 180,
            'url': dummy_audio.get('url', '/api/audio/dummy.mp3') if dummy_audio else '/api/audio/dummy.mp3',
            'createdAt': '2024-01-18T10:00:00Z'
        }
        
        return jsonify(new_audio), 201
    except Exception as e:
        return jsonify({'error': f'Failed to create audio: {str(e)}'}), 500

@app.route('/api/transcription/<audio_id>', methods=['GET'])
def get_transcription(audio_id):
    """Get transcription for a specific audio"""
    try:
        data_path = os.path.join(os.path.dirname(__file__), 'data', 'transcriptions.json')
        with open(data_path, 'r') as f:
            data = json.load(f)
        
        transcriptions = data.get('transcriptions', [])
        transcription = next((t for t in transcriptions if t.get('audioId') == audio_id), None)
        
        if not transcription:
            # Return first transcription as dummy data if no match found
            transcription = transcriptions[0] if transcriptions else None
        
        if not transcription:
            return jsonify({'error': 'No transcription found'}), 404
        
        return jsonify(transcription), 200
    except Exception as e:
        return jsonify({'error': f'Failed to load transcription: {str(e)}'}), 500

@app.route('/api/mentor/<mentor_id>/snapshot', methods=['GET'])
def get_mentor_snapshot(mentor_id):
    """Get mentor snapshot data"""
    try:
        data_path = os.path.join(os.path.dirname(__file__), 'data', 'mentor_snapshot.json')
        with open(data_path, 'r') as f:
            data = json.load(f)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({'error': f'Failed to load mentor snapshot: {str(e)}'}), 500

@app.route('/api/mentor/<mentor_id>/skills', methods=['GET'])
def get_mentor_skills(mentor_id):
    """Get mentor skills data"""
    try:
        data_path = os.path.join(os.path.dirname(__file__), 'data', 'mentor_skills.json')
        with open(data_path, 'r') as f:
            data = json.load(f)
        return jsonify(data), 200
    except Exception as e:
        return jsonify({'error': f'Failed to load mentor skills: {str(e)}'}), 500

@app.route('/api/mentor/<mentor_id>/sessions', methods=['GET'])
def get_mentor_sessions(mentor_id):
    """Get mentor recent sessions"""
    try:
        # First try to load sessions from the DB for this mentor
        limit = request.args.get('limit', type=int)
        sessions = Session.find_by_mentor(mentor_id, limit=limit)

        # If DB returned nothing, fall back to dummy JSON
        if not sessions:
            data_path = os.path.join(os.path.dirname(__file__), 'data', 'mentor_sessions.json')
            with open(data_path, 'r') as f:
                data = json.load(f)
            sessions = data.get('sessions', [])

        return jsonify({'sessions': sessions}), 200
    except Exception as e:
        return jsonify({'error': f'Failed to load mentor sessions: {str(e)}'}), 500

@app.route('/api/mentor/<mentor_id>/sessions/<session_id>/breakdown', methods=['GET'])
def get_session_breakdown(mentor_id, session_id):
    """Get detailed breakdown for a specific session"""
    try:
        # Try DB first
        breakdown = Session.find_by_sessionId(session_id)

        if breakdown:
            # Normalize the DB document to the canonical schema before returning
            try:
                normalized = Session.normalize_for_api(breakdown)
                return jsonify(normalized), 200
            except Exception:
                # Fallback: return the raw breakdown with internal fields stripped
                if '_id' in breakdown:
                    del breakdown['_id']
                return jsonify(breakdown), 200

        # Fallback to static JSON dummy data
        data_path = os.path.join(os.path.dirname(__file__), 'data', 'session_breakdown.json')
        with open(data_path, 'r') as f:
            data = json.load(f)

        # Get breakdown for the specific session
        breakdown = data.get(session_id)

        if not breakdown:
            # Return first available breakdown as dummy data if session not found
            first_key = next(iter(data.keys())) if data else None
            breakdown = data.get(first_key) if first_key else None

        if not breakdown:
            return jsonify({'error': 'No breakdown data found'}), 404

        return jsonify(breakdown), 200
    except Exception as e:
        return jsonify({'error': f'Failed to load session breakdown: {str(e)}'}), 500

def load_uploaded_sessions():
    """Load uploaded sessions data, create file if missing"""
    if not os.path.exists(UPLOADED_SESSIONS_FILE):
        with open(UPLOADED_SESSIONS_FILE, 'w') as f:
            json.dump({'sessions': []}, f, indent=2)

    with open(UPLOADED_SESSIONS_FILE, 'r') as f:
        return json.load(f)

def save_uploaded_sessions(data):
    """Persist uploaded sessions data"""
    with open(UPLOADED_SESSIONS_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def load_public_rankings():
    """Load public rankings data, create file with defaults if missing"""
    if not os.path.exists(PUBLIC_RANKINGS_FILE):
        default_data = {
            "filters": {
                "subjects": ["Mathematics", "Computer Science", "Physics", "Chemistry"],
                "languages": ["English", "Hindi", "Spanish"],
                "experienceLevels": ["0-2 years", "3-5 years", "6-10 years", "10+ years"],
                "timeWindows": ["weekly", "monthly"]
            },
            "rankings": [
                {
                    "id": "m1",
                    "rank": 1,
                    "name": "Ayesha Sharma",
                    "verified": True,
                    "overallScore": 94,
                    "strengthTag": "Best Engagement",
                    "subject": "Computer Science",
                    "language": "English",
                    "experienceLevel": "6-10 years",
                    "timeWindow": "weekly",
                    "avgScoreTrend": [90, 92, 94, 94]
                },
                {
                    "id": "m2",
                    "rank": 2,
                    "name": "Rahul Verma",
                    "verified": True,
                    "overallScore": 92,
                    "strengthTag": "Top 10% in Clarity",
                    "subject": "Mathematics",
                    "language": "Hindi",
                    "experienceLevel": "3-5 years",
                    "timeWindow": "weekly",
                    "avgScoreTrend": [88, 89, 91, 92]
                },
                {
                    "id": "m3",
                    "rank": 3,
                    "name": "Elena García",
                    "verified": True,
                    "overallScore": 90,
                    "strengthTag": "Best Engagement",
                    "subject": "Physics",
                    "language": "Spanish",
                    "experienceLevel": "10+ years",
                    "timeWindow": "weekly",
                    "avgScoreTrend": [86, 88, 89, 90]
                }
            ],
            "profiles": {
                "m1": {
                    "id": "m1",
                    "name": "Ayesha Sharma",
                    "verified": True,
                    "bio": "Computer Science mentor focused on algorithms and systems design.",
                    "expertise": ["Algorithms", "System Design", "Data Structures"],
                    "strengthTag": "Best Engagement",
                    "avgScoreTrend": [90, 92, 94, 94],
                    "peerBadges": ["Top 10% in Clarity", "Top 5% in Engagement"],
                    "teachingHighlights": [
                        "Led 120+ live sessions with 95% satisfaction.",
                        "Introduced interactive quizzes that increased engagement by 20%.",
                        "Mentored students into top competitive programming ranks."
                    ],
                    "contact": {
                        "email": "ayesha.sharma@example.com",
                        "phone": "+91 98765 43210",
                        "linkedin": "https://www.linkedin.com/in/ayesha-sharma",
                        "twitter": "https://twitter.com/ayesha_teaches"
                    }
                },
                "m2": {
                    "id": "m2",
                    "name": "Rahul Verma",
                    "verified": True,
                    "bio": "Mathematics educator specializing in calculus and linear algebra.",
                    "expertise": ["Calculus", "Linear Algebra", "Probability"],
                    "strengthTag": "Top 10% in Clarity",
                    "avgScoreTrend": [88, 89, 91, 92],
                    "peerBadges": ["Top 10% in Clarity"],
                    "teachingHighlights": [
                        "Simplified complex calculus concepts with visual aids.",
                        "Developed problem sets that boosted practice completion by 30%.",
                        "Hosted bilingual sessions to widen accessibility."
                    ],
                    "contact": {
                        "email": "rahul.verma@example.com",
                        "phone": "+91 91234 56780",
                        "linkedin": "https://www.linkedin.com/in/rahul-verma",
                        "twitter": "https://twitter.com/rahul_math"
                    }
                },
                "m3": {
                    "id": "m3",
                    "name": "Elena García",
                    "verified": True,
                    "bio": "Physics mentor with a focus on mechanics and electromagnetism.",
                    "expertise": ["Mechanics", "Electromagnetism", "Exam Strategy"],
                    "strengthTag": "Best Engagement",
                    "avgScoreTrend": [86, 88, 89, 90],
                    "peerBadges": ["Top 5% in Engagement"],
                    "teachingHighlights": [
                        "Uses demos to explain core physics principles.",
                        "Runs weekly doubt-clearing clinics with high attendance.",
                        "Publishes concise recap notes after every session."
                    ],
                    "contact": {
                        "email": "elena.garcia@example.com",
                        "phone": "+34 612 345 678",
                        "linkedin": "https://www.linkedin.com/in/elena-garcia",
                        "twitter": "https://twitter.com/elena_phys"
                    }
                }
            }
        }
        with open(PUBLIC_RANKINGS_FILE, 'w') as f:
            json.dump(default_data, f, indent=2)

    with open(PUBLIC_RANKINGS_FILE, 'r') as f:
        return json.load(f)


@app.route('/api/mentor/<mentor_id>/sessions/uploaded', methods=['GET'])
def get_uploaded_sessions(mentor_id):
    """Return previously uploaded sessions with dummy analysis"""
    try:
        # Prefer DB-backed sessions for this mentor
        sessions = Session.find_by_mentor(mentor_id)
        if sessions:
            return jsonify({'sessions': sessions}), 200

        # Fallback to file-based sessions
        data = load_uploaded_sessions()
        return jsonify({'sessions': data.get('sessions', [])}), 200
    except Exception as e:
        return jsonify({'error': f'Failed to load uploaded sessions: {str(e)}'}), 500

def download_cloudinary_video(video_url, session_id):
    """Download video from Cloudinary URL and save locally."""
    try:
        # Create temp filename for downloaded video
        temp_filename = os.path.join(UPLOAD_FOLDER, f'cloudinary_{session_id}.mp4')
        
        # Download the video from Cloudinary URL
        response = requests.get(video_url, timeout=300, stream=True)
        response.raise_for_status()
        
        # Write to file in chunks
        with open(temp_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        # Verify file was downloaded
        if not os.path.exists(temp_filename) or os.path.getsize(temp_filename) == 0:
            raise Exception(f"Failed to download video or file is empty")
        
        print(f"✓ Downloaded Cloudinary video to {temp_filename}")
        return temp_filename
        
    except Exception as e:
        raise Exception(f"Failed to download Cloudinary video: {str(e)}")

def get_video_duration(video_path):
    """Extract video duration in seconds."""
    try:
        # Try ffprobe first
        probe_cmd = [
            'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1', video_path
        ]
        
        try:
            result = subprocess.run(probe_cmd, capture_output=True, text=True, check=True)
            duration = float(result.stdout.strip())
            return duration
        except (subprocess.CalledProcessError, ValueError, FileNotFoundError):
            # Fallback to MoviePy
            video = VideoFileClip(video_path)
            duration = video.duration
            video.close()
            return duration
            
    except Exception as e:
        print(f"⚠ Could not get video duration: {e}")
        return 0

@app.route('/api/mentor/<mentor_id>/sessions/analyze', methods=['POST'])
def analyze_video_from_url(mentor_id):
    """
    Analyze video from either file upload (form-data) or URL (JSON).
    
    Option 1 - File Upload (multipart/form-data):
        POST /api/mentor/{mentor_id}/sessions/analyze
        Content-Type: multipart/form-data
        
        Form fields:
        - file: <video file> (required)
        - context: "session context" (optional)
        - sessionName: "Session name" (optional)
        - userId: "user123" (optional)
    
    Option 2 - URL Processing (application/json):
        POST /api/mentor/{mentor_id}/sessions/analyze
        Content-Type: application/json
        
        {
          "videoUrl": "https://res.cloudinary.com/.../video.mp4",
          "context": "session context",
          "sessionName": "Session name",
          "userId": "user123"
        }
    """
    
    try:
        session_id = f'session_{uuid.uuid4().hex[:8]}'
        local_video_path = None
        video_url = None
        video_duration = 0
        upload_source = None  # 'file' or 'url'
        context_text = ''
        session_name = f'Session {datetime.utcnow().strftime("%b %d %H:%M")}'
        user_id = None

        # ========== PRIORITY 1: Check for FILE UPLOAD (multipart/form-data) ==========
        if 'file' in request.files:
            file = request.files['file']
            
            # Validate file exists and has a filename
            if not file or file.filename == '':
                return jsonify({'error': 'No file selected. Please provide a video file in the "file" field'}), 400
            
            # Validate file type
            if not allowed_file(file.filename):
                return jsonify({'error': 'File type not allowed. Allowed formats: mp4, avi, mov, mkv, flv, wmv, webm, m4v'}), 400
            
            # Extract form data fields
            context_text = request.form.get('context', '')
            session_name = request.form.get('sessionName', session_name)
            user_id = request.form.get('userId')
            
            # Save uploaded file
            try:
                filename = secure_filename(file.filename)
                file_ext = filename.rsplit('.', 1)[1].lower() if '.' in filename else 'mp4'
                saved_filename = f'session_{session_id}.{file_ext}'
                local_video_path = os.path.join(UPLOAD_FOLDER, saved_filename)
                file.save(local_video_path)
                
                # Verify file was saved
                if not os.path.exists(local_video_path):
                    return jsonify({'error': 'File save failed - file does not exist'}), 400
                
                # Get video duration
                video_duration = get_video_duration(local_video_path)
                print(f"✓ File upload: {saved_filename} (duration: {video_duration}s)")
                upload_source = 'file'
                
            except Exception as e:
                return jsonify({'error': f'Failed to process uploaded file: {str(e)}'}), 400
        
        # ========== PRIORITY 2: Check for VIDEO URL (JSON or form-data) ==========
        elif request.is_json or request.form:
            # Get data from JSON
            if request.is_json:
                data = request.get_json() or {}
                video_url = data.get('videoUrl') or data.get('video_url')
                context_text = data.get('context', '')
                session_name = data.get('sessionName', session_name)
                user_id = data.get('userId') or data.get('user_id')
            else:
                # Get data from form (in case URL is sent as form-data)
                video_url = request.form.get('videoUrl') or request.form.get('video_url')
                context_text = request.form.get('context', '')
                session_name = request.form.get('sessionName', session_name)
                user_id = request.form.get('userId')
            
            if not video_url:
                return jsonify({'error': 'No input provided. Send either: 1) file (multipart/form-data), or 2) videoUrl (JSON or form-data)'}), 400
            
            # Download and process Cloudinary video
            try:
                local_video_path = download_cloudinary_video(video_url, session_id)
                video_duration = get_video_duration(local_video_path)
                print(f"✓ URL download: {video_url} (duration: {video_duration}s)")
                upload_source = 'url'
                
            except Exception as e:
                print(f"⚠ Video download warning: {e}")
                # Store original URL even if download fails (for fallback processing)
                upload_source = 'url'
        
        else:
            return jsonify({'error': 'Invalid request. Content-Type must be multipart/form-data (for file) or application/json (for URL)'}), 400
        
        new_session = {
            'sessionId': session_id,
            'sessionName': session_name,
            'videoUrl': video_url,  # Store the original URL (if present)
            'localVideoPath': local_video_path,  # Store path to local video file
            'uploadSource': upload_source,  # 'file' or 'url'
            'cloudinaryPublicId': None,
            'duration': video_duration,
            'mentorId': mentor_id,
            'userId': user_id,
            'uploadedFile': None,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
            'timeline': {
                'audio': [],
                'video': [],
                'transcript': [],
                'scoreDips': [],
                'scorePeaks': []
            },
            'metrics': [],
            'weakMoments': []
        }

        # Call external analysis service (service 1) if available
        analysis_result = None
        analysis_filename = None
        try:
            # Call analysis service if we have either a local video path or a video URL
            if local_video_path or video_url:
                analysis_url = 'https://plots-helped-blast-revised.trycloudflare.com/api/v1/analyze-video'
                
                # Priority 1: Send local video file if available
                if local_video_path and os.path.exists(local_video_path):
                    try:
                        # Read file content first
                        with open(local_video_path, 'rb') as f:
                            file_content = f.read()
                        
                        # Create multipart form data with file field
                        files = {'file': ('video.mp4', file_content, 'video/mp4')}
                        data_to_send = {'context': context_text} if context_text else {}
                        
                        print(f"→ Sending local video to analysis service (POST)... (size: {len(file_content)} bytes)")
                        print(f"→ Analysis URL: {analysis_url}")
                        print(f"→ Form fields: file={len(file_content)} bytes, context={len(context_text)} chars")
                        
                        resp = requests.post(analysis_url, files=files, data=data_to_send, timeout=300)
                        
                        print(f"← Analysis service response: {resp.status_code}")
                        
                        if resp.ok:
                            analysis_result = resp.json()
                            analysis_filename = os.path.join(DATA_DIR, f'analysis_{session_id}.json')
                            with open(analysis_filename, 'w') as af:
                                json.dump(analysis_result, af)
                            print(f"✓ Analysis service returned results (saved to {analysis_filename})")
                        else:
                            print(f"⚠ Analysis service error: {resp.status_code}")
                            print(f"⚠ Response preview: {resp.text[:500]}")
                            try:
                                analysis_result = resp.json()
                            except Exception:
                                analysis_result = {'error': f'Analysis service returned status {resp.status_code}: {resp.text[:200]}'}
                    except Exception as e:
                        print(f"⚠ Local file upload failed: {str(e)}")
                        import traceback
                        traceback.print_exc()
                        
                        # Fallback to URL-based analysis if video_url exists
                        if video_url:
                            print(f"→ Falling back to URL-based analysis (POST)...")
                            analysis_data = {
                                'context': context_text,
                                'video_url': video_url
                            }
                            try:
                                resp = requests.post(analysis_url, json=analysis_data, timeout=120)
                                print(f"← Analysis service (URL fallback) response: {resp.status_code}")
                                
                                if resp.ok:
                                    analysis_result = resp.json()
                                    analysis_filename = os.path.join(DATA_DIR, f'analysis_{session_id}.json')
                                    with open(analysis_filename, 'w') as af:
                                        json.dump(analysis_result, af)
                                    print(f"✓ Analysis service (URL fallback) returned results")
                                else:
                                    try:
                                        analysis_result = resp.json()
                                    except Exception:
                                        analysis_result = {'error': f'Analysis service returned status {resp.status_code}'}
                            except Exception as url_error:
                                analysis_result = {'error': f'URL fallback failed: {str(url_error)}'}
                        else:
                            analysis_result = {'error': f'File upload failed and no URL available: {str(e)}'}
                
                # Priority 2: Send URL if no local video
                elif video_url:
                    try:
                        analysis_data = {
                            'context': context_text,
                            'video_url': video_url
                        }
                        print(f"→ Sending video URL to analysis service...")
                        resp = requests.post(analysis_url, json=analysis_data, timeout=120)
                        if resp.ok:
                            analysis_result = resp.json()
                            analysis_filename = os.path.join(DATA_DIR, f'analysis_{session_id}.json')
                            with open(analysis_filename, 'w') as af:
                                json.dump(analysis_result, af)
                            print(f"✓ Analysis service returned results")
                        else:
                            try:
                                analysis_result = resp.json()
                            except Exception:
                                analysis_result = {'error': f'Analysis service returned status {resp.status_code}'}
                            print(f"⚠ Analysis service error: {resp.status_code}")
                    except Exception as e:
                        analysis_result = {'error': f'Failed to call analysis service: {str(e)}'}
        except Exception as e:
            analysis_result = {'error': f'Failed to call analysis service: {str(e)}'}

        # Call diarization service (service 2)
        diarization_result = None
        diarization_filename = None
        try:
            # Call diarization service if we have either a local video path or a video URL
            if local_video_path or video_url:
                diarization_url = 'https://papers-mate-prefix-shortcuts.trycloudflare.com/process-video'
                
                # Priority 1: Send local video file if available
                if local_video_path and os.path.exists(local_video_path):
                    try:
                        # Read file content first
                        with open(local_video_path, 'rb') as f:
                            file_content = f.read()
                        
                        # Create multipart form data with file field
                        files = {'file': ('video.mp4', file_content, 'video/mp4')}
                        
                        print(f"→ Sending local video to diarization service (POST)... (size: {len(file_content)} bytes)")
                        print(f"→ Diarization URL: {diarization_url}")
                        
                        resp2 = requests.post(diarization_url, files=files, timeout=300)
                        
                        print(f"← Diarization service response: {resp2.status_code}")
                        
                        if resp2.ok:
                            diarization_result = resp2.json()
                            diarization_filename = os.path.join(DATA_DIR, f'diarization_{session_id}.json')
                            with open(diarization_filename, 'w') as df:
                                json.dump(diarization_result, df)
                            print(f"✓ Diarization service returned results (saved to {diarization_filename})")
                        else:
                            print(f"⚠ Diarization service error: {resp2.status_code}")
                            print(f"⚠ Response preview: {resp2.text[:500]}")
                            try:
                                diarization_result = resp2.json()
                            except Exception:
                                diarization_result = {'error': f'Diarization service returned status {resp2.status_code}: {resp2.text[:200]}'}
                    except Exception as e:
                        print(f"⚠ Local file upload failed: {str(e)}")
                        import traceback
                        traceback.print_exc()
                        
                        # Fallback to URL-based diarization if video_url exists
                        if video_url:
                            print(f"→ Falling back to URL-based diarization (POST)...")
                            diarization_data = {'video_url': video_url}
                            try:
                                resp2 = requests.post(diarization_url, json=diarization_data, timeout=120)
                                print(f"← Diarization service (URL fallback) response: {resp2.status_code}")
                                
                                if resp2.ok:
                                    diarization_result = resp2.json()
                                    diarization_filename = os.path.join(DATA_DIR, f'diarization_{session_id}.json')
                                    with open(diarization_filename, 'w') as df:
                                        json.dump(diarization_result, df)
                                    print(f"✓ Diarization service (URL fallback) returned results")
                                else:
                                    try:
                                        diarization_result = resp2.json()
                                    except Exception:
                                        diarization_result = {'error': f'Diarization service returned status {resp2.status_code}'}
                            except Exception as url_error:
                                diarization_result = {'error': f'URL fallback failed: {str(url_error)}'}
                        else:
                            diarization_result = {'error': f'File upload failed and no URL available: {str(e)}'}
                
                # Priority 2: Send URL if no local video
                elif video_url:
                    try:
                        diarization_data = {'video_url': video_url}
                        print(f"→ Sending video URL to diarization service...")
                        resp2 = requests.post(diarization_url, json=diarization_data, timeout=120)
                        if resp2.ok:
                            diarization_result = resp2.json()
                            diarization_filename = os.path.join(DATA_DIR, f'diarization_{session_id}.json')
                            with open(diarization_filename, 'w') as df:
                                json.dump(diarization_result, df)
                            print(f"✓ Diarization service returned results")
                        else:
                            try:
                                diarization_result = resp2.json()
                            except Exception:
                                diarization_result = {'error': f'Diarization service returned status {resp2.status_code}'}
                            print(f"⚠ Diarization service error: {resp2.status_code}")
                    except Exception as e:
                        diarization_result = {'error': f'Failed to call diarization service: {str(e)}'}
        except Exception as e:
            diarization_result = {'error': f'Failed to call diarization service: {str(e)}'}
        
        # Clean up local video file after processing (optional - keep for debugging)
        try:
            if local_video_path and os.path.exists(local_video_path):
                # Uncomment to auto-delete after processing:
                # os.remove(local_video_path)
                print(f"✓ Local video kept for reference: {local_video_path}")
        except Exception as e:
            print(f"⚠ Could not clean up video: {e}")

        # Attach analysis and diarization results to session
        if analysis_filename:
            new_session['analysisFile'] = os.path.basename(analysis_filename)
            new_session['analysis'] = analysis_result
        else:
            new_session['analysis'] = analysis_result

        if diarization_filename:
            new_session['diarizationFile'] = os.path.basename(diarization_filename)
            new_session['diarization'] = diarization_result
        else:
            new_session['diarization'] = diarization_result

        # Enrich session document using analysis & diarization results
        try:
            # Build metrics list from analysis_result with proper schema compliance
            metrics = []
            if isinstance(analysis_result, dict):
                key_map = {
                    'clarity': 'Clarity',
                    'communication': 'Communication',
                    'engagement': 'Engagement',
                    'technical_depth': 'Technical Depth',
                    'interaction': 'Interaction',
                    'pacing': 'Pacing',
                    'eye_contact': 'Eye Contact',
                    'gestures': 'Gestures'
                }
                for k, label in key_map.items():
                    val = analysis_result.get(k)
                    score = None
                    if isinstance(val, dict):
                        score = val.get('score')
                    elif isinstance(val, (int, float)):
                        score = val
                    if score is not None:
                        try:
                            score = int(float(score))
                            score = max(0, min(100, score))
                            metrics.append({
                                'name': label,
                                'score': score,
                                'confidenceInterval': [max(0, score - 5), min(100, score + 5)],
                                'whatHelped': [],
                                'whatHurt': []
                            })
                        except Exception:
                            pass

                # Overall score
                overall = analysis_result.get('overall_score') or analysis_result.get('overallScore')
                if overall is not None:
                    try:
                        overall_score = int(float(overall))
                        overall_score = max(0, min(100, overall_score))
                        metrics.append({
                            'name': 'Overall',
                            'score': overall_score,
                            'confidenceInterval': [max(0, overall_score - 5), min(100, overall_score + 5)],
                            'whatHelped': [],
                            'whatHurt': []
                        })
                    except Exception:
                        pass

            # Build weakMoments from diarization (sentences flagged for improvement)
            weak_moments = []
            timeline_transcript = []
            try:
                sentences = []
                if isinstance(diarization_result, dict):
                    sentences = diarization_result.get('sentences') or []

                def _format_hms(sec):
                    sec = int(sec or 0)
                    h = sec // 3600
                    m = (sec % 3600) // 60
                    s = sec % 60
                    return f"{h:02d}:{m:02d}:{s:02d}"

                for s in sentences:
                    start = s.get('start', 0)
                    end = s.get('end', 0)
                    text = s.get('text') or s.get('transcript') or ''
                    timeline_transcript.append({
                        'startTime': float(start),
                        'endTime': float(end),
                        'text': text,
                        'keyPhrases': []
                    })

                    needs = s.get('needs_improvement') or s.get('needsImprovement') or False
                    if needs:
                        imp = s.get('improvement') or s.get('improvement', {})
                        msg = ''
                        if isinstance(imp, dict):
                            msg = imp.get('suggestion') or imp.get('reason') or ''
                        elif isinstance(imp, str):
                            msg = imp
                        if not msg:
                            msg = text[:200]

                        weak_moments.append({
                            'timestamp': _format_hms(start),
                            'message': msg
                        })
            except Exception:
                weak_moments = []
                timeline_transcript = []

            # Add timeline and metrics to session
            new_session['metrics'] = metrics
            new_session['timeline'] = {
                'audio': [],
                'video': [],
                'transcript': timeline_transcript,
                'scoreDips': [],
                'scorePeaks': []
            }
            new_session['weakMoments'] = weak_moments

            # Use Gemini to produce an AI summary from transcript
            ai_summary = None
            try:
                if os.getenv('GEMINI_API_KEY'):
                    try:
                        from google import genai
                        client = genai.Client(api_key=os.getenv('GEMINI_API_KEY'))
                        transcript_text = ''
                        if isinstance(analysis_result, dict):
                            transcript_text = analysis_result.get('transcript') or ''
                        if not transcript_text and timeline_transcript:
                            transcript_text = ' '.join([t['text'] for t in timeline_transcript[:50]])

                        if transcript_text:
                            prompt = f"Summarize the following session transcript in 2 concise sentences and give 3 short improvement suggestions:\n\n{transcript_text[:3000]}"
                            response = client.models.generate_content(
                                model='gemini-2.5-flash',
                                contents=prompt
                            )
                            ai_summary = response.text if hasattr(response, 'text') else None
                    except Exception:
                        ai_summary = None
            except Exception:
                ai_summary = None

            if ai_summary:
                new_session['aiSummary'] = ai_summary

            # Save to DB
            try:
                saved = Session.create_session(new_session)
            except Exception:
                saved = None
        except Exception as e:
            try:
                saved = Session.create_session(new_session)
            except Exception:
                saved = None

        # Also keep file-based list for backward compatibility
        try:
            uploaded_sessions = load_uploaded_sessions()
            sessions_list = uploaded_sessions.get('sessions', [])
            
            session_summary = {
                'id': session_id,
                'sessionId': session_id,
                'sessionName': session_name,
                'created_at': new_session.get('created_at', datetime.utcnow()).isoformat(),
                'weakMoments': new_session.get('weakMoments', []),
                'uploadedFile': new_session.get('uploadedFile'),
                'mentorId': mentor_id,
                'userId': user_id
            }
            if new_session.get('metrics'):
                overall_metrics = [m for m in new_session['metrics'] if m.get('name') == 'Overall']
                if overall_metrics:
                    session_summary['score'] = overall_metrics[0].get('score', 0)
                else:
                    avg_score = sum(m.get('score', 0) for m in new_session['metrics']) / len(new_session['metrics']) if new_session['metrics'] else 0
                    session_summary['score'] = int(avg_score)
            
            sessions_list.insert(0, session_summary)
            uploaded_sessions['sessions'] = sessions_list
            save_uploaded_sessions(uploaded_sessions)
        except Exception:
            pass

        # Ensure response session has an 'id' field for frontend compatibility
        response_session = saved or new_session
        try:
            response_session['id'] = response_session.get('sessionId') or response_session.get('_id')
        except Exception:
            pass

        response_payload = {
            'message': 'Video analysis started from URL.',
            'session': response_session,
            'analysis': analysis_result,
            'diarization': diarization_result
        }

        return jsonify(response_payload), 201
    except Exception as e:
        return jsonify({'error': f'Failed to analyze video: {str(e)}'}), 500

@app.route('/api/public/mentors/rankings', methods=['GET'])
def get_public_rankings():
    """Public leaderboard with compact filters; returns normalized scores only."""
    try:
        data = load_public_rankings()
        rankings = data.get('rankings', [])

        subject = request.args.get('subject')
        language = request.args.get('language')
        experience = request.args.get('experience')
        window = request.args.get('window')

        def matches(item):
            return (
                (not subject or item.get('subject') == subject) and
                (not language or item.get('language') == language) and
                (not experience or item.get('experienceLevel') == experience) and
                (not window or item.get('timeWindow') == window)
            )

        filtered = [r for r in rankings if matches(r)]

        # Remove any raw fields if present; keep normalized view only
        sanitized = [
            {
                'id': r.get('id'),
                'rank': r.get('rank'),
                'name': r.get('name'),
                'verified': r.get('verified', False),
                'overallScore': r.get('overallScore'),
                'strengthTag': r.get('strengthTag'),
                'avgScoreTrend': r.get('avgScoreTrend', []),
            }
            for r in filtered
        ]

        return jsonify({
            'filters': data.get('filters', {}),
            'rankings': sanitized
        }), 200
    except Exception as e:
        return jsonify({'error': f'Failed to load public rankings: {str(e)}'}), 500


@app.route('/api/mentor/<mentor_id>/sessions/<session_id>/video', methods=['GET'])
def serve_session_video(mentor_id, session_id):
    """Redirect to the Cloudinary-hosted video for a session."""
    try:
        session = Session.find_by_sessionId(session_id)
        if not session:
            return jsonify({'error': 'Session not found'}), 404

        # Get video URL from Cloudinary
        video_url = session.get('videoUrl')
        if not video_url:
            return jsonify({'error': 'No video attached to this session'}), 404

        # Redirect to Cloudinary URL (or return as JSON for frontend to embed)
        # Using redirect for direct browser access
        return redirect(video_url)
    except Exception as e:
        return jsonify({'error': f'Failed to serve video: {str(e)}'}), 500

@app.route('/api/public/mentors/<mentor_id>', methods=['GET'])
def get_public_mentor_profile(mentor_id):
    """Public mentor profile with only strengths and highlights."""
    try:
        data = load_public_rankings()
        profile = data.get('profiles', {}).get(mentor_id)

        if not profile:
            return jsonify({'error': 'Mentor not found'}), 404

        public_profile = {
            'id': profile.get('id'),
            'name': profile.get('name'),
            'verified': profile.get('verified', False),
            'bio': profile.get('bio'),
            'expertise': profile.get('expertise', []),
            'strengthTag': profile.get('strengthTag'),
            'avgScoreTrend': profile.get('avgScoreTrend', []),
            'peerBadges': profile.get('peerBadges', []),
            'teachingHighlights': profile.get('teachingHighlights', []),
            'contact': profile.get('contact', {})
        }

        return jsonify(public_profile), 200
    except Exception as e:
        return jsonify({'error': f'Failed to load mentor profile: {str(e)}'}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'ok'}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True, port=5000)

