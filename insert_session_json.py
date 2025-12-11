#!/usr/bin/env python3
"""Insert a session document (Mongo Extended JSON) into the sessions collection.

Usage:
  python backend/insert_session_json.py --file path/to/session.json [--download-video]

If --download-video is provided and the session JSON has a remote videoUrl, the script
will download the video to `uploads/` and update the session document to reference the
local file (and provide the streaming endpoint URL used by the app).
"""
import argparse
import json
import os
import uuid
import requests
from datetime import datetime

from bson import json_util
from models import Session

BASE_DIR = os.path.dirname(__file__)
UPLOADS = os.path.join(BASE_DIR, 'uploads')
os.makedirs(UPLOADS, exist_ok=True)


def download_video_to_uploads(url: str) -> str:
    """Download remote video to uploads folder and return filename."""
    local_name = f"{uuid.uuid4().hex}.mp4"
    out_path = os.path.join(UPLOADS, local_name)
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(out_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return local_name
    except Exception as e:
        if os.path.exists(out_path):
            os.remove(out_path)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', required=True, help='Path to session JSON (Mongo extended JSON is supported)')
    parser.add_argument('--download-video', action='store_true', help='If set, download remote videoUrl into uploads/')
    args = parser.parse_args()

    with open(args.file, 'r') as f:
        raw = f.read()

    # Use bson.json_util to parse MongoDB Extended JSON (handles $date/$numberInt etc.)
    session_doc = json_util.loads(raw)

    # If download flag and remote http(s) videoUrl exists, try to download
    video_url = session_doc.get('videoUrl')
    if args.download_video and isinstance(video_url, str) and video_url.startswith('http'):
        try:
            print('Downloading video...')
            local_filename = download_video_to_uploads(video_url)
            session_doc['uploadedFile'] = local_filename
            # set a local streaming endpoint that the frontend can use
            session_doc['videoUrl'] = f"/api/mentor/{session_doc.get('mentorId')}/sessions/{session_doc.get('sessionId')}/video"
            print('Downloaded to uploads/', local_filename)
        except Exception as e:
            print('Video download failed:', str(e))

    # Ensure required ids exist
    if 'sessionId' not in session_doc:
        session_doc['sessionId'] = f'session_{uuid.uuid4().hex[:8]}'

    # Convert any lingering datetimes if they are in bson types they should already be converted
    try:
        created = Session.create_session(session_doc)
        print('Inserted session:', created.get('sessionId') or created.get('_id'))
        print('Video URL (stored):', created.get('videoUrl') or created.get('uploadedFile'))
    except Exception as e:
        print('Failed to insert session:', str(e))


if __name__ == '__main__':
    main()
