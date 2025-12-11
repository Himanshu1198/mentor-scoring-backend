#!/usr/bin/env python3
"""Backfill timeline/audio/video/score arrays for sessions missing them.
Scans the `sessions` collection for documents where timeline.audio or timeline.video are empty
and attempts to rebuild them using existing analysis/diarization JSON files in `data/`.

Usage:
  python backfill_timelines.py
"""
import os
import json
from bson import json_util
from models import sessions_collection, Session
from ingest_session_from_files import build_session

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, 'data')


def find_sessions_to_fix():
    # sessions where timeline missing or audio/video empty
    query = {
        '$or': [
            {'timeline': {'$exists': False}},
            {'timeline.audio': {'$exists': False}},
            {'timeline.video': {'$exists': False}},
            {'timeline.audio': {'$size': 0}},
            {'timeline.video': {'$size': 0}}
        ]
    }
    return list(sessions_collection.find(query))


def load_json_file(path):
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except Exception:
        return None


def backfill():
    sessions = find_sessions_to_fix()
    print(f'Found {len(sessions)} sessions to inspect')
    for s in sessions:
        sid = s.get('sessionId')
        if not sid:
            print('Skipping session without sessionId:', s.get('_id'))
            continue

        print('Processing', sid)

        # try to find analysis/diarization filenames recorded in doc
        analysis_file = s.get('analysisFile') or f'analysis_{sid}.json'
        diarization_file = s.get('diarizationFile') or f'diarization_{sid}.json'

        analysis_path = os.path.join(DATA_DIR, analysis_file) if not analysis_file.startswith('/') else analysis_file
        diarization_path = os.path.join(DATA_DIR, diarization_file) if not diarization_file.startswith('/') else diarization_file

        analysis = load_json_file(analysis_path)
        diarization = load_json_file(diarization_path)

        if not analysis and not diarization:
            print('  No analysis/diarization files found for', sid)
            continue

        # Build a candidate session (non-inserting) and pick the fields we want to update
        built = build_session(analysis or {}, diarization or {}, s.get('mentorId'), s.get('userId'), video_filename=s.get('uploadedFile'), session_name=s.get('sessionName'))

        update_data = {
            'timeline': built.get('timeline'),
            'metrics': built.get('metrics'),
            'weakMoments': built.get('weakMoments'),
            'analysis': built.get('analysis') or s.get('analysis'),
            'diarization': built.get('diarization') or s.get('diarization'),
            'videoUrl': built.get('videoUrl') or s.get('videoUrl')
        }

        try:
            res = Session.update_session(sid, update_data)
            print('  Updated', sid)
        except Exception as e:
            print('  Failed to update', sid, str(e))


if __name__ == '__main__':
    backfill()
