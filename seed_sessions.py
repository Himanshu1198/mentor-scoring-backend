#!/usr/bin/env python3
"""Seed sessions from data/session_breakdown.json into MongoDB.

Usage:
  python backend/seed_sessions.py [--mentor-email EMAIL] [--update]

Options:
  --mentor-email EMAIL    Mentor user email to assign sessions to (default: mentor@example.com)
  --update                If provided, existing sessions will be updated instead of skipped

This script uses the existing `models` module (which wraps pymongo connections and User/Session helpers).
"""
import os
import json
import argparse
from pathlib import Path

# adjust path to import models if running from repo root
from models import User, Session

DATA_FILE = Path(__file__).resolve().parent / 'data' / 'session_breakdown.json'


def load_sessions_from_file(path: Path):
    with open(path, 'r') as f:
        data = json.load(f)
    # data is a mapping from sessionId -> session object
    return data


def main():
    parser = argparse.ArgumentParser(description='Seed sessions into MongoDB')
    parser.add_argument('--mentor-email', default='mentor@example.com', help='Mentor email to assign sessions to')
    parser.add_argument('--update', action='store_true', help='Update existing sessions instead of skipping')
    args = parser.parse_args()

    # Find mentor user
    mentor = User.find_by_email(args.mentor_email)
    if not mentor:
        print(f"Mentor with email {args.mentor_email} not found. Run db_setup.py to seed default users or create the user first.")
        return

    mentor_id = mentor.get('_id')
    print(f"Using mentor: {mentor.get('name')} ({mentor.get('email')}) id={mentor_id}")

    # Load sessions
    if not DATA_FILE.exists():
        print(f"Data file not found: {DATA_FILE}")
        return

    sessions_map = load_sessions_from_file(DATA_FILE)
    total = 0
    inserted = 0
    updated = 0
    skipped = 0

    for session_key, session_obj in sessions_map.items():
        total += 1
        session_doc = {
            'sessionId': session_obj.get('sessionId') or session_key,
            'sessionName': session_obj.get('sessionName'),
            'videoUrl': session_obj.get('videoUrl'),
            'duration': session_obj.get('duration'),
            'timeline': session_obj.get('timeline', {}),
            'metrics': session_obj.get('metrics', []),
            'mentorId': mentor_id,
            # optionally set userId to mentor as well for now
            'userId': mentor_id,
        }

        existing = Session.find_by_sessionId(session_doc['sessionId'])
        if existing:
            if args.update:
                print(f"Updating existing session {session_doc['sessionId']}")
                Session.update_session(session_doc['sessionId'], session_doc)
                updated += 1
            else:
                print(f"Skipping existing session {session_doc['sessionId']} (use --update to overwrite)")
                skipped += 1
            continue

        try:
            Session.create_session(session_doc)
            print(f"Inserted session {session_doc['sessionId']}")
            inserted += 1
        except Exception as e:
            print(f"Failed to insert session {session_doc['sessionId']}: {e}")

    print('\nDone')
    print(f"Total: {total}, Inserted: {inserted}, Updated: {updated}, Skipped: {skipped}")


if __name__ == '__main__':
    main()
