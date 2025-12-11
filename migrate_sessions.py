#!/usr/bin/env python3
"""
Migration script to update existing sessions to the new schema.
This script can be run to ensure all sessions in the database conform to the new structure.

Usage:
    python migrate_sessions.py [--backup] [--limit 100]
    
Options:
    --backup    Create backup of old sessions before migration
    --limit N   Only migrate first N sessions
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from models import Session, sessions_collection
import argparse
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
MONGODB_DB_NAME = os.getenv('MONGODB_DB_NAME', 'mentor_scoring')

# Initialize MongoDB connection
client = MongoClient(MONGODB_URI)
db = client[MONGODB_DB_NAME]
sessions_coll = db['sessions']


def backup_sessions():
    """Create a backup of all sessions before migration."""
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    backup_file = f'sessions_backup_{timestamp}.json'
    
    print(f"Creating backup: {backup_file}")
    
    try:
        sessions = list(sessions_coll.find({}))
        
        # Convert ObjectId to string for JSON serialization
        for s in sessions:
            if '_id' in s:
                s['_id'] = str(s['_id'])
            if 'created_at' in s and isinstance(s['created_at'], datetime):
                s['created_at'] = s['created_at'].isoformat()
            if 'updated_at' in s and isinstance(s['updated_at'], datetime):
                s['updated_at'] = s['updated_at'].isoformat()
        
        with open(backup_file, 'w') as f:
            json.dump({'sessions': sessions, 'timestamp': timestamp}, f, indent=2)
        
        print(f"✓ Backup created: {backup_file} ({len(sessions)} sessions)")
        return backup_file
        
    except Exception as e:
        print(f"✗ Error creating backup: {str(e)}")
        return None


def migrate_session(session_id=None, session_doc=None):
    """
    Migrate a single session to the new schema.
    
    Args:
        session_id: MongoDB _id of session to migrate
        session_doc: Session document to migrate
        
    Returns:
        dict: Migrated session or None on error
    """
    try:
        if session_id and not session_doc:
            from bson.objectid import ObjectId
            session_doc = sessions_coll.find_one({'_id': ObjectId(session_id)})
        
        if not session_doc:
            return None
        
        # Prepare and normalize the session
        prepared = Session.prepare_for_insert(session_doc.copy())
        
        # Fill missing fields using Gemini
        enriched = Session.fill_missing_fields_with_gemini(prepared)
        
        # Update timestamps
        enriched['updated_at'] = datetime.utcnow()
        
        # Update in database
        result = sessions_coll.find_one_and_update(
            {'sessionId': session_doc.get('sessionId')},
            {'$set': enriched},
            return_document=True
        )
        
        return result
        
    except Exception as e:
        print(f"✗ Error migrating session: {str(e)}")
        return None


def migrate_all_sessions(limit=None, use_backup=True):
    """
    Migrate all sessions in the database to the new schema.
    
    Args:
        limit: Maximum number of sessions to migrate
        use_backup: Whether to create backup before migration
        
    Returns:
        dict: Migration statistics
    """
    stats = {
        'total': 0,
        'migrated': 0,
        'failed': 0,
        'skipped': 0
    }
    
    # Create backup if requested
    if use_backup:
        backup_file = backup_sessions()
        if not backup_file:
            print("Backup failed. Aborting migration.")
            return stats
    
    print("\n" + "="*80)
    print("MIGRATING SESSIONS TO NEW SCHEMA")
    print("="*80)
    
    try:
        # Get all sessions
        query = sessions_coll.find({})
        if limit:
            query = query.limit(limit)
        
        sessions = list(query)
        stats['total'] = len(sessions)
        
        print(f"\nFound {stats['total']} sessions to migrate")
        
        for idx, session_doc in enumerate(sessions, 1):
            session_id = session_doc.get('sessionId', session_doc.get('_id', 'unknown'))
            print(f"\n[{idx}/{stats['total']}] Migrating {session_id}...", end=' ')
            
            try:
                # Check if already migrated
                if _is_already_migrated(session_doc):
                    print("already migrated ✓")
                    stats['skipped'] += 1
                    continue
                
                # Migrate the session
                result = migrate_session(session_doc=session_doc)
                
                if result:
                    print("migrated ✓")
                    stats['migrated'] += 1
                else:
                    print("failed ✗")
                    stats['failed'] += 1
                    
            except Exception as e:
                print(f"error: {str(e)} ✗")
                stats['failed'] += 1
        
        print("\n" + "="*80)
        print("MIGRATION COMPLETE")
        print("="*80)
        print(f"\nStatistics:")
        print(f"  Total sessions: {stats['total']}")
        print(f"  Migrated: {stats['migrated']}")
        print(f"  Skipped (already migrated): {stats['skipped']}")
        print(f"  Failed: {stats['failed']}")
        
        if stats['failed'] > 0:
            print(f"\n⚠ {stats['failed']} sessions failed to migrate")
        else:
            print(f"\n✓ All sessions successfully migrated!")
        
        return stats
        
    except Exception as e:
        print(f"\n✗ Migration error: {str(e)}")
        return stats


def _is_already_migrated(session_doc):
    """Check if a session has already been migrated to the new schema."""
    timeline = session_doc.get('timeline', {})
    
    # New schema always has these 5 arrays in timeline
    has_audio = isinstance(timeline.get('audio'), list)
    has_video = isinstance(timeline.get('video'), list)
    has_transcript = isinstance(timeline.get('transcript'), list)
    has_dips = isinstance(timeline.get('scoreDips'), list)
    has_peaks = isinstance(timeline.get('scorePeaks'), list)
    
    # Must have metrics
    has_metrics = isinstance(session_doc.get('metrics'), list)
    
    # New schema should have proper structure
    is_migrated = (has_audio and has_video and has_transcript and 
                   has_dips and has_peaks and has_metrics)
    
    # Additional check: at least one timeline array should have data
    has_data = (len(timeline.get('audio', [])) > 0 or
                len(timeline.get('video', [])) > 0 or
                len(timeline.get('transcript', [])) > 0 or
                len(session_doc.get('metrics', [])) > 0)
    
    return is_migrated and has_data


def validate_migrated_session(session_id):
    """Validate a migrated session has all required fields."""
    from bson.objectid import ObjectId
    
    try:
        session = sessions_coll.find_one({'sessionId': session_id})
        
        if not session:
            print(f"✗ Session not found: {session_id}")
            return False
        
        print(f"\nValidating session: {session_id}")
        print("-" * 60)
        
        required_fields = {
            'sessionId': str,
            'sessionName': str,
            'videoUrl': str,
            'duration': int,
            'mentorId': str,
            'timeline': dict,
            'metrics': list,
            'weakMoments': list,
        }
        
        timeline_arrays = {
            'audio': list,
            'video': list,
            'transcript': list,
            'scoreDips': list,
            'scorePeaks': list,
        }
        
        all_valid = True
        
        # Check required fields
        for field, expected_type in required_fields.items():
            value = session.get(field)
            is_valid = value is not None and (
                expected_type == type(value) if expected_type != int else isinstance(value, int)
            )
            status = "✓" if is_valid else "✗"
            print(f"{status} {field}: {type(value).__name__}")
            if not is_valid:
                all_valid = False
        
        # Check timeline arrays
        timeline = session.get('timeline', {})
        print("\nTimeline arrays:")
        for array_name, expected_type in timeline_arrays.items():
            array = timeline.get(array_name, [])
            is_valid = isinstance(array, expected_type)
            status = "✓" if is_valid else "✗"
            count = len(array) if isinstance(array, (list, dict)) else 0
            print(f"{status} {array_name}: {count} items")
            if not is_valid:
                all_valid = False
        
        # Check metrics
        metrics = session.get('metrics', [])
        print(f"\nMetrics: {len(metrics)} defined")
        metric_names = {m.get('name') for m in metrics if isinstance(m, dict)}
        expected_metrics = {'Clarity', 'Engagement', 'Pacing', 'Eye Contact', 'Gestures', 'Overall'}
        for metric_name in expected_metrics:
            status = "✓" if metric_name in metric_names else "✗"
            print(f"  {status} {metric_name}")
        
        print("-" * 60)
        if all_valid:
            print("✓ Session validation passed")
        else:
            print("✗ Session validation failed")
        
        return all_valid
        
    except Exception as e:
        print(f"✗ Validation error: {str(e)}")
        return False


def main():
    """Parse arguments and run migration."""
    parser = argparse.ArgumentParser(
        description='Migrate sessions to new schema',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_sessions.py                 # Migrate all sessions with backup
  python migrate_sessions.py --limit 10      # Migrate first 10 sessions
  python migrate_sessions.py --no-backup     # Skip backup creation
  python migrate_sessions.py --validate session_001  # Validate a session
        """
    )
    
    parser.add_argument('--backup', action='store_true', default=True,
                        help='Create backup before migration (default: True)')
    parser.add_argument('--no-backup', dest='backup', action='store_false',
                        help='Skip backup creation')
    parser.add_argument('--limit', type=int, default=None,
                        help='Maximum number of sessions to migrate')
    parser.add_argument('--validate', type=str, default=None,
                        help='Validate a specific session instead of migrating')
    
    args = parser.parse_args()
    
    if args.validate:
        validate_migrated_session(args.validate)
    else:
        stats = migrate_all_sessions(limit=args.limit, use_backup=args.backup)
        
        if stats['failed'] > 0:
            sys.exit(1)


if __name__ == '__main__':
    main()
