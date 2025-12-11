"""
MongoDB GridFS Integration for Video Storage
Use this module to handle video file storage in MongoDB instead of local filesystem
"""

from gridfs import GridFS
from bson.objectid import ObjectId
from datetime import datetime
import os

# Initialize GridFS (call after db initialization in models.py)
def init_gridfs(db):
    """Initialize GridFS for the database"""
    return GridFS(db)


def upload_video_to_gridfs(fs, file_obj, filename, metadata=None):
    """
    Upload a video file to GridFS
    
    Args:
        fs: GridFS instance
        file_obj: File object or bytes
        filename: Original filename
        metadata: Dict with metadata (e.g., mentorId, sessionId)
    
    Returns:
        str: GridFS file ID
    """
    try:
        file_id = fs.put(
            file_obj,
            filename=filename,
            upload_date=datetime.utcnow(),
            metadata=metadata or {},
            content_type='video/mp4'
        )
        return str(file_id)
    except Exception as e:
        raise Exception(f'GridFS upload failed: {str(e)}')


def get_video_from_gridfs(fs, file_id):
    """
    Retrieve a video file from GridFS
    
    Args:
        fs: GridFS instance
        file_id: ObjectId as string
    
    Returns:
        GridOut object (file-like)
    """
    try:
        return fs.get(ObjectId(file_id))
    except Exception as e:
        raise Exception(f'GridFS retrieval failed: {str(e)}')


def delete_video_from_gridfs(fs, file_id):
    """
    Delete a video file from GridFS
    
    Args:
        fs: GridFS instance
        file_id: ObjectId as string
    """
    try:
        fs.delete(ObjectId(file_id))
    except Exception as e:
        raise Exception(f'GridFS deletion failed: {str(e)}')


def get_video_metadata(fs, file_id):
    """
    Get metadata for a video file
    
    Args:
        fs: GridFS instance
        file_id: ObjectId as string
    
    Returns:
        dict: File metadata
    """
    try:
        grid_out = fs.get(ObjectId(file_id))
        return {
            'filename': grid_out.filename,
            'upload_date': grid_out.upload_date,
            'metadata': grid_out.metadata,
            'length': grid_out.length,
            'content_type': grid_out.content_type
        }
    except Exception as e:
        raise Exception(f'Failed to get metadata: {str(e)}')


def list_videos_by_mentor(db, mentor_id):
    """
    List all videos uploaded by a mentor
    
    Args:
        db: Database instance
        mentor_id: Mentor ID
    
    Returns:
        list: Sessions with video information
    """
    try:
        sessions = db.sessions.find({
            'mentorId': mentor_id,
            'videoFileId': {'$exists': True}
        })
        return list(sessions)
    except Exception as e:
        raise Exception(f'Failed to list videos: {str(e)}')
