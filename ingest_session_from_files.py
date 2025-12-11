"""Utility: ingest analysis + diarization JSON files and create a session in MongoDB.
Usage:
    python ingest_session_from_files.py --analysis data/analysis_session_xxx.json --diarization data/diarization_session_xxx.json --mentor mentor_id --user user_id --video uploads/<file>
"""
import argparse
import json
import os
import uuid
from datetime import datetime

from models import Session

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')


def format_hms(sec):
    sec = int(sec or 0)
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def build_session(analysis, diarization, mentor_id, user_id, video_filename=None, session_name=None):
    session_id = f"session_{uuid.uuid4().hex[:8]}"
    s = {
        'sessionId': session_id,
        'sessionName': session_name or (analysis.get('video_id') if isinstance(analysis, dict) else 'Session'),
        'date': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'score': float(analysis.get('overall_score', 0)) if isinstance(analysis, dict) else 0,
        'weakMoments': [],
        'studentCount': 0,
        'uploadedFile': os.path.basename(video_filename) if video_filename else None,
        'mentorId': mentor_id,
        'userId': user_id,
    }

    # metrics
    metrics = []
    if isinstance(analysis, dict):
        key_map = {
            'clarity': 'Clarity',
            'communication': 'Communication',
            'engagement': 'Engagement',
            'technical_depth': 'Technical Depth',
            'interaction': 'Interaction'
        }
        for k, label in key_map.items():
            val = analysis.get(k)
            score = None
            if isinstance(val, dict):
                score = val.get('score')
            elif isinstance(val, (int, float)):
                score = val
            if score is not None:
                score = float(score)
                metrics.append({
                    'name': label,
                    'score': score,
                    'confidenceInterval': [max(0, score - 5), min(100, score + 5)],
                    'whatHelped': [],
                    'whatHurt': []
                })
        overall = analysis.get('overall_score') or analysis.get('overallScore')
        if overall is not None:
            metrics.append({
                'name': 'Overall',
                'score': float(overall),
                'confidenceInterval': [max(0, float(overall) - 5), min(100, float(overall) + 5)],
                'whatHelped': [],
                'whatHurt': []
            })

    s['metrics'] = metrics

    # diarization -> transcript and weak moments
    timeline_transcript = []
    weak_moments = []
    if isinstance(diarization, dict):
        sentences = diarization.get('sentences') or []
        for seg in sentences:
            start = seg.get('start', 0)
            end = seg.get('end', 0)
            text = seg.get('text') or seg.get('transcript') or ''
            timeline_transcript.append({'startTime': float(start), 'endTime': float(end), 'text': text, 'keyPhrases': []})
            needs = seg.get('needs_improvement') or seg.get('needsImprovement') or False
            if needs:
                imp = seg.get('improvement') or {}
                msg = imp.get('suggestion') if isinstance(imp, dict) else (imp or text[:200])
                weak_moments.append({'timestamp': format_hms(start), 'message': msg})

    s['timeline'] = {'audio': [], 'video': [], 'transcript': timeline_transcript, 'scoreDips': [], 'scorePeaks': []}
    s['weakMoments'] = weak_moments

    # Build audio/video segments heuristically from transcript if analysis doesn't provide per-segment timelines
    audio_segments = []
    video_segments = []
    score_dips = []
    score_peaks = []

    # Determine approximate duration from diarization or analysis
    total_duration = 0
    if timeline_transcript:
        total_duration = max((seg['endTime'] for seg in timeline_transcript), default=0)
    # fallback to analysis duration fields if present
    if not total_duration and isinstance(analysis, dict):
        # some analysis outputs include processing timestamps
        total_duration = analysis.get('duration') or analysis.get('total_duration') or 0

    for seg in timeline_transcript:
        start = float(seg.get('startTime', 0))
        end = float(seg.get('endTime', 0))
        dur = max(0.001, end - start)
        text = seg.get('text', '')
        words = len(text.split())
        # words per minute
        pace = int((words / dur) * 60) if dur > 0 and words > 0 else 0
        pauses = 0

        # classify pace type
        if pace >= 180:
            a_type = 'fast'
        elif pace >= 160:
            a_type = 'moderate'
        elif pace >= 120:
            a_type = 'normal'
        else:
            a_type = 'poor'

        audio_segments.append({
            'startTime': start,
            'endTime': end,
            'pace': pace,
            'pauses': pauses,
            'type': a_type,
            'message': ''
        })

        # video segment heuristics: derive eyeContact and gestures from presence of keyPhrases
        key_phrases = seg.get('keyPhrases') or []
        eye_contact = 80 + min(15, len(key_phrases) * 2)
        gestures = 5 + min(20, len(key_phrases))
        # classify video quality
        if eye_contact >= 90:
            v_type = 'excellent'
        elif eye_contact >= 80:
            v_type = 'good'
        elif eye_contact >= 60:
            v_type = 'moderate'
        else:
            v_type = 'poor'

        video_segments.append({
            'startTime': start,
            'endTime': end,
            'eyeContact': float(eye_contact),
            'gestures': int(gestures),
            'type': v_type,
            'message': ''
        })

    # Create score events based on audio pace outliers and weak moments
    for seg in audio_segments:
        # mark dips where pace too high or too low
        if seg['pace'] >= 180 or seg['pace'] < 120:
            score_dips.append({'timestamp': seg['startTime'], 'score': max(0, 70 - (abs(150 - seg['pace']) // 2)), 'message': f'Unusual pacing: {seg["pace"]} wpm', 'type': 'audio'})
    for wm in weak_moments:
        # weak moments recorded as timestamps in HH:MM:SS; try to parse to seconds
        try:
            parts = wm.get('timestamp', '').split(':')
            if len(parts) == 3:
                secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            elif len(parts) == 2:
                secs = int(parts[0]) * 60 + int(parts[1])
            else:
                secs = 0
        except Exception:
            secs = 0
        score_dips.append({'timestamp': secs, 'score': 60, 'message': wm.get('message', ''), 'type': 'weakMoment'})

    # For peaks, choose up to 3 longest transcript segments as positive moments
    sorted_by_length = sorted(timeline_transcript, key=lambda x: (x.get('endTime', 0) - x.get('startTime', 0)), reverse=True)
    for peak_seg in sorted_by_length[:3]:
        ts = peak_seg.get('startTime', 0)
        score_peaks.append({'timestamp': ts, 'score': 90, 'message': 'Strong moment', 'type': 'overall'})

    # Assign built arrays to session
    s['timeline']['audio'] = audio_segments
    s['timeline']['video'] = video_segments
    s['timeline']['scoreDips'] = score_dips
    s['timeline']['scorePeaks'] = score_peaks

    # attach raw analysis/diarization
    s['analysis'] = analysis
    s['diarization'] = diarization

    # video URL
    if video_filename:
        s['videoUrl'] = f"/api/mentor/{mentor_id}/sessions/{session_id}/video"

    return s


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--analysis', required=True, help='Path to analysis JSON')
    parser.add_argument('--diarization', required=True, help='Path to diarization JSON')
    parser.add_argument('--mentor', required=True, help='Mentor id')
    parser.add_argument('--user', required=False, help='User id', default=None)
    parser.add_argument('--video', required=False, help='Video filename relative to uploads/', default=None)
    parser.add_argument('--name', required=False, help='Session name', default=None)

    args = parser.parse_args()

    with open(args.analysis, 'r') as f:
        analysis = json.load(f)
    with open(args.diarization, 'r') as f:
        diarization = json.load(f)

    session_doc = build_session(analysis, diarization, args.mentor, args.user, args.video, args.name)
    created = Session.create_session(session_doc)
    print('Inserted session:', created.get('sessionId') or created.get('_id'))
    print('Use video endpoint at:', created.get('videoUrl'))
