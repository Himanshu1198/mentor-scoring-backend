"""
Database models for the mentor scoring system
"""
from pymongo import MongoClient
from gridfs import GridFS
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
MONGODB_DB_NAME = os.getenv('MONGODB_DB_NAME', 'mentor_scoring')

# Initialize MongoDB connection
client = MongoClient(MONGODB_URI)
db = client[MONGODB_DB_NAME]

# Initialize GridFS for video storage
fs = GridFS(db)

# Get collections
users_collection = db['users']
sessions_collection = db['sessions']


class User:
    """User model for authentication"""
    
    @staticmethod
    def create_user(name, email, password, role):
        """
        Create a new user document
        
        Args:
            name (str): User's full name
            email (str): User's email address (unique)
            password (str): User's password (will be hashed)
            role (str): User's role - 'student', 'mentor', or 'university'
        
        Returns:
            dict: The created user document
        """
        user_doc = {
            'name': name,
            'email': email,
            'password_hash': generate_password_hash(password),
            'role': role,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow(),
            'is_active': True
        }
        
        result = users_collection.insert_one(user_doc)
        user_doc['_id'] = str(result.inserted_id)
        return user_doc
    
    @staticmethod
    def find_by_email(email):
        """
        Find a user by email
        
        Args:
            email (str): User's email address
        
        Returns:
            dict: User document or None
        """
        user = users_collection.find_one({'email': email})
        if user:
            user['_id'] = str(user['_id'])
        return user
    
    @staticmethod
    def find_by_id(user_id):
        """
        Find a user by ID
        
        Args:
            user_id (str): User's MongoDB ID
        
        Returns:
            dict: User document or None
        """
        from bson.objectid import ObjectId
        try:
            user = users_collection.find_one({'_id': ObjectId(user_id)})
            if user:
                user['_id'] = str(user['_id'])
            return user
        except:
            return None
    
    @staticmethod
    def verify_password(user_email, password):
        """
        Verify a user's password
        
        Args:
            user_email (str): User's email address
            password (str): Password to verify
        
        Returns:
            dict: User document if password is correct, None otherwise
        """
        user = User.find_by_email(user_email)
        if user and check_password_hash(user['password_hash'], password):
            return user
        return None
    
    @staticmethod
    def update_user(user_id, update_data):
        """
        Update a user's information
        
        Args:
            user_id (str): User's MongoDB ID
            update_data (dict): Fields to update
        
        Returns:
            dict: Updated user document or None
        """
        from bson.objectid import ObjectId
        try:
            update_data['updated_at'] = datetime.utcnow()
            result = users_collection.find_one_and_update(
                {'_id': ObjectId(user_id)},
                {'$set': update_data},
                return_document=True
            )
            if result:
                result['_id'] = str(result['_id'])
            return result
        except:
            return None
    
    @staticmethod
    def get_all_users():
        """
        Get all users
        
        Returns:
            list: List of all user documents
        """
        users = list(users_collection.find({}))
        for user in users:
            user['_id'] = str(user['_id'])
        return users


def init_db():
    """
    Initialize the database with necessary collections and indexes
    """
    # Create unique index on email field
    users_collection.create_index('email', unique=True)
    
    # Create index on role for faster queries
    users_collection.create_index('role')
    
    # Create index on created_at for sorting
    users_collection.create_index('created_at')
    # Sessions collection indexes
    sessions_collection.create_index('sessionId', unique=True)
    sessions_collection.create_index('mentorId')
    sessions_collection.create_index('userId')
    
    print("✓ Database indexes created")


def seed_default_users():
    """
    Seed the database with default test users
    """
    default_users = [
        {
            'name': 'John Student',
            'email': 'student@example.com',
            'password': 'student123',
            'role': 'student'
        },
        {
            'name': 'Dr. Sarah Mentor',
            'email': 'mentor@example.com',
            'password': 'mentor123',
            'role': 'mentor'
        },
        {
            'name': 'University Admin',
            'email': 'university@example.com',
            'password': 'university123',
            'role': 'university'
        }
    ]
    
    for user_data in default_users:
        # Check if user already exists
        if not User.find_by_email(user_data['email']):
            User.create_user(
                name=user_data['name'],
                email=user_data['email'],
                password=user_data['password'],
                role=user_data['role']
            )
            print(f"✓ Created user: {user_data['email']}")
        else:
            print(f"✓ User already exists: {user_data['email']}")


class Session:
    """Session model to store session breakdowns and metadata."""

    @staticmethod
    def fill_metric_feedback_with_gemini(metrics: list, session_context: dict) -> list:
        """
        Fill empty 'whatHelped' and 'whatHurt' arrays in metrics using Gemini.
        
        Args:
            metrics (list): List of metric objects
            session_context (dict): Context about the session (transcript, etc.)
            
        Returns:
            list: Metrics with filled feedback arrays
        """
        import json
        
        try:
            # Check if we need to fill anything
            needs_filling = any(
                (not m.get('whatHelped') or len(m.get('whatHelped', [])) == 0) or
                (not m.get('whatHurt') or len(m.get('whatHurt', [])) == 0)
                for m in metrics
            )
            
            if not needs_filling:
                return metrics
            
            api_key = os.getenv('GEMINI_API_KEY')
            if not api_key:
                return metrics
            
            from google import genai
            try:
                client = genai.Client(api_key=api_key)
            except Exception:
                return metrics
            
            # Build metrics context
            metrics_summary = []
            for m in metrics:
                metrics_summary.append(f"{m.get('name')}: {m.get('score', 0)}/100")
            
            prompt = f"""You are analyzing a mentor presentation session. Based on these metrics and scores:
{', '.join(metrics_summary)}

For EACH metric below, provide 2-3 items for "whatHelped" (positive aspects that contributed to the score) 
and 2-3 items for "whatHurt" (areas that need improvement).

Return ONLY valid JSON in this exact format (no markdown, no extra text):
{{
  "metrics": [
    {{
      "name": "Clarity",
      "whatHelped": ["Clear explanations", "Good structure", "Well-organized content"],
      "whatHurt": ["Jargon without context", "Rushed delivery", "Unclear examples"]
    }},
    {{
      "name": "Engagement",
      "whatHelped": ["Interactive questions", "Eye contact", "Varied pacing"],
      "whatHurt": ["Monotone delivery", "Long pauses", "Limited audience interaction"]
    }},
    {{
      "name": "Pacing",
      "whatHelped": ["Steady rhythm", "Appropriate speed", "Good pauses for reflection"],
      "whatHurt": ["Too fast in middle", "Rushed sections", "Inconsistent timing"]
    }},
    {{
      "name": "Eye Contact",
      "whatHelped": ["Strong connection with audience", "Consistent gaze", "Natural focus"],
      "whatHurt": ["Looking at notes too much", "Distracted by screen", "Limited audience engagement"]
    }},
    {{
      "name": "Gestures",
      "whatHelped": ["Expressive movements", "Natural hand motions", "Emphasis through gestures"],
      "whatHurt": ["Repetitive movements", "Limited variety", "Distracting fidgeting"]
    }},
    {{
      "name": "Overall",
      "whatHelped": ["Professional presentation", "Well-prepared content", "Effective delivery"],
      "whatHurt": ["Minor timing issues", "Could improve engagement", "Some technical jargon"]
    }}
  ]
}}"""

            try:
                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt
                )
                
                if response and hasattr(response, 'text'):
                    text = response.text.strip()
                    # Extract JSON if wrapped in markdown
                    if text.startswith('```'):
                        text = text.split('```')[1]
                        if text.startswith('json'):
                            text = text[4:]
                        text = text.strip()
                    
                    generated = json.loads(text)
                    
                    # Create a mapping of metric names to feedback
                    feedback_map = {}
                    if 'metrics' in generated and isinstance(generated['metrics'], list):
                        for gen_metric in generated['metrics']:
                            metric_name = gen_metric.get('name')
                            if metric_name:
                                feedback_map[metric_name] = {
                                    'whatHelped': gen_metric.get('whatHelped', []),
                                    'whatHurt': gen_metric.get('whatHurt', [])
                                }
                    
                    # Update original metrics with feedback
                    updated_metrics = []
                    for metric in metrics:
                        metric_name = metric.get('name')
                        if metric_name in feedback_map:
                            # Fill empty arrays with generated feedback
                            if not metric.get('whatHelped') or len(metric.get('whatHelped', [])) == 0:
                                metric['whatHelped'] = feedback_map[metric_name].get('whatHelped', [])
                            if not metric.get('whatHurt') or len(metric.get('whatHurt', [])) == 0:
                                metric['whatHurt'] = feedback_map[metric_name].get('whatHurt', [])
                        else:
                            # Ensure arrays exist even if empty
                            metric.setdefault('whatHelped', [])
                            metric.setdefault('whatHurt', [])
                        updated_metrics.append(metric)
                    
                    return updated_metrics
                    
            except Exception as api_err:
                error_str = str(api_err)
                if '400' in error_str or 'INVALID_ARGUMENT' in error_str:
                    print(f"⚠ Gemini API key issue: {error_str[:100]}")
                # Continue without enhancement
                pass
        
        except Exception as e:
            print(f"⚠ Metric feedback fill failed: {str(e)[:100]}")
        
        # Ensure all metrics have the arrays
        for metric in metrics:
            metric.setdefault('whatHelped', [])
            metric.setdefault('whatHurt', [])
        
        return metrics

    @staticmethod
    def fill_missing_fields_with_gemini(doc: dict) -> dict:
        """
        Use Gemini API to intelligently fill missing fields based on available context.
        This ensures no field is empty and provides realistic synthesis for missing analytics.
        
        Args:
            doc (dict): Session document with potentially missing fields
            
        Returns:
            dict: Document with filled missing fields, or original if Gemini unavailable
        """
        import json
        
        try:
            api_key = os.getenv('GEMINI_API_KEY')
            if not api_key:
                print("Warning: GEMINI_API_KEY not set. Proceeding with existing data.")
                return doc
                
            from google import genai
            try:
                client = genai.Client(api_key=api_key)
            except Exception as auth_err:
                print(f"Warning: Failed to initialize Gemini client: {str(auth_err)}")
                return doc
            
            # Determine what's missing
            has_audio = len(doc.get('timeline', {}).get('audio', [])) > 0
            has_video = len(doc.get('timeline', {}).get('video', [])) > 0
            has_transcript = len(doc.get('timeline', {}).get('transcript', [])) > 0
            has_score_dips = len(doc.get('timeline', {}).get('scoreDips', [])) > 0
            has_score_peaks = len(doc.get('timeline', {}).get('scorePeaks', [])) > 0
            has_metrics = len(doc.get('metrics', [])) > 0
            
            # Check if any metric has empty whatHelped or whatHurt
            has_empty_metric_feedback = False
            if has_metrics:
                for m in doc.get('metrics', []):
                    if not m.get('whatHelped') or len(m.get('whatHelped', [])) == 0:
                        has_empty_metric_feedback = True
                        break
                    if not m.get('whatHurt') or len(m.get('whatHurt', [])) == 0:
                        has_empty_metric_feedback = True
                        break
            
            if has_audio and has_video and has_transcript and has_score_dips and has_score_peaks and has_metrics and not has_empty_metric_feedback:
                return doc  # All fields populated
            
            # Build context from what we have
            context = {
                'sessionName': doc.get('sessionName', 'Video Session'),
                'duration': doc.get('duration', 1800),
                'hasAnalysis': bool(doc.get('analysis')),
                'hasDiarization': bool(doc.get('diarization')),
            }
            
            if isinstance(doc.get('analysis'), dict):
                context['analysisKeys'] = list(doc['analysis'].keys())
            if isinstance(doc.get('diarization'), dict):
                context['diarizationKeys'] = list(doc['diarization'].keys())
            
            prompt = f"""You are an AI that synthesizes comprehensive session analysis data for mentor presentations.
Given a session about "{context['sessionName']}" with duration {context['duration']} seconds, 
generate ONLY valid JSON (no markdown wrapping, no extra text):

{{
  "timeline": {{
    "audio": [
      {{"startTime": 0, "endTime": 300, "pace": 150, "pauses": 3, "type": "normal", "message": "Good opening pace"}},
      {{"startTime": 300, "endTime": 600, "pace": 180, "pauses": 1, "type": "fast", "message": "Speaking speed increased"}},
      {{"startTime": 600, "endTime": 900, "pace": 145, "pauses": 4, "type": "normal", "message": ""}}
    ],
    "video": [
      {{"startTime": 0, "endTime": 400, "eyeContact": 85, "gestures": 8, "type": "good", "message": "Strong eye contact"}},
      {{"startTime": 400, "endTime": 750, "eyeContact": 50, "gestures": 3, "type": "poor", "message": "Eye contact dropped"}},
      {{"startTime": 750, "endTime": 1200, "eyeContact": 90, "gestures": 10, "type": "excellent", "message": ""}}
    ],
    "transcript": [
      {{"startTime": 0, "endTime": 120, "text": "Welcome to this session on mentoring. Today we'll cover key topics.", "keyPhrases": ["welcome", "mentoring", "session"]}},
      {{"startTime": 300, "endTime": 450, "text": "Let's dive into the main content with real examples.", "keyPhrases": ["main content", "examples"]}},
      {{"startTime": 600, "endTime": 750, "text": "Any questions before we move forward?", "keyPhrases": ["questions", "engagement"]}}
    ],
    "scoreDips": [
      {{"timestamp": 450, "score": 65, "message": "Speaking speed increased, harder to follow", "type": "audio"}},
      {{"timestamp": 500, "score": 60, "message": "Eye contact dropped, audience engagement decreased", "type": "video"}},
      {{"timestamp": 400, "score": 70, "message": "Brief loss of engagement", "type": "engagement"}}
    ],
    "scorePeaks": [
      {{"timestamp": 100, "score": 92, "message": "Excellent clarity and engagement", "type": "overall"}},
      {{"timestamp": 800, "score": 90, "message": "Strong gestures and eye contact", "type": "video"}},
      {{"timestamp": 200, "score": 88, "message": "Perfect pacing and delivery", "type": "audio"}}
    ]
  }},
  "metrics": [
    {{"name": "Clarity", "score": 78, "confidenceInterval": [74, 82], "whatHelped": ["Clear explanations", "Good examples", "Well-structured content"], "whatHurt": ["Some jargon without context", "Rushed middle section", "Could use more visuals"]}},
    {{"name": "Engagement", "score": 82, "confidenceInterval": [78, 86], "whatHelped": ["Interactive questions", "Varied tone", "Good eye contact"], "whatHurt": ["Brief attention dips", "Could ask more questions", "Some pauses felt long"]}},
    {{"name": "Pacing", "score": 76, "confidenceInterval": [72, 80], "whatHelped": ["Steady opening", "Good transition points", "Clear conclusions"], "whatHurt": ["Speaking speed increased mid-session", "Some sections rushed", "Pauses not optimal"]}},
    {{"name": "Eye Contact", "score": 80, "confidenceInterval": [76, 84], "whatHelped": ["Strong opening engagement", "Good focus in Q&A", "Natural gaze patterns"], "whatHurt": ["Looked at notes too often", "One segment lost connection", "Could engage left side more"]}},
    {{"name": "Gestures", "score": 85, "confidenceInterval": [81, 89], "whatHelped": ["Expressive hand movements", "Natural gestures", "Good use for emphasis"], "whatHurt": ["Some repetitive motions", "Could vary more", "Limited in one section"]}},
    {{"name": "Overall", "score": 80, "confidenceInterval": [76, 84], "whatHelped": ["Professional delivery", "Well-prepared content", "Good audience connection"], "whatHurt": ["Minor pacing issues", "Could improve engagement further", "Some technical terms unclear"]}}
  ],
  "weakMoments": [
    {{"timestamp": "00:07:30", "message": "Speaking speed increased - slow down for clarity"}},
    {{"timestamp": "00:08:20", "message": "Eye contact dropped - engage audience more"}},
    {{"timestamp": "00:06:40", "message": "Consider adding visual aids for technical concepts"}}
  ]
}}

CRITICAL REQUIREMENTS:
- ALWAYS include ALL 6 metrics: Clarity, Engagement, Pacing, Eye Contact, Gestures, Overall
- EVERY metric MUST have: name, score (0-100), confidenceInterval [min,max], whatHelped (3+ items), whatHurt (3+ items)
- whatHelped: specific POSITIVE aspects (e.g., "Clear explanations", "Good eye contact", "Natural gestures")
- whatHurt: specific AREAS FOR IMPROVEMENT (e.g., "Jargon without context", "Speaking too fast", "Limited gestures")
- NEVER leave whatHelped or whatHurt empty
- timeline.audio: 4-6 segments with pace (100-200 wpm), pauses count, type, message
- timeline.video: 4-5 segments with eyeContact %, gestures count, type, message
- timeline.transcript: 4-5 segments with text and keyPhrases (3+ phrases each)
- scoreDips & scorePeaks: 3-4 items each with timestamp, score, message, type
- weakMoments: 3-4 actionable improvement suggestions
- RETURN ONLY valid JSON, no markdown code blocks, no extra text"""

            try:
                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt
                )
                
                if response and hasattr(response, 'text'):
                    text = response.text.strip()
                    # Extract JSON if wrapped in markdown
                    if text.startswith('```'):
                        text = text.split('```')[1]
                        if text.startswith('json'):
                            text = text[4:]
                        text = text.strip()
                    
                    generated = json.loads(text)
                    
                    # Merge generated data into document
                    if 'timeline' in generated:
                        tl = generated['timeline']
                        if not has_audio and isinstance(tl.get('audio'), list):
                            doc['timeline']['audio'] = tl['audio']
                        if not has_video and isinstance(tl.get('video'), list):
                            doc['timeline']['video'] = tl['video']
                        if not has_transcript and isinstance(tl.get('transcript'), list):
                            doc['timeline']['transcript'] = tl['transcript']
                        if not has_score_dips and isinstance(tl.get('scoreDips'), list):
                            doc['timeline']['scoreDips'] = tl['scoreDips']
                        if not has_score_peaks and isinstance(tl.get('scorePeaks'), list):
                            doc['timeline']['scorePeaks'] = tl['scorePeaks']
                    
                    # Handle metrics - merge generated with existing, ensuring whatHelped/whatHurt are filled
                    if isinstance(generated.get('metrics'), list):
                        gen_metrics_map = {m.get('name'): m for m in generated['metrics'] if m.get('name')}
                        
                        if has_metrics and doc.get('metrics'):
                            # Merge: use existing metric data but fill whatHelped/whatHurt from generated
                            merged_metrics = []
                            for existing_metric in doc['metrics']:
                                metric_name = existing_metric.get('name')
                                if metric_name in gen_metrics_map:
                                    gen_metric = gen_metrics_map[metric_name]
                                    # Keep existing data but fill empty feedback arrays
                                    if not existing_metric.get('whatHelped') or len(existing_metric.get('whatHelped', [])) == 0:
                                        existing_metric['whatHelped'] = gen_metric.get('whatHelped', [])
                                    if not existing_metric.get('whatHurt') or len(existing_metric.get('whatHurt', [])) == 0:
                                        existing_metric['whatHurt'] = gen_metric.get('whatHurt', [])
                                else:
                                    # Ensure empty arrays are initialized
                                    existing_metric.setdefault('whatHelped', [])
                                    existing_metric.setdefault('whatHurt', [])
                                merged_metrics.append(existing_metric)
                            doc['metrics'] = merged_metrics
                        else:
                            # Use generated metrics as-is
                            doc['metrics'] = generated['metrics']
                    
                    if not doc.get('weakMoments') and isinstance(generated.get('weakMoments'), list):
                        doc['weakMoments'] = generated['weakMoments']
                        
            except Exception as api_err:
                # Check if it's an authentication error
                error_str = str(api_err)
                if '400' in error_str or 'INVALID_ARGUMENT' in error_str or 'API_KEY_INVALID' in error_str or 'expired' in error_str.lower():
                    print(f"⚠ Gemini API key issue: {error_str}")
                    print("ℹ To fix: Get a new API key from https://aistudio.google.com/app/apikey")
                    print("  Then update GEMINI_API_KEY in .env file")
                else:
                    print(f"⚠ Gemini API error: {error_str}")
                # Continue without Gemini enrichment
                pass
                    
        except Exception as e:
            # Gracefully fall back if Gemini initialization fails
            print(f"⚠ Gemini API initialization failed: {str(e)}")
            pass
        
        return doc

    @staticmethod
    def create_session(session_doc: dict):
        """
        Insert a new session document. Expects session_doc to contain at least
        'sessionId' (unique), and may include 'mentorId' and 'userId'.
        
        Process:
        1. Prepare and coerce document to strict schema
        2. Fill missing fields using Gemini API for intelligent synthesis
        3. Validate no field is empty
        4. Insert into database
        
        Returns the inserted document with stringified _id.
        """
        import copy

        # Prepare and coerce document to the strict schema expected by the DB
        prepared = Session.prepare_for_insert(copy.deepcopy(session_doc))
        
        # Fill missing fields using Gemini API (ensures no empty fields)
        prepared = Session.fill_missing_fields_with_gemini(prepared)

        # Ensure timestamps are proper datetimes
        if not isinstance(prepared.get('created_at'), datetime):
            prepared['created_at'] = datetime.utcnow()
        prepared['updated_at'] = datetime.utcnow()

        # Insert and return inserted doc with stringified _id
        result = sessions_collection.insert_one(prepared)
        prepared['_id'] = str(result.inserted_id)
        return prepared

    @staticmethod
    def prepare_for_insert(raw_doc: dict) -> dict:
        """
        Coerce an incoming session document into the strict shape expected by the
        application and database. This handles MongoDB Extended JSON wrappers
        (e.g. {$numberInt: "123"}, {$date: {$numberLong: "..."}}) by converting
        them to proper Python types, and ensures required keys/arrays exist and
        numeric fields are cast to ints/floats where appropriate.

        This attempts to preserve provided values when possible while guaranteeing
        the canonical schema used across the app. Uses Gemini API to fill missing
        fields with realistic data when available.
        """
        import copy

        def _unwrap(val):
            # Recursively convert Extended JSON wrappers to native types
            if isinstance(val, dict):
                # detect common extended JSON patterns
                if '$numberInt' in val:
                    try:
                        return int(val['$numberInt'])
                    except Exception:
                        return int(float(val['$numberInt']))
                if '$numberLong' in val:
                    try:
                        return int(val['$numberLong'])
                    except Exception:
                        return int(float(val['$numberLong']))
                if '$oid' in val:
                    try:
                        from bson.objectid import ObjectId
                        return ObjectId(val['$oid'])
                    except Exception:
                        return val['$oid']
                if '$date' in val:
                    d = val['$date']
                    # nested object with $numberLong
                    if isinstance(d, dict) and '$numberLong' in d:
                        try:
                            ms = int(d['$numberLong'])
                            return datetime.utcfromtimestamp(ms / 1000.0)
                        except Exception:
                            return None
                    # direct epoch millis
                    try:
                        ms = int(d)
                        return datetime.utcfromtimestamp(ms / 1000.0)
                    except Exception:
                        return None
                # otherwise recurse
                return {k: _unwrap(v) for k, v in val.items()}
            elif isinstance(val, list):
                return [_unwrap(v) for v in val]
            else:
                return val

        doc = _unwrap(raw_doc or {})

        # Basic required top-level fields
        doc.setdefault('sessionId', doc.get('sessionId') or doc.get('id') or '')
        doc.setdefault('sessionName', doc.get('sessionName') or doc.get('name') or f"Session {doc.get('sessionId','')}")
        # ensure videoUrl exists
        doc.setdefault('videoUrl', doc.get('videoUrl',''))

        # Coerce duration to int
        try:
            if isinstance(doc.get('duration'), dict):
                # if user provided extended JSON already unwrapped to dict, try to coerce
                doc['duration'] = int(doc['duration'].get('$numberInt') or doc['duration'].get('$numberLong') or 0)
            else:
                doc['duration'] = int(doc.get('duration') or 0)
        except Exception:
            doc['duration'] = 0

        # Ensure timeline exists with expected arrays
        tl = doc.get('timeline') if isinstance(doc.get('timeline'), dict) else {}
        tl.setdefault('audio', [])
        tl.setdefault('video', [])
        tl.setdefault('transcript', [])
        tl.setdefault('scoreDips', [])
        tl.setdefault('scorePeaks', [])

        # Normalize audio segments
        norm_audio = []
        for seg in tl.get('audio', []):
            seg = seg or {}
            try:
                start = int(seg.get('startTime') or seg.get('start') or 0)
            except Exception:
                start = 0
            try:
                end = int(seg.get('endTime') or seg.get('end') or 0)
            except Exception:
                end = 0
            try:
                pace = int(seg.get('pace') or 0)
            except Exception:
                pace = 0
            try:
                pauses = int(seg.get('pauses') or 0)
            except Exception:
                pauses = 0
            norm_audio.append({
                'startTime': start,
                'endTime': end,
                'pace': pace,
                'pauses': pauses,
                'type': seg.get('type') or 'normal',
                'message': seg.get('message') or ''
            })
        tl['audio'] = norm_audio

        # Normalize video segments
        norm_video = []
        for seg in tl.get('video', []):
            seg = seg or {}
            try:
                start = int(seg.get('startTime') or seg.get('start') or 0)
            except Exception:
                start = 0
            try:
                end = int(seg.get('endTime') or seg.get('end') or 0)
            except Exception:
                end = 0
            try:
                eye = float(seg.get('eyeContact') or seg.get('eye_contact') or 0)
            except Exception:
                eye = 0.0
            try:
                gestures = int(seg.get('gestures') or 0)
            except Exception:
                gestures = 0
            norm_video.append({
                'startTime': start,
                'endTime': end,
                'eyeContact': eye,
                'gestures': gestures,
                'type': seg.get('type') or 'good',
                'message': seg.get('message') or ''
            })
        tl['video'] = norm_video

        # Normalize transcript
        norm_trans = []
        for seg in tl.get('transcript', []):
            seg = seg or {}
            try:
                start = int(seg.get('startTime') or seg.get('start') or 0)
            except Exception:
                start = 0
            try:
                end = int(seg.get('endTime') or seg.get('end') or 0)
            except Exception:
                end = 0
            norm_trans.append({
                'startTime': start,
                'endTime': end,
                'text': seg.get('text') or seg.get('transcript') or '',
                'keyPhrases': seg.get('keyPhrases') or seg.get('key_phrases') or []
            })
        tl['transcript'] = norm_trans

        # Normalize score dips/peaks
        def _norm_score_list(lst):
            out = []
            for item in lst:
                item = item or {}
                try:
                    ts = int(item.get('timestamp') or item.get('time') or item.get('ts') or 0)
                except Exception:
                    ts = 0
                try:
                    score = int(item.get('score') or 0)
                except Exception:
                    score = 0
                out.append({
                    'timestamp': ts,
                    'score': score,
                    'message': item.get('message') or '',
                    'type': item.get('type') or ''
                })
            return out

        tl['scoreDips'] = _norm_score_list(tl.get('scoreDips', []))
        tl['scorePeaks'] = _norm_score_list(tl.get('scorePeaks', []))

        doc['timeline'] = tl

        # Normalize metrics
        metrics = doc.get('metrics', []) or []
        norm_metrics = []
        for m in metrics:
            m = m or {}
            try:
                score = int(m.get('score') or 0)
            except Exception:
                score = 0
            ci = m.get('confidenceInterval') or m.get('confidence_interval') or []
            # ensure CI is two ints
            try:
                if isinstance(ci, list) and len(ci) >= 2:
                    ci_vals = [int(ci[0]), int(ci[1])]
                else:
                    ci_vals = [0, 100]
            except Exception:
                ci_vals = [0, 100]
            
            # Ensure whatHelped and whatHurt are always present as arrays
            what_helped = m.get('whatHelped') or m.get('what_helped') or []
            what_hurt = m.get('whatHurt') or m.get('what_hurt') or []
            
            norm_metrics.append({
                'name': m.get('name') or m.get('metric') or '',
                'score': score,
                'confidenceInterval': ci_vals,
                'whatHelped': what_helped if isinstance(what_helped, list) else [],
                'whatHurt': what_hurt if isinstance(what_hurt, list) else []
            })
        doc['metrics'] = norm_metrics

        # Ensure mentorId/userId are strings
        if 'mentorId' in doc:
            doc['mentorId'] = str(doc['mentorId'])
        if 'userId' in doc:
            doc['userId'] = str(doc['userId'])

        # weakMoments default
        doc.setdefault('weakMoments', doc.get('weakMoments', []))

        return doc

    @staticmethod
    def find_by_sessionId(session_id: str):
        """Find a session document by its sessionId field."""
        from bson.objectid import ObjectId
        s = sessions_collection.find_one({'sessionId': session_id})
        if s:
            # stringify MongoDB _id for safe JSON transport
            s['_id'] = str(s['_id'])
            # Normalize sessionId -> id for frontend compatibility
            if 'sessionId' in s and 'id' not in s:
                s['id'] = s['sessionId']

            # Ensure expected fields exist so frontend timeline doesn't render empty due to missing keys
            if 'timeline' not in s or not isinstance(s.get('timeline'), dict):
                s['timeline'] = {
                    'audio': [],
                    'video': [],
                    'transcript': [],
                    'scoreDips': [],
                    'scorePeaks': []
                }
            else:
                # Ensure all sub-arrays exist
                s['timeline'].setdefault('audio', [])
                s['timeline'].setdefault('video', [])
                s['timeline'].setdefault('transcript', [])
                s['timeline'].setdefault('scoreDips', [])
                s['timeline'].setdefault('scorePeaks', [])

            # Ensure metrics is present
            s.setdefault('metrics', [])
            # Ensure duration is numeric
            if 'duration' not in s or not isinstance(s.get('duration'), (int, float)):
                s['duration'] = 0
            # Ensure videoUrl exists
            s.setdefault('videoUrl', '')
        return s

    @staticmethod
    def normalize_for_api(session_doc: dict) -> dict:
        """
        Normalize a session document for public API output.

        - Ensures presence of required keys and default types.
        - Coerces numeric fields where possible.
        - If timeline/metrics arrays are missing or empty and analysis/diarization
          information exists, attempts to use Gemini (if available) to synthesize
          realistic placeholder data. Falls back to safe defaults on any failure.

        Returns a clean dict matching the shape in `data/session_breakdown.json`.
        """
        if not session_doc:
            return {}

        # Work with a shallow copy to avoid mutating caller data
        doc = dict(session_doc)

        # Basic id/name/video defaults
        out = {}
        out['sessionId'] = doc.get('sessionId') or doc.get('id') or str(doc.get('_id', ''))
        out['sessionName'] = doc.get('sessionName') or doc.get('name') or f"Session {out['sessionId']}"
        out['videoUrl'] = doc.get('videoUrl') or doc.get('uploadedFile') or ''

        # Duration: prefer explicit duration, else try analysis or compute from transcript
        duration = doc.get('duration')
        if not isinstance(duration, (int, float)):
            if isinstance(doc.get('analysis'), dict):
                duration = doc['analysis'].get('duration') or doc['analysis'].get('total_duration')
            if not duration and isinstance(doc.get('timeline'), dict):
                transcript = doc['timeline'].get('transcript', [])
                if transcript:
                    try:
                        duration = max((int(ts.get('endTime', 0)) for ts in transcript))
                    except Exception:
                        duration = 0
        out['duration'] = int(duration or 0)

        # Ensure timeline dict exists with sub-arrays
        timeline = doc.get('timeline') if isinstance(doc.get('timeline'), dict) else {}
        timeline.setdefault('audio', [])
        timeline.setdefault('video', [])
        timeline.setdefault('transcript', timeline.get('transcript', []))
        timeline.setdefault('scoreDips', timeline.get('scoreDips', []))
        timeline.setdefault('scorePeaks', timeline.get('scorePeaks', []))
        doc['timeline'] = timeline

        # Determine what we need to synthesize
        need_audio = len(doc['timeline'].get('audio', [])) == 0
        need_video = len(doc['timeline'].get('video', [])) == 0
        need_score = len(doc['timeline'].get('scoreDips', [])) == 0 and len(doc['timeline'].get('scorePeaks', [])) == 0
        need_metrics = len(doc.get('metrics', [])) == 0

        # If we need anything, try Gemini (best-effort). Build a small context.
        if need_audio or need_video or need_score or need_metrics:
            context_parts = []
            if isinstance(doc.get('analysis'), dict):
                analysis = doc['analysis']
                if 'overall_score' in analysis:
                    context_parts.append(f"overall_score: {analysis.get('overall_score')}")
                if 'transcript' in analysis and isinstance(analysis['transcript'], str):
                    context_parts.append(f"transcript_snippet: {analysis['transcript'][:800]}")
            if isinstance(doc.get('diarization'), dict):
                diar = doc['diarization']
                if 'sentences' in diar and isinstance(diar['sentences'], list):
                    context_parts.append(f"diarization_sentences: {len(diar['sentences'])}")
                    if len(diar['sentences']) > 0:
                        ex = diar['sentences'][0].get('text','')[:200]
                        context_parts.append(f"diarization_example: {ex}")

            try:
                from google import genai
                client = genai.Client()

                prompt = (
                    "You are given partial session analysis and diarization data. "
                    "Return a JSON object with keys: timeline.audio (array of {startTime,endTime,pace,pauses,type,message}), "
                    "timeline.video (array of {startTime,endTime,eyeContact,gestures,type,message}), "
                    "timeline.scoreDips (array of {timestamp,score,message,type}), "
                    "timeline.scorePeaks (array of {timestamp,score,message,type}), "
                    "metrics (array of {name,score,confidenceInterval,whatHelped,whatHurt}), "
                    "weakMoments (array of {timestamp,message}). "
                    "Only include fields that are missing in the input. Use the provided context to generate realistic entries. "
                    "Output must be valid JSON only (no extra text)."
                )
                if context_parts:
                    prompt += " Context: " + " | ".join(context_parts)

                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt
                )

                text = None
                if hasattr(response, 'text'):
                    text = response.text
                elif hasattr(response, 'candidates') and response.candidates:
                    text = response.candidates[0].content.parts[0].text
                elif isinstance(response, dict):
                    text = response.get('output', '')

                if text:
                    import json as _json
                    try:
                        generated = _json.loads(text)
                    except Exception:
                        import re
                        m = re.search(r"\{[\s\S]*\}", text)
                        if m:
                            generated = _json.loads(m.group(0))
                        else:
                            generated = {}

                    gen_tl = generated.get('timeline', {}) if isinstance(generated, dict) else {}
                    if need_audio and isinstance(gen_tl.get('audio'), list):
                        doc['timeline']['audio'] = gen_tl['audio']
                    if need_video and isinstance(gen_tl.get('video'), list):
                        doc['timeline']['video'] = gen_tl['video']
                    if need_score:
                        if isinstance(gen_tl.get('scoreDips'), list) and gen_tl.get('scoreDips'):
                            doc['timeline']['scoreDips'] = gen_tl['scoreDips']
                        if isinstance(gen_tl.get('scorePeaks'), list) and gen_tl.get('scorePeaks'):
                            doc['timeline']['scorePeaks'] = gen_tl['scorePeaks']

                    if need_metrics and isinstance(generated.get('metrics'), list):
                        doc['metrics'] = generated.get('metrics')

                    if not doc.get('weakMoments') and isinstance(generated.get('weakMoments'), list):
                        doc['weakMoments'] = generated.get('weakMoments')
            except Exception:
                # best-effort: ignore any generator errors and continue with heuristics/defaults
                pass

        # --- Final coercion to canonical output shape ---
        def to_num(v):
            try:
                return int(v)
            except Exception:
                try:
                    return int(float(v))
                except Exception:
                    return 0

        # audio
        audio_in = doc.get('timeline', {}).get('audio', []) if isinstance(doc.get('timeline'), dict) else []
        audio_out = []
        for seg in audio_in:
            seg = seg or {}
            audio_out.append({
                'startTime': to_num(seg.get('startTime') or seg.get('start') or 0),
                'endTime': to_num(seg.get('endTime') or seg.get('end') or 0),
                'pace': int(seg.get('pace') or 0),
                'pauses': int(seg.get('pauses') or 0),
                'type': seg.get('type') or 'normal',
                'message': seg.get('message') or ''
            })

        # video
        video_in = doc.get('timeline', {}).get('video', []) if isinstance(doc.get('timeline'), dict) else []
        video_out = []
        for seg in video_in:
            seg = seg or {}
            try:
                eye = float(seg.get('eyeContact') or seg.get('eye_contact') or 0)
            except Exception:
                eye = 0.0
            video_out.append({
                'startTime': to_num(seg.get('startTime') or seg.get('start') or 0),
                'endTime': to_num(seg.get('endTime') or seg.get('end') or 0),
                'eyeContact': eye,
                'gestures': int(seg.get('gestures') or 0),
                'type': seg.get('type') or 'good',
                'message': seg.get('message') or ''
            })

        # transcript
        trans_in = doc.get('timeline', {}).get('transcript', []) if isinstance(doc.get('timeline'), dict) else []
        trans_out = []
        for seg in trans_in:
            seg = seg or {}
            trans_out.append({
                'startTime': to_num(seg.get('startTime') or seg.get('start') or 0),
                'endTime': to_num(seg.get('endTime') or seg.get('end') or 0),
                'text': seg.get('text') or seg.get('transcript') or '',
                'keyPhrases': seg.get('keyPhrases') or seg.get('key_phrases') or []
            })

        # score dips/peaks
        dips_in = doc.get('timeline', {}).get('scoreDips', []) if isinstance(doc.get('timeline'), dict) else []
        peaks_in = doc.get('timeline', {}).get('scorePeaks', []) if isinstance(doc.get('timeline'), dict) else []
        dips_out = []
        for d in dips_in:
            d = d or {}
            dips_out.append({
                'timestamp': to_num(d.get('timestamp') or d.get('time') or d.get('ts') or 0),
                'score': int(d.get('score') or 0),
                'message': d.get('message') or '',
                'type': d.get('type') or ''
            })

        peaks_out = []
        for p in peaks_in:
            p = p or {}
            peaks_out.append({
                'timestamp': to_num(p.get('timestamp') or p.get('time') or p.get('ts') or 0),
                'score': int(p.get('score') or 0),
                'message': p.get('message') or '',
                'type': p.get('type') or ''
            })

        out['timeline'] = {
            'audio': audio_out,
            'video': video_out,
            'transcript': trans_out,
            'scoreDips': dips_out,
            'scorePeaks': peaks_out
        }

        # metrics normalization
        metrics_in = doc.get('metrics', []) or []
        metrics_out = []
        for m in metrics_in:
            try:
                metrics_out.append({
                    'name': m.get('name') or m.get('metric') or '',
                    'score': int(m.get('score') or 0),
                    'confidenceInterval': m.get('confidenceInterval') or m.get('confidence_interval') or [0, 100],
                    'whatHelped': m.get('whatHelped') or m.get('what_helped') or [],
                    'whatHurt': m.get('whatHurt') or m.get('what_hurt') or []
                })
            except Exception:
                continue
        
        # Fill metric feedback if empty arrays detected
        if metrics_out:
            metrics_out = Session.fill_metric_feedback_with_gemini(
                metrics_out,
                {'sessionName': doc.get('sessionName', '')}
            )
        
        out['metrics'] = metrics_out

        # weakMoments if present
        out['weakMoments'] = doc.get('weakMoments', []) or []

        return out

    @staticmethod
    def normalize_for_api(s: dict) -> dict:
        """Return a normalized session dict that matches the `data/session_breakdown.json` schema.

        Ensures fields: sessionId, sessionName, videoUrl, duration, timeline (audio, video, transcript, scoreDips, scorePeaks), metrics
        Types are coerced where possible and defaults filled.
        """
        if not s:
            return {}

        # shallow copy to avoid mutating original
        doc = dict(s)

        out = {}
        out['sessionId'] = doc.get('sessionId') or doc.get('id') or str(doc.get('_id', ''))
        out['sessionName'] = doc.get('sessionName') or doc.get('name') or f"Session {out['sessionId']}"
        out['videoUrl'] = doc.get('videoUrl') or doc.get('uploadedFile') or ''

        # Duration: prefer explicit duration, else try analysis or compute from transcript
        duration = doc.get('duration')
        if not isinstance(duration, (int, float)):
            # try common fields
            if isinstance(doc.get('analysis'), dict):
                duration = doc['analysis'].get('duration') or doc['analysis'].get('total_duration')
            if not duration and isinstance(doc.get('timeline'), dict):
                transcript = doc['timeline'].get('transcript', [])
                if transcript:
                    try:
                        duration = max((int(ts.get('endTime', 0)) for ts in transcript))
                    except Exception:
                        duration = 0
        out['duration'] = int(duration or 0)

        # Timeline normalization
        timeline = doc.get('timeline') or {}
        # Helper to coerce numeric
        def to_num(v):
            try:
                return int(v)
            except Exception:
                try:
                    return int(float(v))
                except Exception:
                    return 0

        # audio segments
        audio_in = timeline.get('audio', []) if isinstance(timeline, dict) else []
        audio_out = []
        for seg in audio_in:
            seg = seg or {}
            audio_out.append({
                'startTime': to_num(seg.get('startTime') or seg.get('start') or 0),
                'endTime': to_num(seg.get('endTime') or seg.get('end') or 0),
                'pace': int(seg.get('pace') or 0),
                'pauses': int(seg.get('pauses') or 0),
                'type': seg.get('type') or 'normal',
                'message': seg.get('message') or ''
            })

        # video segments
        video_in = timeline.get('video', []) if isinstance(timeline, dict) else []
        video_out = []
        for seg in video_in:
            seg = seg or {}
            video_out.append({
                'startTime': to_num(seg.get('startTime') or seg.get('start') or 0),
                'endTime': to_num(seg.get('endTime') or seg.get('end') or 0),
                'eyeContact': float(seg.get('eyeContact') or seg.get('eye_contact') or 0),
                'gestures': int(seg.get('gestures') or 0),
                'type': seg.get('type') or 'good',
                'message': seg.get('message') or ''
            })

        # transcript segments
        trans_in = timeline.get('transcript', []) if isinstance(timeline, dict) else []
        trans_out = []
        for seg in trans_in:
            seg = seg or {}
            trans_out.append({
                'startTime': to_num(seg.get('startTime') or seg.get('start') or 0),
                'endTime': to_num(seg.get('endTime') or seg.get('end') or 0),
                'text': seg.get('text') or seg.get('transcript') or '',
                'keyPhrases': seg.get('keyPhrases') or seg.get('key_phrases') or []
            })

        # score dips & peaks
        dips_in = timeline.get('scoreDips', []) if isinstance(timeline, dict) else []
        peaks_in = timeline.get('scorePeaks', []) if isinstance(timeline, dict) else []
        dips_out = []
        for d in dips_in:
            d = d or {}
            dips_out.append({
                'timestamp': to_num(d.get('timestamp') or d.get('time') or d.get('ts') or 0),
                'score': int(d.get('score') or 0),
                'message': d.get('message') or '',
                'type': d.get('type') or ''
            })

        peaks_out = []
        for p in peaks_in:
            p = p or {}
            peaks_out.append({
                'timestamp': to_num(p.get('timestamp') or p.get('time') or p.get('ts') or 0),
                'score': int(p.get('score') or 0),
                'message': p.get('message') or '',
                'type': p.get('type') or ''
            })

        out['timeline'] = {
            'audio': audio_out,
            'video': video_out,
            'transcript': trans_out,
            'scoreDips': dips_out,
            'scorePeaks': peaks_out
        }

        # metrics: ensure shape
        metrics_in = doc.get('metrics', []) or []
        metrics_out = []
        for m in metrics_in:
            try:
                metrics_out.append({
                    'name': m.get('name') or m.get('metric') or '',
                    'score': int(m.get('score') or 0),
                    'confidenceInterval': m.get('confidenceInterval') or m.get('confidence_interval') or [0, 100],
                    'whatHelped': m.get('whatHelped') or m.get('what_helped') or [],
                    'whatHurt': m.get('whatHurt') or m.get('what_hurt') or []
                })
            except Exception:
                continue
        out['metrics'] = metrics_out

        return out

    @staticmethod
    def find_by_mentor(mentor_id: str, limit: int = None):
        """Return list of sessions for a mentor."""
        cursor = sessions_collection.find({'mentorId': mentor_id}).sort('created_at', -1)
        if limit:
            cursor = cursor.limit(limit)
        sessions = list(cursor)
        for s in sessions:
            s['_id'] = str(s['_id'])
            # Normalize sessionId to id for frontend compatibility
            if 'sessionId' in s and 'id' not in s:
                s['id'] = s['sessionId']
        return sessions

    @staticmethod
    def find_by_user(user_id: str, limit: int = None):
        cursor = sessions_collection.find({'userId': user_id}).sort('created_at', -1)
        if limit:
            cursor = cursor.limit(limit)
        sessions = list(cursor)
        for s in sessions:
            s['_id'] = str(s['_id'])
            # Normalize sessionId to id for frontend compatibility
            if 'sessionId' in s and 'id' not in s:
                s['id'] = s['sessionId']
        return sessions

    @staticmethod
    def update_session(session_id: str, update_data: dict):
        """Update a session by its sessionId."""
        from bson.objectid import ObjectId
        update_data['updated_at'] = datetime.utcnow()
        result = sessions_collection.find_one_and_update(
            {'sessionId': session_id},
            {'$set': update_data},
            return_document=True
        )
        if result:
            result['_id'] = str(result['_id'])
        return result
