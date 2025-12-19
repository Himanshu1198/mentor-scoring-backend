# CORS Error Fix: Video Endpoint

## Problem
```
Access to fetch at 'https://hcltech-techfest.s3.eu-north-1.amazonaws.com/...' 
blocked by CORS policy: Response to preflight request doesn't pass access control check
```

The backend endpoint `/api/mentor/{mentorId}/sessions/{sessionId}/video` was using `redirect()` to send the browser directly to the S3 URL. When the browser follows a redirect, it makes a new request to the target URL without the CORS headers from the backend response, causing S3 to block the request.

## Solution
Changed the endpoint to return the video URL as JSON instead of using redirect:

### Before (Backend)
```python
@app.route('/api/mentor/<mentor_id>/sessions/<session_id>/video', methods=['GET'])
def serve_session_video(mentor_id, session_id):
    video_url = session.get('videoUrl')
    return redirect(video_url)  # ❌ Causes CORS issues
```

### After (Backend)
```python
@app.route('/api/mentor/<mentor_id>/sessions/<session_id>/video', methods=['GET', 'OPTIONS'])
def serve_session_video(mentor_id, session_id):
    # Handle OPTIONS preflight
    if request.method == 'OPTIONS':
        return '', 204
    
    video_url = session.get('videoUrl')
    
    # Return URL as JSON with CORS headers
    response = jsonify({
        'videoUrl': video_url,
        'sessionId': session_id,
        'mentorId': mentor_id,
        'sessionName': session.get('sessionName', ''),
        'duration': session.get('duration', 0)
    })
    
    # Explicitly set CORS headers
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    
    return response, 200
```

### Frontend Update
The frontend already had code to handle this:

```typescript
const videoData = await apiClient.get<any>(
  API_ENDPOINTS.mentor.video(mentorId, sessionId)
);
videoUrl = videoData.videoUrl || videoData.url || '';
```

Added enhanced logging to help debug:
```typescript
console.log('Video endpoint response:', videoData);
console.log('Extracted video URL:', videoUrl);
```

## How It Works Now

1. **Frontend** → Calls backend `/api/mentor/{id}/sessions/{sessionId}/video`
2. **Backend** → Returns JSON with video URL (not a redirect)
3. **Backend Response** includes CORS headers
4. **Frontend** → Extracts `videoUrl` from JSON response
5. **Video element** → Uses the URL directly with S3's native CORS configuration

## Why This Works

- No redirect means the CORS headers from your backend are applied to the preflight request
- The frontend then fetches directly from S3 using the pre-signed URL
- S3 already has the CORS policy configured for your domain
- The direct S3 fetch respects S3's CORS headers properly

## Testing

1. Open browser DevTools Network tab
2. Go to breakdown page for a session
3. You should see:
   - ✅ `OPTIONS /api/mentor/.../video` → 204 (preflight passes)
   - ✅ `GET /api/mentor/.../video` → 200 with JSON response
   - ✅ Direct fetch to S3 URL succeeds
   - ✅ Video plays without CORS errors

## S3 CORS Configuration (for reference)

Your S3 bucket should have CORS configured like:
```json
[
    {
        "AllowedOrigins": ["http://localhost:3001", "http://localhost:5000"],
        "AllowedMethods": ["GET", "HEAD"],
        "AllowedHeaders": ["*"],
        "ExposeHeaders": ["ETag"],
        "MaxAgeSeconds": 3000
    }
]
```

This is separate from the backend CORS, which now properly passes through with explicit headers.
