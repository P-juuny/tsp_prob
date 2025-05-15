import os
import jwt
from flask import request, jsonify
from functools import wraps

# 환경변수
JWT_SECRET = os.environ.get("JWT_SECRET", "your-secret-key")
BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://backend:8080")

def auth_required(f):
    """인증 확인 데코레이터"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = None
        
        # Authorization 헤더에서 토큰 추출
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            try:
                token = auth_header.split(' ')[1]  # Bearer TOKEN
            except IndexError:
                return jsonify({"error": "잘못된 토큰 형식입니다"}), 401
        
        if not token:
            return jsonify({"error": "토큰이 없습니다"}), 401
        
        try:
            # 토큰 검증
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            request.current_user_id = payload['user_id']
            
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "토큰이 만료되었습니다"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "유효하지 않은 토큰입니다"}), 401
            
        return f(*args, **kwargs)
    
    return decorated_function

def get_current_driver():
    """현재 로그인한 기사 정보 가져오기 (API 호출)"""
    import requests
    
    try:
        user_id = request.current_user_id
        
        # 백엔드 API에서 사용자 정보 가져오기
        response = requests.get(
            f"{BACKEND_API_URL}/api/user/{user_id}",
            headers={"Authorization": request.headers.get('Authorization')}
        )
        
        if response.status_code == 200:
            user_data = response.json()
            
            # 기사 정보 가져오기
            driver_response = requests.get(
                f"{BACKEND_API_URL}/api/driver/user/{user_id}",
                headers={"Authorization": request.headers.get('Authorization')}
            )
            
            if driver_response.status_code == 200:
                driver_data = driver_response.json()
                return {
                    "id": driver_data.get("id"),
                    "name": user_data.get("name"),
                    "zone": driver_data.get("zone"),
                    "user_id": user_id
                }
        
        # API 호출 실패시 기본값 반환
        return {
            "id": user_id,
            "name": "Unknown Driver",
            "zone": "Unknown"
        }
            
    except Exception as e:
        # 에러 발생시 기본값 반환
        return {
            "id": getattr(request, 'current_user_id', 1),
            "name": "Default Driver",
            "zone": "강남서부"
        }