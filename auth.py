import os
import jwt
from flask import request, jsonify
from functools import wraps
from sqlalchemy.orm import Session
from models import User, DriverInfo
from database import get_db

# 환경변수
JWT_SECRET = os.environ.get("JWT_SECRET")

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

def get_current_driver(db: Session):
    """현재 로그인한 기사 정보 가져오기"""
    user_id = request.current_user_id
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return None
    
    driver_info = db.query(DriverInfo).filter(
        DriverInfo.userId == user_id
    ).first()
    
    return {
        "user": user,
        "driver_info": driver_info
    }