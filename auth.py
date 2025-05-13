import os
import jwt
import bcrypt
from flask import request, jsonify
from functools import wraps
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from models import User, DriverInfo, UserType
from database import get_db

# 환경변수
JWT_SECRET = os.environ.get("JWT_SECRET")
JWT_EXPIRATION_HOURS = 24

def hash_password(password: str) -> str:
    """비밀번호 해시화"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    """비밀번호 검증"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def generate_token(user_id: int) -> str:
    """JWT 토큰 생성"""
    payload = {
        'user_id': user_id,
        'exp': datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm='HS256')

def login():
    """기사 로그인 API"""
    try:
        data = request.json
        email = data.get('email')
        password = data.get('password')
        
        if not email or not password:
            return jsonify({"error": "이메일과 비밀번호를 입력해주세요"}), 400
        
        with get_db() as db:
            # 기사 정보 조회
            user = db.query(User).filter(
                User.email == email,
                User.userType == UserType.DRIVER
            ).first()
            
            if not user:
                return jsonify({"error": "존재하지 않는 계정입니다"}), 404
            
            # 비밀번호 검증
            if not verify_password(password, user.password):
                return jsonify({"error": "비밀번호가 일치하지 않습니다"}), 401
            
            # 승인 여부 확인
            if not user.isApproved:
                return jsonify({"error": "승인되지 않은 계정입니다"}), 403
            
            # 기사 정보 가져오기
            driver_info = db.query(DriverInfo).filter(
                DriverInfo.userId == user.id
            ).first()
            
            if not driver_info:
                return jsonify({"error": "기사 정보를 찾을 수 없습니다"}), 404
            
            # 토큰 생성
            token = generate_token(user.id)
            
            return jsonify({
                "status": "success",
                "token": token,
                "driver": {
                    "id": user.id,
                    "name": user.name,
                    "email": user.email,
                    "driverId": driver_info.id,
                    "regionCity": driver_info.regionCity,
                    "regionDistrict": driver_info.regionDistrict,
                    "phoneNumber": driver_info.phoneNumber,
                    "vehicleNumber": driver_info.vehicleNumber
                }
            }), 200
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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

def check_driver_zone(required_zone: str):
    """기사의 구역 권한 확인 데코레이터"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            with get_db() as db:
                driver = get_current_driver(db)
                if not driver:
                    return jsonify({"error": "기사 정보를 찾을 수 없습니다"}), 404
                
                if driver["driver_info"].regionDistrict != required_zone:
                    return jsonify({"error": "해당 구역에 대한 권한이 없습니다"}), 403
                
                request.current_driver = driver
                return f(*args, **kwargs)
        
        return decorated_function
    return decorator