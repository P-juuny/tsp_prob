import os
import jwt
import pymysql
import logging
from flask import request, jsonify
from functools import wraps

# 환경변수
JWT_SECRET = os.environ.get("JWT_SECRET", "your-secret-key")
BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://backend:8080")

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """DB 연결 생성"""
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "subtrack-rds.cv860smoa37l.ap-northeast-2.rds.amazonaws.com"),
        user=os.environ.get("MYSQL_USER", "admin"),
        password=os.environ.get("MYSQL_PASSWORD", "adminsubtrack"),
        db=os.environ.get("MYSQL_DATABASE", "subtrack"),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

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
            
            # userId 또는 user_id 키 확인 후 할당
            if 'userId' in payload:
                request.current_user_id = payload['userId']
            elif 'user_id' in payload:
                request.current_user_id = payload['user_id']
            else:
                return jsonify({"error": "토큰에 사용자 ID 정보가 없습니다"}), 401
            
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "토큰이 만료되었습니다"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "유효하지 않은 토큰입니다"}), 401
            
        return f(*args, **kwargs)
    
    return decorated_function

def determine_zone_by_district(district):
    """구별 구역 결정 함수"""
    district_zone_mapping = {
        # 강북서부
        "은평구": "강북서부", "서대문구": "강북서부", "마포구": "강북서부",
        # 강북동부
        "도봉구": "강북동부", "노원구": "강북동부", "강북구": "강북동부", "성북구": "강북동부",
        # 강북중부
        "종로구": "강북중부", "중구": "강북중부", "용산구": "강북중부",
        # 강남서부
        "강서구": "강남서부", "양천구": "강남서부", "구로구": "강남서부", 
        "영등포구": "강남서부", "동작구": "강남서부", "관악구": "강남서부", "금천구": "강남서부",
        # 강남동부
        "성동구": "강남동부", "광진구": "강남동부", "동대문구": "강남동부", "중랑구": "강남동부",
        "강동구": "강남동부", "송파구": "강남동부", "강남구": "강남동부", "서초구": "강남동부"
    }
    return district_zone_mapping.get(district, "Unknown")

def get_current_driver():
    """현재 로그인한 기사 정보 가져오기 (DB 직접 접근)"""
    try:
        # user_id가 없는 경우 대비한 예외 처리 추가
        if not hasattr(request, 'current_user_id'):
            logger.error("current_user_id가 request 객체에 없습니다.")
            return {
                "id": 1,  # 기본값
                "name": "Default Driver",
                "zone": "강남서부",
                "district": "강남구"
            }
        
        user_id = request.current_user_id
        logger.info(f"인증된 사용자 ID: {user_id}")
        
        # DB에서 사용자 정보 가져오기
        conn = get_db_connection()
        try:
            # 사용자 정보 조회
            with conn.cursor() as cursor:
                sql = """
                SELECT id, name, email, userType, isApproved
                FROM User 
                WHERE id = %s
                """
                cursor.execute(sql, (user_id,))
                user_data = cursor.fetchone()
                
                if not user_data:
                    logger.warning(f"사용자 ID {user_id}에 대한 정보를 찾을 수 없습니다.")
                    # 사용자를 찾을 수 없는 경우
                    return {
                        "id": user_id,
                        "name": "Unknown Driver",
                        "zone": "Unknown",
                        "district": ""
                    }
                
                # 기사 정보 조회
                sql = """
                SELECT id, userId, phoneNumber, vehicleNumber, regionCity, regionDistrict
                FROM DriverInfo
                WHERE userId = %s
                """
                cursor.execute(sql, (user_id,))
                driver_data = cursor.fetchone()
                
                if not driver_data:
                    logger.warning(f"사용자 ID {user_id}에 대한 기사 정보를 찾을 수 없습니다.")
                    # 기사 정보를 찾을 수 없는 경우
                    return {
                        "id": user_id,
                        "name": user_data.get('name', 'Unknown Driver'),
                        "zone": "Unknown",
                        "district": ""
                    }
                
                # 구별 구역 결정
                district = driver_data.get("regionDistrict", "")
                zone = determine_zone_by_district(district)
                
                result = {
                    "id": driver_data.get("id"),
                    "name": user_data.get("name"),
                    "zone": zone,
                    "district": district,
                    "user_id": user_id,
                    "phoneNumber": driver_data.get("phoneNumber"),
                    "vehicleNumber": driver_data.get("vehicleNumber"),
                    "regionCity": driver_data.get("regionCity")
                }
                
                logger.info(f"기사 정보 조회 성공: {result}")
                return result
                
        except Exception as e:
            logger.error(f"DB 쿼리 실행 오류: {e}")
            # DB 오류 시 기본값 반환
            return {
                "id": user_id,
                "name": "Error Driver",
                "zone": "Unknown",
                "district": ""
            }
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"DB에서 기사 정보 조회 오류: {e}")
        # 에러 발생시 기본값 반환
        return {
            "id": getattr(request, 'current_user_id', 1),
            "name": "Default Driver",
            "zone": "강남서부",
            "district": "강남구"
        }