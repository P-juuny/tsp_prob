import requests
import json
import numpy as np
import logging
import os
import pymysql
from datetime import datetime, timedelta, time as datetime_time
from flask import Flask, request, jsonify
import pytz
import polyline

# 인증 관련 추가
from auth import auth_required, get_current_driver

# Valhalla 관련 함수만 임포트
from get_valhalla_matrix import get_time_distance_matrix
from get_valhalla_route import get_turn_by_turn_route

# 로깅 설정
logging.basicConfig(
   level=logging.INFO,
   format='%(asctime)s - %(levelname)s - %(message)s',
   handlers=[
       logging.StreamHandler()  # FileHandler 제거
   ]
)

# --- 설정 ---
HUB_LOCATION = {"lat": 37.5299, "lon": 126.9648, "name": "용산역"}
COSTING_MODEL = "auto"
BACKEND_API_URL = os.environ.get("BACKEND_API_URL", "http://backend:8080")
LKH_SERVICE_URL = os.environ.get("LKH_SERVICE_URL", "http://lkh:5001/solve")
VALHALLA_HOST = os.environ.get("VALHALLA_HOST", "traffic-proxy")
VALHALLA_PORT = os.environ.get("VALHALLA_PORT", "8003")

# 🔧 카카오 API 설정
KAKAO_API_KEY = os.environ.get('KAKAO_API_KEY', 'YOUR_KAKAO_API_KEY_HERE')
KAKAO_ADDRESS_API = "https://dapi.kakao.com/v2/local/search/address.json"
KAKAO_KEYWORD_API = "https://dapi.kakao.com/v2/local/search/keyword.json"

# 기사별 허브 도착 상태 (메모리 저장)
driver_hub_status = {}  # {driver_id: True/False}

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')
PICKUP_START_TIME = datetime_time(7, 0)  # 오전 7시
PICKUP_CUTOFF_TIME = datetime_time(12, 0)  # 정오 12시 (신규 요청 마감)

# 구별 기사 직접 매핑
DISTRICT_DRIVER_MAPPING = {
   # 강북서부 (driver_id: 1)
   "은평구": 1, "서대문구": 1, "마포구": 1,
   
   # 강북동부 (driver_id: 2)
   "도봉구": 2, "노원구": 2, "강북구": 2, "성북구": 2,
   
   # 강북중부 (driver_id: 3)
   "종로구": 3, "중구": 3, "용산구": 3,
   
   # 강남서부 (driver_id: 4)
   "강서구": 4, "양천구": 4, "구로구": 4, "영등포구": 4, 
   "동작구": 4, "관악구": 4, "금천구": 4,
   
   # 강남동부 (driver_id: 5)
   "성동구": 5, "광진구": 5, "동대문구": 5, "중랑구": 5, 
   "강동구": 5, "송파구": 5, "강남구": 5, "서초구": 5
}

# Flask 앱 설정
app = Flask(__name__)

# --- DB 접근 함수들 ---
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

def get_parcel_from_db(parcel_id):
   """DB에서 직접 소포 정보 가져오기"""
   conn = get_db_connection()
   try:
       with conn.cursor() as cursor:
           sql = """
           SELECT p.*, 
                  o.name as ownerName, 
                  pd.name as pickupDriverName, 
                  dd.name as deliveryDriverName
           FROM Parcel p
           LEFT JOIN User o ON p.ownerId = o.id
           LEFT JOIN User pd ON p.pickupDriverId = pd.id
           LEFT JOIN User dd ON p.deliveryDriverId = dd.id
           WHERE p.id = %s AND p.isDeleted = 0
           """
           cursor.execute(sql, (parcel_id,))
           parcel = cursor.fetchone()
           
           if parcel:
               # 필드명 변환 (Prisma 스키마와 Python 코드 간 맞추기)
               if 'pickupDriverId' in parcel:
                   parcel['driverId'] = parcel['pickupDriverId']
               
               # 날짜 타입을 문자열로 변환
               for key, value in parcel.items():
                   if isinstance(value, datetime):
                       parcel[key] = value.isoformat()
                       
               # 상태값 변환 (DB의 ParcelStatus enum -> 'PENDING'/'COMPLETED')
               if parcel['status'] == 'PICKUP_PENDING':
                   parcel['status'] = 'PENDING'
               elif parcel['status'] == 'PICKUP_COMPLETED':
                   parcel['status'] = 'COMPLETED'
               
               return parcel
           return None
   except Exception as e:
       logging.error(f"DB 쿼리 오류: {e}")
       return None
   finally:
       conn.close()

def get_driver_parcels_from_db(driver_id):
   """DB에서 직접 기사 할당 소포 목록 가져오기 (오늘 처리할 것만)"""
   conn = get_db_connection()
   try:
       with conn.cursor() as cursor:
           today = datetime.now(KST).date()
           sql = """
           SELECT p.*, 
                  o.name as ownerName
           FROM Parcel p
           LEFT JOIN User o ON p.ownerId = o.id
           WHERE p.pickupDriverId = %s AND p.isDeleted = 0
           AND (
               p.pickupScheduledDate IS NULL OR 
               DATE(p.pickupScheduledDate) <= %s
           )
           ORDER BY p.createdAt DESC
           """
           cursor.execute(sql, (driver_id, today))
           parcels = cursor.fetchall()
           
           # API 응답 형식에 맞게 변환
           result = []
           for p in parcels:
               # 상태값 변환 (DB의 ParcelStatus enum -> 'PENDING'/'COMPLETED')
               status = 'PENDING' if p['status'] == 'PICKUP_PENDING' else 'COMPLETED'
               
               # 날짜 필드 처리
               completed_at = p['pickupCompletedAt'].isoformat() if p['pickupCompletedAt'] else None
               created_at = p['createdAt'].isoformat() if p['createdAt'] else None
               
               item = {
                   'id': p['id'],
                   'status': status,
                   'recipientAddr': p['recipientAddr'],
                   'productName': p['productName'],
                   'pickupCompletedAt': completed_at,
                   'assignedAt': created_at,
                   'ownerId': p['ownerId'],
                   'ownerName': p.get('ownerName'),
                   'size': p['size']
               }
               result.append(item)
           
           return result
   except Exception as e:
       logging.error(f"DB 쿼리 오류: {e}")
       return []
   finally:
       conn.close()

def assign_driver_to_parcel_in_db(parcel_id, driver_id):
   """DB에서 직접 기사 할당 (오늘 처리용)"""
   conn = get_db_connection()
   try:
       with conn.cursor() as cursor:
           sql = """
           UPDATE Parcel 
           SET pickupDriverId = %s, 
               status = 'PICKUP_PENDING', 
               isNextPickupTarget = TRUE,
               pickupScheduledDate = CURDATE()
           WHERE id = %s AND isDeleted = 0
           """
           cursor.execute(sql, (driver_id, parcel_id))
       conn.commit()
       return cursor.rowcount > 0
   except Exception as e:
       logging.error(f"DB 쿼리 오류: {e}")
       conn.rollback()
       return False
   finally:
       conn.close()

def assign_driver_to_parcel_for_tomorrow(parcel_id, tomorrow_date):
   """내일 처리용으로 소포 할당 - 카카오 지오코딩 사용"""
   conn = get_db_connection()
   try:
       with conn.cursor() as cursor:
           # 파셀 정보 가져와서 구 확인
           parcel = get_parcel_from_db(parcel_id)
           if not parcel:
               return False
           
           address = parcel.get('recipientAddr', '')
           
           # 🔧 카카오 API로 구 정보 추출
           district = extract_district_from_kakao_geocoding(address)
           
           if not district:
               logging.warning(f"구 정보 추출 실패: {address}")
               return False
           
           driver_id = DISTRICT_DRIVER_MAPPING.get(district)
           if not driver_id:
               logging.warning(f"해당 구에 대응하는 기사 없음: {district}")
               return False
           
           # 내일 처리용으로 할당
           sql = """
           UPDATE Parcel 
           SET pickupDriverId = %s, 
               status = 'PICKUP_PENDING',
               pickupScheduledDate = %s,
               isNextPickupTarget = FALSE
           WHERE id = %s AND isDeleted = 0
           """
           cursor.execute(sql, (driver_id, tomorrow_date, parcel_id))
       conn.commit()
       return cursor.rowcount > 0
   except Exception as e:
       logging.error(f"DB 쿼리 오류: {e}")
       conn.rollback()
       return False
   finally:
       conn.close()

def complete_parcel_in_db(parcel_id):
   """DB에서 직접 수거 완료 처리"""
   conn = get_db_connection()
   try:
       with conn.cursor() as cursor:
           sql = """
           UPDATE Parcel 
           SET status = 'PICKUP_COMPLETED', 
               isNextPickupTarget = FALSE,
               pickupCompletedAt = NOW() 
           WHERE id = %s AND isDeleted = 0
           """
           cursor.execute(sql, (parcel_id,))
       conn.commit()
       return cursor.rowcount > 0
   except Exception as e:
       logging.error(f"DB 쿼리 오류: {e}")
       conn.rollback()
       return False
   finally:
       conn.close()

def get_completed_pickups_today_from_db():
   """DB에서 오늘 완료된 수거 목록 가져오기"""
   conn = get_db_connection()
   try:
       with conn.cursor() as cursor:
           sql = """
           SELECT p.*, 
                  o.name as ownerName
           FROM Parcel p
           LEFT JOIN User o ON p.ownerId = o.id
           WHERE p.status = 'PICKUP_COMPLETED' 
           AND DATE(p.pickupCompletedAt) = CURDATE()
           AND p.isDeleted = 0
           """
           cursor.execute(sql)
           parcels = cursor.fetchall()
           
           # API 응답 형식에 맞게 변환
           result = []
           for p in parcels:
               # 날짜 필드 처리
               completed_at = p['pickupCompletedAt'].isoformat() if p['pickupCompletedAt'] else None
               created_at = p['createdAt'].isoformat() if p['createdAt'] else None
               
               item = {
                   'id': p['id'],
                   'status': 'COMPLETED',
                   'recipientAddr': p['recipientAddr'],
                   'productName': p['productName'],
                   'pickupCompletedAt': completed_at,
                   'assignedAt': created_at,
                   'ownerId': p['ownerId'],
                   'ownerName': p.get('ownerName'),
                   'pickupDriverId': p['pickupDriverId'],
                   'size': p['size']
               }
               result.append(item)
           
           return result
   except Exception as e:
       logging.error(f"DB 쿼리 오류: {e}")
       return []
   finally:
       conn.close()

# --- 🔧 카카오 지오코딩 전용 함수들 ---

def kakao_geocoding(address):
    """카카오 API로 주소를 위도/경도로 변환"""
    try:
        headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
        
        # 1차: 주소 검색 API 시도
        params = {"query": address}
        response = requests.get(KAKAO_ADDRESS_API, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            documents = data.get("documents", [])
            
            if documents:
                doc = documents[0]  # 첫 번째 결과 사용
                lat = float(doc["y"])
                lon = float(doc["x"])
                address_name = doc.get("address_name", address)
                
                logging.info(f"카카오 주소 검색 성공: {address} -> ({lat}, {lon}) [{address_name}]")
                return lat, lon, address_name
        
        # 2차: 주소 검색 실패시 키워드 검색 시도
        response = requests.get(KAKAO_KEYWORD_API, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            documents = data.get("documents", [])
            
            if documents:
                doc = documents[0]  # 첫 번째 결과 사용
                lat = float(doc["y"])
                lon = float(doc["x"])
                place_name = doc.get("place_name", address)
                
                logging.info(f"카카오 키워드 검색 성공: {address} -> ({lat}, {lon}) [{place_name}]")
                return lat, lon, place_name
        
        # 카카오 API 실패시 기본 좌표
        logging.warning(f"카카오 지오코딩 실패, 기본 좌표 사용: {address}")
        return get_default_coordinates_by_district(address)
        
    except Exception as e:
        logging.error(f"카카오 지오코딩 오류: {e}")
        return get_default_coordinates_by_district(address)

def extract_district_from_kakao_geocoding(address):
    """카카오 API를 통해 정확한 구 정보 추출"""
    try:
        headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
        params = {"query": address}
        
        # 주소 검색 API 사용
        response = requests.get(KAKAO_ADDRESS_API, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            documents = data.get("documents", [])
            
            if documents:
                doc = documents[0]
                
                # address 객체에서 구 정보 추출
                address_info = doc.get("address", {})
                if address_info:
                    district = address_info.get("region_2depth_name", "")
                    if district and district.endswith("구"):
                        logging.info(f"카카오 API로 구 추출 성공: {address} -> {district}")
                        return district
                
                # road_address 객체에서 구 정보 추출
                road_address = doc.get("road_address", {})
                if road_address:
                    district = road_address.get("region_2depth_name", "")
                    if district and district.endswith("구"):
                        logging.info(f"카카오 API로 구 추출 성공 (도로명): {address} -> {district}")
                        return district
        
        # API 실패시 텍스트에서 직접 추출
        address_parts = address.split()
        for part in address_parts:
            if part.endswith('구'):
                logging.info(f"텍스트에서 구 추출: {address} -> {part}")
                return part
        
        logging.warning(f"구 정보 추출 실패: {address}")
        return None
        
    except Exception as e:
        logging.error(f"구 추출 오류: {e}")
        # fallback: 텍스트에서 직접 추출
        address_parts = address.split()
        for part in address_parts:
            if part.endswith('구'):
                return part
        return None

def address_to_coordinates(address):
    """카카오 API를 사용한 주소 -> 좌표 변환 (메인 함수)"""
    lat, lon, _ = kakao_geocoding(address)
    return lat, lon

def get_default_coordinates_by_district(address):
    """구별 기본 좌표 (카카오 API 실패시 사용)"""
    district_coords = {
        "강남구": (37.5172, 127.0473, "강남구 역삼동"),
        "서초구": (37.4837, 127.0324, "서초구 서초동"),
        "송파구": (37.5145, 127.1059, "송파구 잠실동"),
        "강동구": (37.5301, 127.1238, "강동구 천호동"),
        "성동구": (37.5634, 127.0369, "성동구 성수동"),
        "광진구": (37.5384, 127.0822, "광진구 광장동"),
        "동대문구": (37.5744, 127.0396, "동대문구 전농동"),
        "중랑구": (37.6063, 127.0927, "중랑구 면목동"),
        "종로구": (37.5735, 126.9790, "종로구 종로"),
        "중구": (37.5641, 126.9979, "중구 명동"),
        "용산구": (37.5311, 126.9810, "용산구 한강로"),
        "성북구": (37.5894, 127.0167, "성북구 성북동"),
        "강북구": (37.6396, 127.0253, "강북구 번동"),
        "도봉구": (37.6687, 127.0472, "도봉구 방학동"),
        "노원구": (37.6543, 127.0568, "노원구 상계동"),
        "은평구": (37.6176, 126.9269, "은평구 불광동"),
        "서대문구": (37.5791, 126.9368, "서대문구 신촌동"),
        "마포구": (37.5638, 126.9084, "마포구 공덕동"),
        "양천구": (37.5170, 126.8667, "양천구 목동"),
        "강서구": (37.5509, 126.8496, "강서구 화곡동"),
        "구로구": (37.4954, 126.8877, "구로구 구로동"),
        "금천구": (37.4564, 126.8955, "금천구 가산동"),
        "영등포구": (37.5263, 126.8966, "영등포구 영등포동"),
        "동작구": (37.5124, 126.9393, "동작구 상도동"),
        "관악구": (37.4784, 126.9516, "관악구 봉천동")
    }
    
    for district, (lat, lon, name) in district_coords.items():
        if district in address:
            logging.info(f"기본 좌표 사용: {address} -> ({lat}, {lon}) [{name}]")
            return lat, lon, name
    
    # 서울시청 기본 좌표
    logging.warning(f"구를 찾을 수 없어 서울시청 좌표 사용: {address}")
    return 37.5665, 126.9780, "서울시청"

# 🔧 수정된 waypoints 추출 함수
def extract_waypoints_from_route(route_info):
    """Valhalla route 응답에서 waypoints와 coordinates 추출"""
    waypoints = []
    coordinates = []
    
    try:
        if not route_info or 'trip' not in route_info:
            return waypoints, coordinates
        
        trip = route_info['trip']
        if 'legs' not in trip or not trip['legs']:
            return waypoints, coordinates
        
        # 첫 번째 leg의 정보 추출
        leg = trip['legs'][0]
        maneuvers = leg.get('maneuvers', [])
        
        # Shape 디코딩해서 전체 좌표 배열 생성
        if 'shape' in leg and leg['shape']:
            try:
                # polyline 디코딩: shape -> 좌표 배열
                decoded_coords = polyline.decode(leg['shape'], precision = 6)
                coordinates = [{"lat": lat, "lon": lon} for lat, lon in decoded_coords]
                logging.info(f"Decoded {len(coordinates)} coordinates from shape")
            except Exception as e:
                logging.error(f"Shape decoding error: {e}")
                coordinates = []
        
        # 🔧 핵심 수정: maneuvers에서 waypoints 추출할 때 좌표 처리
        for i, maneuver in enumerate(maneuvers):
            instruction = maneuver.get('instruction', f'구간 {i+1}')
            street_names = maneuver.get('street_names', [])
            street_name = street_names[0] if street_names else f'구간{i+1}'
            
            # 🔧 중요: begin_shape_index를 사용해서 실제 좌표 가져오기
            begin_idx = maneuver.get('begin_shape_index', 0)
            
            if coordinates and begin_idx < len(coordinates):
                # 🔧 여기가 문제였음: 딕셔너리에서 값을 제대로 가져와야 함
                lat = coordinates[begin_idx]["lat"]
                lon = coordinates[begin_idx]["lon"]
            else:
                # 기본값
                lat = 0.0
                lon = 0.0
            
            waypoint = {
                "lat": lat,
                "lon": lon,
                "name": street_name,
                "instruction": instruction
            }
            waypoints.append(waypoint)
        
        logging.info(f"Extracted {len(waypoints)} waypoints and {len(coordinates)} coordinates")
        
    except Exception as e:
        logging.error(f"Error extracting waypoints: {e}")
    
    return waypoints, coordinates
    
# --- API 엔드포인트 ---

@app.route('/api/pickup/webhook', methods=['POST'])
def webhook_new_pickup():
   """백엔드에서 새 수거 추가시 호출하는 웹훅 - 마감 시간 적용"""
   try:
       data = request.json
       parcel_id = data.get('parcelId')
       
       if not parcel_id:
           return jsonify({"error": "parcelId is required"}), 400
       
       # ===== 마감 시간 체크 로직 =====
       current_time = datetime.now(KST).time()
       current_date = datetime.now(KST).date()
       
       if current_time >= PICKUP_CUTOFF_TIME:  # 정오 12시 이후
           logging.info(f"수거 요청 마감 시간 후 접수 - 내일로 처리: {parcel_id}")
           
           # 내일 처리용으로 DB에 저장
           tomorrow = current_date + timedelta(days=1)
           
           if assign_driver_to_parcel_for_tomorrow(parcel_id, tomorrow):
               return jsonify({
                   "status": "scheduled_tomorrow", 
                   "message": "정오 12시 이후 요청은 다음날 수거로 처리됩니다.",
                   "scheduled_date": tomorrow.isoformat(),
                   "cutoff_time": "12:00",
                   "current_time": current_time.strftime("%H:%M")
               }), 200
           else:
               return jsonify({"error": "Failed to schedule for tomorrow"}), 500
       
       # ===== 정오 이전 - 오늘 할당 =====
       # DB에서 수거 정보 가져오기
       parcel = get_parcel_from_db(parcel_id)
       if not parcel:
           return jsonify({"error": "Parcel not found"}), 404
       
       # 이미 기사 할당되었는지 확인
       if parcel.get('driverId') or parcel.get('pickupDriverId'):
           return jsonify({"status": "already_processed"}), 200
       
       # 🔧 카카오 API로 주소 처리
       address = parcel.get('recipientAddr', '')
       lat, lon, location_name = kakao_geocoding(address)
       
       # 🔧 카카오 API로 구 정보 추출
       district = extract_district_from_kakao_geocoding(address)
       
       if not district:
           return jsonify({
               "error": "Could not determine district from address",
               "address": address
           }), 400
       
       # 구별로 기사 직접 할당
       driver_id = DISTRICT_DRIVER_MAPPING.get(district)
       if not driver_id:
           return jsonify({
               "status": "error",
               "message": f"No driver for district {district}",
               "district": district
           }), 500
       
       # DB에 기사 할당 (오늘 처리용)
       if assign_driver_to_parcel_in_db(parcel_id, driver_id):
           return jsonify({
               "status": "success",
               "parcelId": parcel_id,
               "district": district,
               "driverId": driver_id,
               "coordinates": {"lat": lat, "lon": lon},
               "location_name": location_name,
               "scheduled_for": "today",
               "geocoding_method": "kakao"
           }), 200
       else:
           return jsonify({"error": "Failed to assign driver"}), 500
               
   except Exception as e:
       logging.error(f"Error processing webhook: {e}", exc_info=True)
       return jsonify({"error": "Internal server error"}), 500

@app.route('/api/pickup/hub-arrived', methods=['POST'])
@auth_required
def hub_arrived():
    """허브 도착 완료 처리 (간단 버전)"""
    try:
        # 현재 로그인한 기사 확인
        driver_info = get_current_driver()
        driver_id = driver_info['user_id']
        
        # driver_id는 1-5 중 하나여야 함 (수거 기사)
        if driver_id not in [1, 2, 3, 4, 5]:
            return jsonify({"error": "수거 기사만 접근 가능합니다"}), 403
        
        # 현재 할당된 수거가 없는지 확인
        parcels = get_driver_parcels_from_db(driver_id)
        pending_pickups = [p for p in parcels if p['status'] == 'PENDING']
        
        if pending_pickups:
            return jsonify({
                "error": "아직 완료하지 않은 수거가 있습니다",
                "remaining_pickups": len(pending_pickups)
            }), 400
        
        # 🔧 메모리에 허브 도착 상태 저장
        driver_hub_status[driver_id] = True
        
        return jsonify({
            "status": "success",
            "message": "허브 도착이 완료되었습니다. 수고하셨습니다!",
            "location": HUB_LOCATION,
            "arrival_time": datetime.now(KST).strftime("%H:%M")
        }), 200
            
    except Exception as e:
        logging.error(f"Error processing hub arrival: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/pickup/next', methods=['GET'])
@auth_required
def get_next_destination():
   """현재 로그인한 기사의 다음 최적 목적지 계산 (카카오 지오코딩 사용)"""
   try:
       # 현재 로그인한 기사 정보 가져오기
       driver_info = get_current_driver()
       driver_id = driver_info['user_id']
       
       # driver_id는 1-5 중 하나여야 함 (수거 기사)
       if driver_id not in [1, 2, 3, 4, 5]:
           return jsonify({"error": "수거 기사만 접근 가능합니다"}), 403
       
       # 시간 체크 추가
       current_time = datetime.now(KST).time()
       if current_time < PICKUP_START_TIME:  # 오전 7시 이전
           hours_left = PICKUP_START_TIME.hour - current_time.hour
           minutes_left = PICKUP_START_TIME.minute - current_time.minute
           if minutes_left < 0:
               hours_left -= 1
               minutes_left += 60
           
           return jsonify({
               "status": "waiting",
               "message": f"수거는 오전 7시부터 시작됩니다. {hours_left}시간 {minutes_left}분 남았습니다.",
               "start_time": "07:00",
               "current_time": current_time.strftime("%H:%M")
           }), 200
           
       # DB에서 기사의 소포 목록 가져오기
       parcels = get_driver_parcels_from_db(driver_id)
       pending_pickups = [p for p in parcels if p['status'] == 'PENDING']
       
       # 🔧 현재 위치 계산 (마지막 수거 완료 위치)
       current_location = HUB_LOCATION  # 기본값
       
       # 1. 먼저 허브 도착 상태 확인
       if driver_hub_status.get(driver_id, False):
           current_location = HUB_LOCATION
           logging.info(f"기사 {driver_id} 허브 도착 완료 상태")
       else:
           # 2. 오늘 완료된 수거가 있으면 마지막 완료 위치가 현재 위치
           today = datetime.now(KST).strftime('%Y-%m-%d')
           completed_today = [p for p in parcels 
                            if p['status'] == 'COMPLETED' 
                            and (p.get('pickupCompletedAt') or '').startswith(today)]
           
           if completed_today:
               last_completed = sorted(completed_today, 
                                     key=lambda x: x['pickupCompletedAt'], 
                                     reverse=True)[0]
               actual_address = last_completed['recipientAddr']
               # 🔧 카카오 지오코딩 사용
               lat, lon, location_name = kakao_geocoding(actual_address)
               current_location = {"lat": lat, "lon": lon, "name": location_name}
               logging.info(f"마지막 수거 완료 위치 (카카오): {actual_address} -> ({lat}, {lon}) [{location_name}]")
       
       # 미완료 수거가 없을 때
       if not pending_pickups:
           current_time = datetime.now(KST).time()
           
           # 🔧 이미 허브에 있다면
           if driver_hub_status.get(driver_id, False):
               return jsonify({
                   "status": "at_hub",
                   "message": "허브에 도착했습니다. 수고하셨습니다!",
                   "current_location": current_location,
                   "remaining_pickups": 0,
                   "is_last": True
               }), 200
           
           # 🔧 12시 이전이면 "대기" 상태
           if current_time < PICKUP_CUTOFF_TIME:  # 정오 12시 이전
               return jsonify({
                   "status": "waiting_for_orders",
                   "message": f"현재 할당된 수거가 없습니다. 신규 요청을 대기 중입니다. (마감: 12:00)",
                   "current_time": current_time.strftime("%H:%M"),
                   "cutoff_time": "12:00",
                   "current_location": current_location,
                   "is_last": False,
                   "remaining_pickups": 0
               }), 200
           
           # 🔧 12시 이후면 허브 복귀
           else:
               route_info = get_turn_by_turn_route(
                   current_location,
                   HUB_LOCATION,
                   costing=COSTING_MODEL
               )
               
               # 🔧 waypoints 및 coordinates 추출
               waypoints, coordinates = extract_waypoints_from_route(route_info)
               if not waypoints:
                   # 기본 waypoints (출발지 -> 목적지)
                   waypoints = [
                       {
                           "lat": current_location["lat"],
                           "lon": current_location["lon"],
                           "name": current_location.get("name", "현재위치"),
                           "instruction": "허브로 복귀 시작"
                       },
                       {
                           "lat": HUB_LOCATION["lat"],
                           "lon": HUB_LOCATION["lon"],
                           "name": HUB_LOCATION["name"],
                           "instruction": "허브 도착"
                       }
                   ]
                   # 기본 coordinates
                   coordinates = [
                       {"lat": current_location["lat"], "lon": current_location["lon"]},
                       {"lat": HUB_LOCATION["lat"], "lon": HUB_LOCATION["lon"]}
                   ]
               
               # 🔧 route에 waypoints와 coordinates 추가
               if route_info and 'trip' in route_info:
                   route_info['waypoints'] = waypoints
                   route_info['coordinates'] = coordinates
               
               return jsonify({
                   "status": "return_to_hub",
                   "message": "모든 수거가 완료되었습니다. 허브로 복귀해주세요.",
                   "next_destination": HUB_LOCATION,
                   "route": route_info,
                   "is_last": True,
                   "remaining_pickups": 0,
                   "current_location": current_location,
                   "distance_to_hub": route_info['trip']['summary']['length'] if route_info else 0
               }), 200
       
       # 🔧 새로운 수거가 시작되면 허브 상태 리셋
       if pending_pickups and driver_hub_status.get(driver_id, False):
           driver_hub_status[driver_id] = False
           logging.info(f"기사 {driver_id} 새로운 수거 시작으로 허브 상태 리셋")
       
       # 🔧 수거 대기 장소만 TSP 계산 (현재 위치 제외)
       pickup_locations = []
       for pickup in pending_pickups:
           # 카카오 지오코딩으로 정확한 좌표 계산
           lat, lon, location_name = kakao_geocoding(pickup['recipientAddr'])
           pickup_locations.append({
               "lat": lat,
               "lon": lon,
               "parcel_id": pickup['id'],
               "parcelId": pickup['id'],
               "name": pickup['productName'],
               "productName": pickup['productName'],
               "address": pickup['recipientAddr'],
               "location_name": location_name
           })
       
       # 수거 장소가 1개면 TSP 계산 없이 바로 선택
       if len(pickup_locations) == 1:
           next_location = pickup_locations[0]
           
           route_info = get_turn_by_turn_route(
               current_location,
               {"lat": next_location["lat"], "lon": next_location["lon"]},
               costing=COSTING_MODEL
           )
           
           # waypoints 및 coordinates 추출
           waypoints, coordinates = extract_waypoints_from_route(route_info)
           if not waypoints:
               waypoints = [
                   {
                       "lat": current_location["lat"],
                       "lon": current_location["lon"],
                       "name": current_location.get("name", "출발지"),
                       "instruction": "수거 시작"
                   },
                   {
                       "lat": next_location["lat"],
                       "lon": next_location["lon"],
                       "name": next_location.get("location_name", next_location["productName"]),
                       "instruction": "목적지 도착"
                   }
               ]
               coordinates = [
                   {"lat": current_location["lat"], "lon": current_location["lon"]},
                   {"lat": next_location["lat"], "lon": next_location["lon"]}
               ]
           
           if route_info and 'trip' in route_info:
               route_info['waypoints'] = waypoints
               route_info['coordinates'] = coordinates
           
           return jsonify({
               "status": "success",
               "next_destination": {
                   "lat": next_location["lat"],
                   "lon": next_location["lon"],
                   "parcel_id": next_location.get("parcel_id"),
                   "parcelId": next_location.get("parcel_id"),
                   "name": next_location.get("productName"),
                   "productName": next_location.get("productName"),
                   "address": next_location.get("address"),
                   "location_name": next_location.get("location_name")
               },
               "route": route_info,
               "is_last": False,
               "remaining_pickups": len(pending_pickups),
               "geocoding_method": "kakao"
           }), 200
       
       # 🔧 수거 장소가 2개 이상이면 TSP 계산
       if len(pickup_locations) > 1:
           # 현재 위치에서 각 수거 장소까지의 거리를 포함한 매트릭스 계산
           all_coords = [{"lat": current_location["lat"], "lon": current_location["lon"]}]
           all_coords.extend([{"lat": loc["lat"], "lon": loc["lon"]} for loc in pickup_locations])
           
           time_matrix, _ = get_time_distance_matrix(all_coords, costing=COSTING_MODEL)
           
           if time_matrix is not None:
               # LKH로 최적 경로 계산
               response = requests.post(
                   LKH_SERVICE_URL,
                   json={"matrix": time_matrix.tolist()}
               )
               if response.status_code == 200:
                   result = response.json()
                   optimal_tour = result.get("tour")
                   
                   if optimal_tour and len(optimal_tour) > 1:
                       # 시작점(0)이 tour에서 몇 번째인지 찾기
                       start_pos = optimal_tour.index(0)
                       # 그 다음 위치 선택
                       next_pos = (start_pos + 1) % len(optimal_tour)
                       next_idx = optimal_tour[next_pos]
                       
                       # next_idx가 0이면 (현재 위치면) 그 다음 선택
                       if next_idx == 0:
                           next_pos = (start_pos + 2) % len(optimal_tour)
                           next_idx = optimal_tour[next_pos]
                       
                       # pickup_locations에서 선택 (인덱스 조정: -1)
                       pickup_idx = next_idx - 1
                       if 0 <= pickup_idx < len(pickup_locations):
                           next_location = pickup_locations[pickup_idx]
                           
                           route_info = get_turn_by_turn_route(
                               current_location,
                               {"lat": next_location["lat"], "lon": next_location["lon"]},
                               costing=COSTING_MODEL
                           )
                           
                           # waypoints 및 coordinates 추출
                           waypoints, coordinates = extract_waypoints_from_route(route_info)
                           if not waypoints:
                               waypoints = [
                                   {
                                       "lat": current_location["lat"],
                                       "lon": current_location["lon"],
                                       "name": current_location.get("name", "출발지"),
                                       "instruction": "수거 시작"
                                   },
                                   {
                                       "lat": next_location["lat"],
                                       "lon": next_location["lon"],
                                       "name": next_location.get("location_name", next_location["productName"]),
                                       "instruction": "목적지 도착"
                                   }
                               ]
                               coordinates = [
                                   {"lat": current_location["lat"], "lon": current_location["lon"]},
                                   {"lat": next_location["lat"], "lon": next_location["lon"]}
                               ]
                           
                           if route_info and 'trip' in route_info:
                               route_info['waypoints'] = waypoints
                               route_info['coordinates'] = coordinates
                           
                           return jsonify({
                               "status": "success",
                               "next_destination": {
                                   "lat": next_location["lat"],
                                   "lon": next_location["lon"],
                                   "parcel_id": next_location.get("parcel_id"),
                                   "parcelId": next_location.get("parcel_id"),
                                   "name": next_location.get("productName"),
                                   "productName": next_location.get("productName"),
                                   "address": next_location.get("address"),
                                   "location_name": next_location.get("location_name")
                               },
                               "route": route_info,
                               "is_last": False,
                               "remaining_pickups": len(pending_pickups),
                               "geocoding_method": "kakao"
                           }), 200
       
       # 🔧 fallback: 첫 번째 수거 장소 선택
       if pickup_locations:
           next_location = pickup_locations[0]
           
           route_info = get_turn_by_turn_route(
               current_location,
               {"lat": next_location["lat"], "lon": next_location["lon"]},
               costing=COSTING_MODEL
           )
           
           # waypoints 및 coordinates 추출
           waypoints, coordinates = extract_waypoints_from_route(route_info)
           if not waypoints:
               waypoints = [
                   {
                       "lat": current_location["lat"],
                       "lon": current_location["lon"],
                       "name": current_location.get("name", "출발지"),
                       "instruction": "출발"
                   },
                   {
                       "lat": next_location["lat"],
                       "lon": next_location["lon"],
                       "name": next_location.get("location_name", next_location.get("productName", "목적지")),
                       "instruction": "도착"
                   }
               ]
               coordinates = [
                   {"lat": current_location["lat"], "lon": current_location["lon"]},
                   {"lat": next_location["lat"], "lon": next_location["lon"]}
               ]
           
           if route_info and 'trip' in route_info:
               route_info['waypoints'] = waypoints
               route_info['coordinates'] = coordinates
           
           return jsonify({
               "status": "success",
               "next_destination": {
                   "lat": next_location["lat"],
                   "lon": next_location["lon"],
                   "parcel_id": next_location.get("parcel_id"),
                   "parcelId": next_location.get("parcel_id"),
                   "name": next_location.get("productName"),
                   "productName": next_location.get("productName"),
                   "address": next_location.get("address"),
                   "location_name": next_location.get("location_name")
               },
               "route": route_info,
               "is_last": False,
               "remaining_pickups": len(pending_pickups),
               "geocoding_method": "kakao"
           }), 200
       
       # 마지막 fallback: 허브로
       return jsonify({
           "status": "return_to_hub",
           "message": "수거할 장소가 없습니다. 허브로 복귀해주세요.",
           "next_destination": HUB_LOCATION,
           "is_last": True,
           "remaining_pickups": 0,
           "current_location": current_location
       }), 200
           
   except Exception as e:
       logging.error(f"Error getting next destination: {e}", exc_info=True)
       return jsonify({"error": "Internal server error"}), 500
       
@app.route('/api/pickup/complete', methods=['POST'])
@auth_required
def complete_pickup():
   """수거 완료 처리"""
   try:
       # 현재 로그인한 기사 확인
       driver_info = get_current_driver()
       driver_id = driver_info['user_id']
       
       data = request.json
       parcel_id = data.get('parcelId')
       
       if not parcel_id:
           return jsonify({"error": "parcelId is required"}), 400
       
       # 해당 소포가 현재 기사에게 할당되었는지 확인
       parcel = get_parcel_from_db(parcel_id)
       if not parcel or parcel.get('pickupDriverId') != driver_id:
           return jsonify({"error": "권한이 없습니다"}), 403
       
       # DB에서 완료 처리
       if complete_parcel_in_db(parcel_id):
           return jsonify({"status": "success"}), 200
       else:
           return jsonify({"error": "Failed to complete pickup"}), 500
           
   except Exception as e:
       logging.error(f"Error completing pickup: {e}", exc_info=True)
       return jsonify({"error": "Internal server error"}), 500

@app.route('/api/pickup/all-completed', methods=['GET'])
def check_all_completed():
    """모든 수거가 완료됐는지 확인하고 자동으로 배달 전환"""
    try:
        # 오늘 날짜
        today = datetime.now(KST).strftime('%Y-%m-%d')
        
        # 모든 기사(1-5) 체크
        all_drivers = [1, 2, 3, 4, 5]
        total_pending = 0
        total_completed = 0
        first_pending_driver = None
        first_pending_count = 0
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 오늘 처리할 미완료 수거 확인
                sql_pending = """
                SELECT pickupDriverId, COUNT(*) as pending_count
                FROM Parcel
                WHERE status = 'PICKUP_PENDING' 
                AND (pickupScheduledDate IS NULL OR DATE(pickupScheduledDate) <= CURDATE())
                AND isDeleted = 0
                GROUP BY pickupDriverId
                """
                cursor.execute(sql_pending)
                pending_results = cursor.fetchall()
                
                # 오늘 완료된 수거 확인
                sql_completed = """
                SELECT COUNT(*) as completed_count
                FROM Parcel
                WHERE status = 'PICKUP_COMPLETED'
                AND DATE(pickupCompletedAt) = CURDATE()
                AND isDeleted = 0
                """
                cursor.execute(sql_completed)
                completed_result = cursor.fetchone()
                
                # 🔧 수정: 모든 결과를 먼저 집계
                if pending_results:
                    for result in pending_results:
                        driver_id = result['pickupDriverId']
                        pending_count = result['pending_count']
                        total_pending += pending_count
                        
                        # 첫 번째 미완료 기사 정보 저장
                        if pending_count > 0 and first_pending_driver is None:
                            first_pending_driver = driver_id
                            first_pending_count = pending_count
                
                # 완료된 수거 개수
                total_completed = completed_result['completed_count'] if completed_result else 0
                
        finally:
            conn.close()
        
        # 🔧 수정: 미완료가 있으면 집계 완료 후 응답
        if total_pending > 0:
            return jsonify({
                "completed": False, 
                "remaining": total_pending,
                "completed_count": total_completed,
                "driver_status": f"Driver {first_pending_driver} has {first_pending_count} pending"
            }), 200
        
        # 모든 수거가 완료됨
        if total_completed > 0:  # 오늘 수거한 게 있을 때만
            try:
                # 배달로 자동 전환
                import_response = requests.post("http://delivery-service:5000/api/delivery/import")
                assign_response = requests.post("http://delivery-service:5000/api/delivery/assign")
                
                return jsonify({
                    "completed": True,
                    "message": "All pickups completed and converted to delivery",
                    "total_converted": total_completed,
                    "import_status": import_response.status_code,
                    "assign_status": assign_response.status_code
                }), 200
                
            except Exception as e:
                logging.error(f"Error converting to delivery: {e}")
                return jsonify({
                    "completed": True,
                    "error": "Failed to convert to delivery",
                    "details": str(e)
                }), 500
        else:
            return jsonify({
                "completed": True,
                "message": "No pickups today",
                "total_completed": 0
            }), 200
            
    except Exception as e:
        logging.error(f"Error checking completion: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/pickup/status')
def status():
   return jsonify({
       "status": "healthy",
       "geocoding": "kakao",
       "kakao_api_configured": bool(KAKAO_API_KEY and KAKAO_API_KEY != 'YOUR_KAKAO_API_KEY_HERE')
   })

# 디버깅용 엔드포인트 - DB 직접 확인
@app.route('/api/debug/db-check')
def check_db_connection():
   try:
       conn = get_db_connection()
       with conn.cursor() as cursor:
           cursor.execute("SELECT COUNT(*) as count FROM Parcel")
           result = cursor.fetchone()
       conn.close()
       
       return jsonify({
           "status": "success",
           "connection": "ok",
           "total_parcels": result['count'],
           "geocoding": "kakao"
       }), 200
   except Exception as e:
       return jsonify({
           "status": "error",
           "message": f"DB connection failed: {str(e)}"
       }), 500

# 🔧 디버깅용 - 카카오 지오코딩 테스트
@app.route('/api/debug/kakao-test', methods=['POST'])
def test_kakao_geocoding():
    """카카오 지오코딩 테스트 엔드포인트"""
    try:
        data = request.json
        address = data.get('address', '')
        
        if not address:
            return jsonify({"error": "address is required"}), 400
        
        # 카카오 지오코딩 테스트
        lat, lon, location_name = kakao_geocoding(address)
        
        # 구 추출 테스트
        district = extract_district_from_kakao_geocoding(address)
        
        # 기사 할당 테스트
        driver_id = DISTRICT_DRIVER_MAPPING.get(district) if district else None
        
        return jsonify({
            "input_address": address,
            "coordinates": {"lat": lat, "lon": lon},
            "location_name": location_name,
            "extracted_district": district,
            "assigned_driver": driver_id,
            "api_status": "ok" if KAKAO_API_KEY and KAKAO_API_KEY != 'YOUR_KAKAO_API_KEY_HERE' else "api_key_needed"
        }), 200
        
    except Exception as e:
        logging.error(f"카카오 지오코딩 테스트 오류: {e}")
        return jsonify({"error": str(e)}), 500

# --- 메인 실행 ---
if __name__ == "__main__":
   port = int(os.environ.get("PORT", 5000))
   host = os.environ.get("HOST", "0.0.0.0")
   
   logging.info(f"Starting TSP optimization service on {host}:{port}")
   logging.info(f"카카오 API 설정: {'OK' if KAKAO_API_KEY and KAKAO_API_KEY != 'YOUR_KAKAO_API_KEY_HERE' else 'API KEY 필요'}")
   app.run(host=host, port=port, debug=False)