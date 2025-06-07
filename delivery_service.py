import requests
import json
import numpy as np
import logging
import os
import pymysql
from datetime import datetime, time as datetime_time
from flask import Flask, request, jsonify
import pytz
import polyline

# 인증 관련
from auth import auth_required, get_current_driver

# LKH 및 Valhalla 
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

driver_hub_status = {}

# --- 설정 ---
BACKEND_API_URL = os.environ.get("BACKEND_API_URL")  # 실제 백엔드 주소
LKH_SERVICE_URL = os.environ.get("LKH_SERVICE_URL", "http://lkh:5001/solve")
DELIVERY_START_TIME = datetime_time(15, 0)  # 오후 3시
HUB_LOCATION = {"lat": 37.5299, "lon": 126.9648, "name": "용산역"}
COSTING_MODEL = "auto"
KST = pytz.timezone('Asia/Seoul')

# 🔧 카카오 API 설정
KAKAO_API_KEY = os.environ.get('KAKAO_API_KEY', 'YOUR_KAKAO_API_KEY_HERE')
KAKAO_ADDRESS_API = "https://dapi.kakao.com/v2/local/search/address.json"
KAKAO_KEYWORD_API = "https://dapi.kakao.com/v2/local/search/keyword.json"

# 구별 기사 직접 매핑 (배달 기사 6-10)
DISTRICT_DRIVER_MAPPING = {
    # 강북서부 (driver_id: 6)
    "은평구": 6, "서대문구": 6, "마포구": 6,
    
    # 강북동부 (driver_id: 7)
    "도봉구": 7, "노원구": 7, "강북구": 7, "성북구": 7,
    
    # 강북중부 (driver_id: 8)
    "종로구": 8, "중구": 8, "용산구": 8,
    
    # 강남서부 (driver_id: 9)
    "강서구": 9, "양천구": 9, "구로구": 9, "영등포구": 9, 
    "동작구": 9, "관악구": 9, "금천구": 9,
    
    # 강남동부 (driver_id: 10)
    "성동구": 10, "광진구": 10, "동대문구": 10, "중랑구": 10, 
    "강동구": 10, "송파구": 10, "강남구": 10, "서초구": 10
}

# Flask 앱 설정
app = Flask(__name__)

# 🔧 실시간 교통정보 반영을 위한 함수들 (수거와 동일)
def get_traffic_weight_by_time():
    """현재 시간대에 따른 교통 가중치 반환"""
    current_time = datetime.now(KST).time()
    current_hour = current_time.hour
    
    # 시간대별 교통량 패턴 반영
    if 7 <= current_hour <= 9:  # 출근 러시아워
        return 1.6
    elif 12 <= current_hour <= 13:  # 점심시간
        return 1.3
    elif 18 <= current_hour <= 20:  # 퇴근 러시아워
        return 1.7
    elif 21 <= current_hour <= 23:  # 저녁 시간
        return 1.2
    elif 0 <= current_hour <= 6:  # 새벽 시간
        return 0.7
    else:  # 평상시
        return 1.0

def get_district_traffic_weight(address):
    """구별 교통 복잡도에 따른 가중치 반환"""
    # 교통 복잡 지역
    complex_districts = ["강남구", "서초구", "종로구", "중구", "마포구", "영등포구"]
    # 중간 복잡 지역
    medium_districts = ["송파구", "강동구", "성동구", "광진구", "용산구", "서대문구"]
    
    for district in complex_districts:
        if district in address:
            return 1.4
    
    for district in medium_districts:
        if district in address:
            return 1.2
    
    return 1.0  # 기본값

def apply_traffic_weights_to_matrix(time_matrix, locations):
    """매트릭스에 실시간 교통 가중치 적용"""
    if time_matrix is None or len(locations) == 0:
        return time_matrix
    
    # 시간대별 기본 가중치
    time_weight = get_traffic_weight_by_time()
    
    # 각 구간별로 가중치 적용
    weighted_matrix = time_matrix.copy()
    
    for i in range(len(locations)):
        for j in range(len(locations)):
            if i != j:
                # 출발지와 도착지의 구별 가중치 평균
                start_weight = get_district_traffic_weight(locations[i].get('address', ''))
                end_weight = get_district_traffic_weight(locations[j].get('address', ''))
                district_weight = (start_weight + end_weight) / 2
                
                # 최종 가중치 = 시간대 가중치 × 구별 가중치
                final_weight = time_weight * district_weight
                
                # 매트릭스에 가중치 적용
                weighted_matrix[i][j] *= final_weight
    
    logging.info(f"교통 가중치 적용 완료 - 시간대: {time_weight:.2f}, 현재시간: {datetime.now(KST).strftime('%H:%M')}")
    return weighted_matrix

def get_enhanced_time_distance_matrix(locations, costing="auto"):
    """교통정보가 반영된 매트릭스 생성"""
    # 기본 매트릭스 계산 (traffic-proxy를 통해 어느 정도 실시간 정보 반영됨)
    time_matrix, distance_matrix = get_time_distance_matrix(locations, costing=costing, use_traffic=True)
    
    if time_matrix is not None:
        # 🔧 추가 교통 가중치 적용
        enhanced_locations = []
        for i, loc in enumerate(locations):
            enhanced_loc = {
                'lat': loc['lat'],
                'lon': loc['lon'],
                'address': loc.get('address', ''),
                'name': loc.get('name', f'위치{i+1}')
            }
            enhanced_locations.append(enhanced_loc)
        
        # 실시간 교통 패턴 반영
        time_matrix = apply_traffic_weights_to_matrix(time_matrix, enhanced_locations)
    
    return time_matrix, distance_matrix

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

def get_completed_pickups_today_from_db():
    """DB에서 오늘 완료된 수거 목록 가져오기"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT p.*, 
                   o.name as ownerName, 
                   pd.name as pickupDriverName
            FROM Parcel p
            LEFT JOIN User o ON p.ownerId = o.id
            LEFT JOIN User pd ON p.pickupDriverId = pd.id
            WHERE p.status = 'PICKUP_COMPLETED' 
            AND DATE(p.pickupCompletedAt) = CURDATE()
            AND p.isDeleted = 0
            AND p.deliveryDriverId IS NULL
            """
            cursor.execute(sql)
            parcels = cursor.fetchall()
            
            # 날짜 필드를 문자열로 변환
            for p in parcels:
                for key, value in p.items():
                    if isinstance(value, datetime):
                        p[key] = value.isoformat()
            
            return parcels
    except Exception as e:
        logging.error(f"DB 쿼리 오류: {e}")
        return []
    finally:
        conn.close()

def get_unassigned_deliveries_today_from_db():
    """DB에서 오늘 미할당 배달 목록 가져오기"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT p.*, 
                   o.name as ownerName
            FROM Parcel p
            LEFT JOIN User o ON p.ownerId = o.id
            WHERE p.status = 'DELIVERY_PENDING' 
            AND deliveryDriverId IS NULL
            AND DATE(p.pickupCompletedAt) = CURDATE()
            AND p.isDeleted = 0
            """
            cursor.execute(sql)
            deliveries = cursor.fetchall()
            
            # 날짜 필드를 문자열로 변환
            for p in deliveries:
                for key, value in p.items():
                    if isinstance(value, datetime):
                        p[key] = value.isoformat()
            
            return deliveries
    except Exception as e:
        logging.error(f"DB 쿼리 오류: {e}")
        return []
    finally:
        conn.close()

# ✅ 수정된 함수: 실시간으로 미완료 배달만 가져오기 (수거 코드와 동일한 패턴)
def get_real_pending_deliveries(driver_id):
    """실시간으로 미완료 배달만 가져오기"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            today = datetime.now(KST).date()
            # ✅ 명확하게 미완료 상태만 쿼리
            sql = """
            SELECT p.*, 
                   o.name as ownerName
            FROM Parcel p
            LEFT JOIN User o ON p.ownerId = o.id
            WHERE p.deliveryDriverId = %s 
            AND p.status = 'DELIVERY_PENDING'  -- ✅ 미완료만
            AND p.isDeleted = 0
            ORDER BY p.createdAt DESC
            """
            cursor.execute(sql, (driver_id,))
            parcels = cursor.fetchall()
            
            # API 응답 형식에 맞게 변환
            result = []
            for p in parcels:
                # 날짜 필드 처리
                completed_at = p['deliveryCompletedAt'].isoformat() if p['deliveryCompletedAt'] else None
                created_at = p['createdAt'].isoformat() if p['createdAt'] else None
                
                item = {
                    'id': p['id'],
                    'status': 'IN_PROGRESS',  # DELIVERY_PENDING -> IN_PROGRESS
                    'productName': p['productName'],
                    'recipientName': p['recipientName'],
                    'recipientPhone': p['recipientPhone'],
                    'recipientAddr': p['recipientAddr'],
                    'deliveryCompletedAt': completed_at,
                    'createdAt': created_at,
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

# ✅ 수정된 함수: 현재 기사 위치 정확히 계산 (수거 코드와 동일)
def get_current_driver_location(driver_id):
    """현재 기사 위치 정확히 계산"""
    
    # 1. 허브 도착 완료 상태면 허브 위치
    if driver_hub_status.get(driver_id, False):
        logging.info(f"배달 기사 {driver_id} 허브 도착 완료 상태")
        return HUB_LOCATION
    
    # 2. 오늘 완료된 마지막 배달 위치
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT recipientAddr, deliveryCompletedAt
            FROM Parcel
            WHERE deliveryDriverId = %s 
            AND status = 'DELIVERY_COMPLETED'
            AND DATE(deliveryCompletedAt) = CURDATE()
            AND isDeleted = 0
            ORDER BY deliveryCompletedAt DESC
            LIMIT 1
            """
            cursor.execute(sql, (driver_id,))
            last_completed = cursor.fetchone()
            
            if last_completed:
                address = last_completed['recipientAddr']
                lat, lon, _ = kakao_geocoding(address)
                logging.info(f"배달 기사 {driver_id} 현재 위치: {address} -> ({lat}, {lon})")
                return {"lat": lat, "lon": lon}
    
    except Exception as e:
        logging.error(f"현재 위치 계산 오류: {e}")
    finally:
        conn.close()
    
    # 3. 기본값: 허브 (아직 배달 시작 안 함)
    logging.info(f"배달 기사 {driver_id} 기본 위치: 허브")
    return HUB_LOCATION
        
def convert_pickup_to_delivery_in_db(pickup_id):
    """DB에서 수거를 배달로 전환"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            UPDATE Parcel 
            SET status = 'DELIVERY_PENDING' 
            WHERE id = %s 
            AND status = 'PICKUP_COMPLETED'
            AND isDeleted = 0
            """
            cursor.execute(sql, (pickup_id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logging.error(f"DB 쿼리 오류: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def assign_delivery_driver_in_db(delivery_id, driver_id):
    """DB에서 배달 기사 할당"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            UPDATE Parcel 
            SET deliveryDriverId = %s,
                isNextDeliveryTarget = TRUE
            WHERE id = %s 
            AND status = 'DELIVERY_PENDING'
            AND isDeleted = 0
            """
            cursor.execute(sql, (driver_id, delivery_id))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logging.error(f"DB 쿼리 오류: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def complete_delivery_in_db(delivery_id):
    """DB에서 배달 완료 처리"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            UPDATE Parcel 
            SET status = 'DELIVERY_COMPLETED',
                isNextDeliveryTarget = FALSE,
                deliveryCompletedAt = NOW()
            WHERE id = %s 
            AND status = 'DELIVERY_PENDING'
            AND isDeleted = 0
            """
            cursor.execute(sql, (delivery_id,))
        conn.commit()
        return cursor.rowcount > 0
    except Exception as e:
        logging.error(f"DB 쿼리 오류: {e}")
        conn.rollback()
        return False
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

# ✅ 수정된 TSP 최적화 함수 (수거 코드에서 가져옴)
def calculate_optimal_next_destination(locations, current_location):
   """TSP로 최적 다음 목적지 계산"""
   try:
       # 교통정보 반영된 매트릭스 생성
       location_coords = [{"lat": loc["lat"], "lon": loc["lon"]} for loc in locations]
       time_matrix, _ = get_enhanced_time_distance_matrix(location_coords, costing=COSTING_MODEL)
       
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
                   # ✅ 수정: 현재 위치(0번 인덱스) 제외하고 다음 목적지 선택
                   next_idx = None
                   for idx in optimal_tour[1:]:  # 첫 번째(현재위치) 제외
                       if idx != 0:  # 현재위치가 아닌 것만
                           next_idx = idx
                           break
                   
                   # 현재위치가 아닌 목적지를 찾지 못한 경우 fallback
                   if next_idx is None and len(locations) > 1:
                       next_idx = 1  # 첫 번째 배달 지점
                   
                   if next_idx is not None:
                       next_location = locations[next_idx]
                       
                       # 경로 계산
                       route_info = get_turn_by_turn_route(
                           current_location,  # 현재 위치
                           {"lat": next_location["lat"], "lon": next_location["lon"]},
                           costing=COSTING_MODEL
                       )
                       
                       # waypoints 및 coordinates 추출
                       waypoints, coordinates = extract_waypoints_from_route(route_info)
                       if not waypoints:
                           # 기본 waypoints
                           waypoints = [
                               {
                                   "lat": current_location["lat"],
                                   "lon": current_location["lon"],
                                   "name": "현재위치",
                                   "instruction": "배달 시작"
                               },
                               {
                                   "lat": next_location["lat"],
                                   "lon": next_location["lon"],
                                   "name": next_location["name"],
                                   "instruction": "목적지 도착"
                               }
                           ]
                           coordinates = [
                               {"lat": current_location["lat"], "lon": current_location["lon"]},
                               {"lat": next_location["lat"], "lon": next_location["lon"]}
                           ]
                       
                       # route에 waypoints와 coordinates 추가
                       if route_info and 'trip' in route_info:
                           route_info['waypoints'] = waypoints
                           route_info['coordinates'] = coordinates
                       
                       return next_location, route_info, "LKH_TSP"
       
       # Fallback: 가장 가까운 지점
       next_location = locations[1] if len(locations) > 1 else locations[0]
       route_info = get_turn_by_turn_route(
           current_location,
           {"lat": next_location["lat"], "lon": next_location["lon"]},
           costing=COSTING_MODEL
       )
       
       # waypoints 추가
       waypoints, coordinates = extract_waypoints_from_route(route_info)
       if route_info and 'trip' in route_info:
           route_info['waypoints'] = waypoints
           route_info['coordinates'] = coordinates
       
       return next_location, route_info, "nearest"
       
   except Exception as e:
       logging.error(f"TSP 계산 오류: {e}")
       fallback_location = locations[1] if len(locations) > 1 else locations[0]
       return fallback_location, None, "fallback"

# --- API 엔드포인트 ---

@app.route('/api/delivery/import', methods=['POST'])
def import_todays_pickups():
    """오늘 수거 완료된 것들을 배달로 전환 (관리자용)"""
    try:
        # DB에서 오늘 완료된 수거 목록 가져오기
        completed_pickups = get_completed_pickups_today_from_db()
        
        # 각 수거를 배달로 전환
        converted_count = 0
        district_stats = {}  # 구별 통계
        
        for pickup in completed_pickups:
            # DB에서 배달로 전환
            if convert_pickup_to_delivery_in_db(pickup['id']):
                converted_count += 1
                
                # 🔧 카카오 API로 구별 통계
                address = pickup['recipientAddr']
                district = extract_district_from_kakao_geocoding(address)
                if district:
                    district_stats[district] = district_stats.get(district, 0) + 1
                else:
                    # fallback: 텍스트에서 직접 추출
                    for part in address.split():
                        if part.endswith('구'):
                            district_stats[part] = district_stats.get(part, 0) + 1
                            break
        
        return jsonify({
            "status": "success",
            "converted": converted_count,
            "by_district": district_stats,
            "geocoding_method": "kakao"
        }), 200
        
    except Exception as e:
        logging.error(f"Error importing pickups: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/delivery/assign', methods=['POST'])
def assign_to_drivers():
    """배달 물건들을 기사에게 할당 (관리자용) - 카카오 API 사용"""
    try:
        # DB에서 미할당 배달 목록 가져오기
        unassigned = get_unassigned_deliveries_today_from_db()
        
        # 🔧 카카오 API로 구별 분류
        district_deliveries = {}
        for delivery in unassigned:
            address = delivery['recipientAddr']
            
            # 카카오 API로 정확한 구 정보 추출
            district = extract_district_from_kakao_geocoding(address)
            
            if not district:
                # fallback: 텍스트에서 직접 추출
                for part in address.split():
                    if part.endswith('구'):
                        district = part
                        break
            
            if district:
                if district not in district_deliveries:
                    district_deliveries[district] = []
                district_deliveries[district].append(delivery)
            else:
                logging.warning(f"구 정보 추출 실패: {address}")
        
        # 각 구의 기사에게 할당
        results = {}
        for district, deliveries in district_deliveries.items():
            # 구별 기사 ID 가져오기
            driver_id = DISTRICT_DRIVER_MAPPING.get(district)
            
            if driver_id:
                # 배달 할당
                assign_count = 0
                for delivery in deliveries:
                    if assign_delivery_driver_in_db(delivery['id'], driver_id):
                        assign_count += 1
                
                results[district] = {
                    "driver_id": driver_id,
                    "count": assign_count
                }
            else:
                logging.warning(f"해당 구에 대응하는 배달 기사 없음: {district}")
        
        return jsonify({
            "status": "success", 
            "assignments": results,
            "geocoding_method": "kakao"
        }), 200
        
    except Exception as e:
        logging.error(f"Error assigning deliveries: {e}")
        return jsonify({"error": str(e)}), 500

# ✅ 수정된 메인 함수: get_next_delivery (수거 코드 적용)
@app.route('/api/delivery/next', methods=['GET'])
@auth_required
def get_next_delivery():
    """현재 기사의 다음 배달지 계산 (수거 API 구조 참고)"""
    try:
        # 현재 로그인한 기사 정보
        driver_info = get_current_driver()
        driver_id = driver_info['user_id']  # ✅ user_id 사용 (수거와 동일)
        
        # 시간 체크 추가
        current_time = datetime.now(KST).time()
        if current_time < DELIVERY_START_TIME:  # 오후 3시 이전
            hours_left = DELIVERY_START_TIME.hour - current_time.hour
            minutes_left = DELIVERY_START_TIME.minute - current_time.minute
            if minutes_left < 0:
                hours_left -= 1
                minutes_left += 60
            
            return jsonify({
                "status": "waiting",
                "message": f"배달은 오후 3시부터 시작됩니다. {hours_left}시간 {minutes_left}분 남았습니다.",
                "start_time": "15:00",
                "current_time": current_time.strftime("%H:%M")
            }), 200
        
        # ✅ 실시간으로 미완료 배달만 가져오기
        pending_deliveries = get_real_pending_deliveries(driver_id)
        
        # ✅ 현재 위치 정확히 계산
        current_location = get_current_driver_location(driver_id)
        
        # 미완료 배달이 없을 때
        if not pending_deliveries:
            current_time = datetime.now(KST).time()
            
            # 🔧 이미 허브에 있다면
            if driver_hub_status.get(driver_id, False):
                return jsonify({
                    "status": "at_hub",
                    "message": "허브에 도착했습니다. 수고하셨습니다!",
                    "current_location": current_location,
                    "remaining": 0,
                    "is_last": True
                }), 200
            
            # 🔧 모든 배달 완료면 허브 복귀
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
                "message": "모든 배달이 완료되었습니다. 허브로 복귀해주세요.",
                "next_destination": HUB_LOCATION,
                "route": route_info,
                "is_last": True,
                "remaining": 0,
                "current_location": current_location,
                "distance_to_hub": route_info['trip']['summary']['length'] if route_info else 0
            }), 200
        
        # 🔧 새로운 배달이 시작되면 허브 상태 리셋
        if pending_deliveries and driver_hub_status.get(driver_id, False):
            driver_hub_status[driver_id] = False
            logging.info(f"배달 기사 {driver_id} 새로운 배달 시작으로 허브 상태 리셋")
        
        # ✅ 실시간 교통정보가 반영된 TSP 계산
        # locations[0] = 현재 위치 (시작점)
        # locations[1:] = 미완료 배달 지점들만
        locations = [current_location]
        for delivery in pending_deliveries:
            # 카카오 지오코딩으로 정확한 좌표 계산
            lat, lon, location_name = kakao_geocoding(delivery['recipientAddr'])
            locations.append({
                "lat": lat,
                "lon": lon,
                "delivery_id": delivery['id'],
                "parcelId": str(delivery['id']),  # 🔧 parcelId 추가!
                "name": delivery.get('productName', ''),
                "productName": delivery.get('productName', ''),
                "address": delivery['recipientAddr'],
                "location_name": location_name,
                "recipientName": delivery.get('recipientName', ''),
                "recipientPhone": delivery.get('recipientPhone', '')
            })
        
        # ✅ TSP 최적화 - 현재 위치에서 시작하는 최적 경로
        if len(locations) > 1:
            next_location, route_info, algorithm = calculate_optimal_next_destination(locations, current_location)
            
            return jsonify({
                "status": "success",
                "next_destination": {
                    "lat": next_location["lat"],
                    "lon": next_location["lon"],
                    "delivery_id": next_location.get("delivery_id"),
                    "parcelId": next_location.get("parcelId"),  # 🔧 parcelId!
                    "name": next_location.get("productName"),
                    "productName": next_location.get("productName"),
                    "address": next_location.get("address"),
                    "location_name": next_location.get("location_name"),
                    "recipientName": next_location.get("recipientName"),
                    "recipientPhone": next_location.get("recipientPhone")
                },
                "route": route_info,
                "is_last": False,
                "remaining": len(pending_deliveries),
                "current_location": current_location,
                "algorithm_used": algorithm,
                "geocoding_method": "kakao",
                "traffic_info": {
                    "time_weight": get_traffic_weight_by_time(),
                    "current_hour": datetime.now(KST).hour
                }
            }), 200
        
        # Fallback: 단일 배달 지점
        next_location = locations[1] if len(locations) > 1 else HUB_LOCATION
        route_info = get_turn_by_turn_route(
            current_location,
            {"lat": next_location["lat"], "lon": next_location["lon"]},
            costing=COSTING_MODEL
        )
        
        # waypoints 추가
        waypoints, coordinates = extract_waypoints_from_route(route_info)
        if route_info and 'trip' in route_info:
            route_info['waypoints'] = waypoints
            route_info['coordinates'] = coordinates
        
        return jsonify({
            "status": "success",
            "next_destination": next_location,
            "route": route_info,
            "is_last": False,
            "remaining": len(pending_deliveries),
            "current_location": current_location,
            "geocoding_method": "kakao"
        }), 200
        
    except Exception as e:
        logging.error(f"Error getting next delivery: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

# ✅ 수정된 complete_delivery 함수 (수거 코드와 동일한 패턴)        
@app.route('/api/delivery/complete', methods=['POST'])
@auth_required
def complete_delivery():
    """배달 완료 처리"""
    try:
        # 현재 로그인한 기사 확인
        driver_info = get_current_driver()
        driver_id = driver_info['user_id']  # ✅ user_id 사용
        
        data = request.json
        delivery_id = data.get('deliveryId')
        
        if not delivery_id:
            return jsonify({"error": "deliveryId required"}), 400
        
        # 해당 배달이 현재 기사에게 할당되었는지 확인
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT deliveryDriverId FROM Parcel WHERE id = %s", (delivery_id,))
                parcel = cursor.fetchone()
                
                if not parcel or parcel['deliveryDriverId'] != driver_id:
                    return jsonify({"error": "권한이 없습니다"}), 403
        finally:
            conn.close()
        
        # ✅ DB에서 완료 처리
        if complete_delivery_in_db(delivery_id):
            logging.info(f"배달 완료: 기사 {driver_id}, 배달 {delivery_id}")
            
            # ✅ 완료 후 남은 미완료 배달 개수 실시간 확인
            remaining_deliveries = get_real_pending_deliveries(driver_id)
            
            return jsonify({
                "status": "success",
                "message": "배달이 완료되었습니다",
                "remaining": len(remaining_deliveries),
                "completed_at": datetime.now(KST).isoformat()
            }), 200
        else:
            return jsonify({"error": "완료 처리 실패"}), 500
            
    except Exception as e:
        logging.error(f"배달 완료 오류: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/delivery/status')  
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
            # 수거/배달 상태별 통계
            cursor.execute("""
                SELECT status, COUNT(*) as count 
                FROM Parcel 
                WHERE isDeleted = 0
                GROUP BY status
            """)
            status_counts = cursor.fetchall()
            
            # 오늘 날짜 조회
            cursor.execute("SELECT CURDATE() as today")
            today = cursor.fetchone()
            
            # 오늘 완료된 수거/배달 건수
            cursor.execute("""
                SELECT 
                    COUNT(CASE WHEN status = 'PICKUP_COMPLETED' AND DATE(pickupCompletedAt) = CURDATE() THEN 1 END) as pickup_completed,
                    COUNT(CASE WHEN status = 'DELIVERY_COMPLETED' AND DATE(deliveryCompletedAt) = CURDATE() THEN 1 END) as delivery_completed
                FROM Parcel
                WHERE isDeleted = 0
            """)
            today_counts = cursor.fetchone()
        
        conn.close()
        
        return jsonify({
            "status": "success",
            "connection": "ok",
            "today": today['today'].isoformat() if today else None,
            "status_counts": status_counts,
            "today_counts": today_counts,
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    host = os.environ.get("HOST", "0.0.0.0")
    
    logging.info(f"Starting delivery service on {host}:{port}")
    logging.info(f"카카오 API 설정: {'OK' if KAKAO_API_KEY and KAKAO_API_KEY != 'YOUR_KAKAO_API_KEY_HERE' else 'API KEY 필요'}")
    app.run(host=host, port=port, debug=False)