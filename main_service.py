import requests
import json
import numpy as np
import logging
import os
import pymysql
from datetime import datetime, timedelta, time as datetime_time
from flask import Flask, request, jsonify
import pytz

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

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')
PICKUP_START_TIME = datetime_time(7, 0)  # 오전 7시

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
    """DB에서 직접 기사 할당 소포 목록 가져오기"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT p.*, 
                   o.name as ownerName
            FROM Parcel p
            LEFT JOIN User o ON p.ownerId = o.id
            WHERE p.pickupDriverId = %s AND p.isDeleted = 0
            ORDER BY p.createdAt DESC
            """
            cursor.execute(sql, (driver_id,))
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
                    'completedAt': completed_at,
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
    """DB에서 직접 기사 할당"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            UPDATE Parcel 
            SET pickupDriverId = %s, 
                status = 'PICKUP_PENDING', 
                isNextPickupTarget = TRUE
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
                    'completedAt': completed_at,
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

# --- 주소 처리 함수들 ---
def address_to_coordinates(address):
    """주소를 위도/경도로 변환"""
    try:
        url = f"http://{VALHALLA_HOST}:{VALHALLA_PORT}/search"
        params = {
            "text": address,
            "focus.point.lat": 37.5665,
            "focus.point.lon": 126.9780,
            "boundary.country": "KR",
            "size": 1
        }
        
        response = requests.get(url, params=params, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("features") and len(data["features"]) > 0:
                coords = data["features"][0]["geometry"]["coordinates"]
                return coords[1], coords[0]
        
        return get_default_coordinates(address)
            
    except Exception as e:
        logging.error(f"Error geocoding address: {e}")
        return get_default_coordinates(address)

def get_default_coordinates(address):
    """구별 기본 좌표"""
    district_coords = {
        "강남구": (37.5172, 127.0473),
        "서초구": (37.4837, 127.0324),
        "송파구": (37.5145, 127.1059),
        "강동구": (37.5301, 127.1238),
        "성동구": (37.5634, 127.0369),
        "광진구": (37.5384, 127.0822),
        "동대문구": (37.5744, 127.0396),
        "중랑구": (37.6063, 127.0927),
        "종로구": (37.5735, 126.9790),
        "중구": (37.5641, 126.9979),
        "용산구": (37.5311, 126.9810),
        "성북구": (37.5894, 127.0167),
        "강북구": (37.6396, 127.0253),
        "도봉구": (37.6687, 127.0472),
        "노원구": (37.6543, 127.0568),
        "은평구": (37.6176, 126.9269),
        "서대문구": (37.5791, 126.9368),
        "마포구": (37.5638, 126.9084),
        "양천구": (37.5170, 126.8667),
        "강서구": (37.5509, 126.8496),
        "구로구": (37.4954, 126.8877),
        "금천구": (37.4564, 126.8955),
        "영등포구": (37.5263, 126.8966),
        "동작구": (37.5124, 126.9393),
        "관악구": (37.4784, 126.9516)
    }
    
    for district, coords in district_coords.items():
        if district in address:
            return coords
    
    return (37.5665, 126.9780)

# --- API 엔드포인트 ---

@app.route('/api/pickup/webhook', methods=['POST'])
def webhook_new_pickup():
    """백엔드에서 새 수거 추가시 호출하는 웹훅"""
    try:
        data = request.json
        parcel_id = data.get('parcelId')
        
        if not parcel_id:
            return jsonify({"error": "parcelId is required"}), 400
        
        # DB에서 수거 정보 가져오기
        parcel = get_parcel_from_db(parcel_id)
        if not parcel:
            return jsonify({"error": "Parcel not found"}), 404
        
        # 이미 기사 할당되었는지 확인
        if parcel.get('driverId') or parcel.get('pickupDriverId'):
            return jsonify({"status": "already_processed"}), 200
        
        # 주소로 좌표 변환
        address = parcel.get('recipientAddr', '')
        lat, lon = address_to_coordinates(address)
        
        # 구 추출
        address_parts = address.split()
        district = None
        for part in address_parts:
            if part.endswith('구'):
                district = part
                break
        
        if not district:
            return jsonify({"error": "Could not determine district"}), 400
        
        # 구별로 기사 직접 할당
        driver_id = DISTRICT_DRIVER_MAPPING.get(district)
        if not driver_id:
            return jsonify({
                "status": "error",
                "message": f"No driver for district {district}"
            }), 500
        
        # DB에 기사 할당
        if assign_driver_to_parcel_in_db(parcel_id, driver_id):
            return jsonify({
                "status": "success",
                "parcelId": parcel_id,
                "district": district,        # 구만 반환
                "driverId": driver_id,       # 할당된 기사 ID
                "coordinates": {"lat": lat, "lon": lon}
            }), 200
        else:
            return jsonify({"error": "Failed to assign driver"}), 500
                
    except Exception as e:
        logging.error(f"Error processing webhook: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/pickup/next/<int:driver_id>', methods=['GET'])
def get_next_destination(driver_id):
    """기사의 다음 최적 목적지 계산"""
    try:
        # driver_id는 1-5 중 하나 (고정)
        if driver_id not in [1, 2, 3, 4, 5]:
            return jsonify({"error": "Invalid driver_id"}), 400
        
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
            
        # DB에서 기사의 미완료 수거 목록 가져오기
        parcels = get_driver_parcels_from_db(driver_id)
        pending_pickups = [p for p in parcels if p['status'] == 'PENDING']
        
        if not pending_pickups:
            # 모든 수거 완료, 허브로 복귀
            today = datetime.now(KST).strftime('%Y-%m-%d')
            completed_today = [p for p in parcels 
                             if p['status'] == 'COMPLETED' 
                             and p.get('completedAt', '').startswith(today)]
            
            if completed_today:
                last_completed = sorted(completed_today, 
                                      key=lambda x: x['completedAt'], 
                                      reverse=True)[0]
                lat, lon = address_to_coordinates(last_completed['recipientAddr'])
                current_location = {"lat": lat, "lon": lon}
            else:
                current_location = HUB_LOCATION
            
            route_info = get_turn_by_turn_route(
                current_location,
                HUB_LOCATION,
                costing=COSTING_MODEL
            )
            
            return jsonify({
                "status": "success",
                "next_destination": HUB_LOCATION,
                "route": route_info,
                "is_last": True,
                "remaining_pickups": 0
            }), 200
        
        # 현재 위치 결정
        today = datetime.now(KST).strftime('%Y-%m-%d')
        completed_today = [p for p in parcels 
                         if p['status'] == 'COMPLETED' 
                         and p.get('completedAt', '').startswith(today)]
        
        if completed_today:
            last_completed = sorted(completed_today, 
                                  key=lambda x: x['completedAt'], 
                                  reverse=True)[0]
            lat, lon = address_to_coordinates(last_completed['recipientAddr'])
            current_location = {"lat": lat, "lon": lon}
        else:
            current_location = HUB_LOCATION
        
        # TSP 계산을 위한 위치 목록
        locations = [current_location]
        for pickup in pending_pickups:
            lat, lon = address_to_coordinates(pickup['recipientAddr'])
            locations.append({
                "lat": lat,
                "lon": lon,
                "parcel_id": pickup['id'],
                "name": pickup['productName'],
                "address": pickup['recipientAddr']
            })
        
        # 매트릭스 계산
        if len(locations) > 1:
            location_coords = [{"lat": loc["lat"], "lon": loc["lon"]} for loc in locations]
            time_matrix, _ = get_time_distance_matrix(location_coords, costing=COSTING_MODEL)
            
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
                        next_idx = optimal_tour[1]
                        next_location = locations[next_idx]
                        
                        route_info = get_turn_by_turn_route(
                            current_location,
                            {"lat": next_location["lat"], "lon": next_location["lon"]},
                            costing=COSTING_MODEL
                        )
                        
                        return jsonify({
                            "status": "success",
                            "next_destination": next_location,
                            "route": route_info,
                            "is_last": False,
                            "remaining_pickups": len(pending_pickups)
                        }), 200
        
        # 가장 가까운 수거 지점으로
        next_location = locations[1] if len(locations) > 1 else HUB_LOCATION
        route_info = get_turn_by_turn_route(
            current_location,
            {"lat": next_location["lat"], "lon": next_location["lon"]},
            costing=COSTING_MODEL
        )
        
        return jsonify({
            "status": "success",
            "next_destination": next_location,
            "route": route_info,
            "is_last": False,
            "remaining_pickups": len(pending_pickups)
        }), 200
            
    except Exception as e:
        logging.error(f"Error getting next destination: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/pickup/complete', methods=['POST'])
def complete_pickup():
    """수거 완료 처리"""
    try:
        data = request.json
        parcel_id = data.get('parcelId')
        
        if not parcel_id:
            return jsonify({"error": "parcelId is required"}), 400
        
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
        
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 오늘 할당된 미완료 수거 확인
                sql_pending = """
                SELECT pickupDriverId, COUNT(*) as pending_count
                FROM Parcel
                WHERE status = 'PICKUP_PENDING' 
                AND DATE(createdAt) = CURDATE()
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
                
                # 결과 처리
                if pending_results:
                    for result in pending_results:
                        driver_id = result['pickupDriverId']
                        pending_count = result['pending_count']
                        total_pending += pending_count
                        
                        if pending_count > 0:
                            return jsonify({
                                "completed": False, 
                                "remaining": total_pending,
                                "completed_count": completed_result['completed_count'] if completed_result else 0,
                                "driver_status": f"Driver {driver_id} has {pending_count} pending"
                            }), 200
                
                # 모든 수거가 완료됨
                total_completed = completed_result['completed_count'] if completed_result else 0
                
        finally:
            conn.close()
        
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
    return jsonify({"status": "healthy"})

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
            "total_parcels": result['count']
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"DB connection failed: {str(e)}"
        }), 500

# --- 메인 실행 ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    
    logging.info(f"Starting TSP optimization service on {host}:{port}")
    app.run(host=host, port=port, debug=False)