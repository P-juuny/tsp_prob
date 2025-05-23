import requests
import json
import numpy as np
import logging
import os
import pymysql
from datetime import datetime, time as datetime_time
from flask import Flask, request, jsonify
import pytz

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

# --- 설정 ---
BACKEND_API_URL = os.environ.get("BACKEND_API_URL")  # 실제 백엔드 주소
LKH_SERVICE_URL = os.environ.get("LKH_SERVICE_URL", "http://lkh:5001/solve")
DELIVERY_START_TIME = datetime_time(15, 0)  # 오후 3시
HUB_LOCATION = {"lat": 37.5299, "lon": 126.9648, "name": "용산역"}
COSTING_MODEL = "auto"
KST = pytz.timezone('Asia/Seoul')

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

def get_driver_deliveries_from_db(driver_id):
    """DB에서 배달 기사의 배달 목록 가져오기"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
            SELECT p.*, 
                   o.name as ownerName
            FROM Parcel p
            LEFT JOIN User o ON p.ownerId = o.id
            WHERE p.deliveryDriverId = %s
            AND (p.status = 'DELIVERY_PENDING' OR p.status = 'DELIVERY_COMPLETED')
            AND p.isDeleted = 0
            ORDER BY p.isNextDeliveryTarget DESC, p.createdAt ASC
            """
            cursor.execute(sql, (driver_id,))
            deliveries = cursor.fetchall()
            
            # 날짜 필드를 문자열로 변환하고 상태값 변환
            result = []
            for p in deliveries:
                # 상태값 변환
                status = 'IN_PROGRESS' if p['status'] == 'DELIVERY_PENDING' else 'COMPLETED'
                
                # 날짜 필드 처리
                delivery_completed_at = p['deliveryCompletedAt'].isoformat() if p['deliveryCompletedAt'] else None
                pickup_completed_at = p['pickupCompletedAt'].isoformat() if p['pickupCompletedAt'] else None
                
                item = {
                    'id': p['id'],
                    'status': status,
                    'recipientAddr': p['recipientAddr'],
                    'productName': p['productName'],
                    'completedAt': delivery_completed_at,
                    'pickupCompletedAt': pickup_completed_at,
                    'ownerId': p['ownerId'],
                    'ownerName': p.get('ownerName'),
                    'size': p['size'],
                    'isNextDeliveryTarget': p['isNextDeliveryTarget'],
                    'recipientName': p['recipientName'],
                    'recipientPhone': p['recipientPhone']
                }
                result.append(item)
            
            return result
    except Exception as e:
        logging.error(f"DB 쿼리 오류: {e}")
        return []
    finally:
        conn.close()

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

# --- 주소 처리 함수들 (main_service.py에서 복사) ---
def address_to_coordinates(address):
    """주소를 위도/경도로 변환"""
    try:
        url = f"http://{os.environ.get('VALHALLA_HOST', 'traffic-proxy')}:{os.environ.get('VALHALLA_PORT', '8003')}/search"
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
                
                # 구별 통계
                address = pickup['recipientAddr']
                for part in address.split():
                    if part.endswith('구'):
                        district_stats[part] = district_stats.get(part, 0) + 1
                        break
        
        return jsonify({
            "status": "success",
            "converted": converted_count,
            "by_district": district_stats
        }), 200
        
    except Exception as e:
        logging.error(f"Error importing pickups: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/delivery/assign', methods=['POST'])
def assign_to_drivers():
    """배달 물건들을 기사에게 할당 (관리자용)"""
    try:
        # DB에서 미할당 배달 목록 가져오기
        unassigned = get_unassigned_deliveries_today_from_db()
        
        # 구별로 분류
        district_deliveries = {}
        for delivery in unassigned:
            address = delivery['recipientAddr']
            for part in address.split():
                if part.endswith('구'):
                    if part not in district_deliveries:
                        district_deliveries[part] = []
                    district_deliveries[part].append(delivery)
                    break
        
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
        
        return jsonify({"status": "success", "assignments": results}), 200
        
    except Exception as e:
        logging.error(f"Error assigning deliveries: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/delivery/next', methods=['GET'])
@auth_required
def get_next_delivery():
    """현재 기사의 다음 배달지 계산"""
    try:
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
        
        # 현재 로그인한 기사 정보
        driver_info = get_current_driver()
        driver_id = driver_info['id']
        
        # DB에서 내 배달 목록 가져오기
        my_deliveries = get_driver_deliveries_from_db(driver_id)
        pending = [d for d in my_deliveries if d['status'] == 'IN_PROGRESS']
        
        if not pending:
            # 모든 배달 완료, 허브로 복귀
            completed = [d for d in my_deliveries if d['status'] == 'COMPLETED']
            
            if completed:
                # 마지막 완료 위치에서 출발
                last = max(completed, key=lambda x: x['completedAt'])
                lat, lon = address_to_coordinates(last['recipientAddr'])
                current_location = {"lat": lat, "lon": lon}
            else:
                # 허브에서 출발
                current_location = HUB_LOCATION
            
            route = get_turn_by_turn_route(current_location, HUB_LOCATION, COSTING_MODEL)
            return jsonify({
                "status": "success",
                "next_destination": HUB_LOCATION,
                "route": route,
                "is_last": True,
                "remaining": 0
            }), 200
        
        # 현재 위치 결정
        completed = [d for d in my_deliveries if d['status'] == 'COMPLETED']
        if completed:
            last = max(completed, key=lambda x: x['completedAt'])
            lat, lon = address_to_coordinates(last['recipientAddr'])
            current_location = {"lat": lat, "lon": lon}
        else:
            current_location = HUB_LOCATION
        
        # TSP 계산
        locations = [current_location]
        for delivery in pending:
            lat, lon = address_to_coordinates(delivery['recipientAddr'])
            locations.append({
                "lat": lat,
                "lon": lon,
                "delivery_id": delivery['id'],
                "address": delivery['recipientAddr']
            })
        
        # 매트릭스 계산 후 LKH 호출
        coords = [{"lat": loc["lat"], "lon": loc["lon"]} for loc in locations]
        time_matrix, _ = get_time_distance_matrix(coords, COSTING_MODEL)
        
        if time_matrix is not None:
            # LKH 서비스 직접 호출
            response = requests.post(
                LKH_SERVICE_URL,
                json={"matrix": time_matrix.tolist()}
            )
            
            if response.status_code == 200:
                result = response.json()
                tour = result.get("tour")
                
                if tour and len(tour) > 1:
                    next_idx = tour[1]  # 현재 다음 위치
                    next_location = locations[next_idx]
                    
                    route = get_turn_by_turn_route(
                        current_location,
                        {"lat": next_location["lat"], "lon": next_location["lon"]},
                        COSTING_MODEL
                    )
                    
                    return jsonify({
                        "status": "success",
                        "next_destination": next_location,
                        "route": route,
                        "is_last": False,
                        "remaining": len(pending)
                    }), 200
        
        # 문제가 있으면 첫 번째로
        next_location = locations[1]
        route = get_turn_by_turn_route(
            current_location,
            {"lat": next_location["lat"], "lon": next_location["lon"]},
            COSTING_MODEL
        )
        
        return jsonify({
            "status": "success",
            "next_destination": next_location,
            "route": route,
            "is_last": False,
            "remaining": len(pending)
        }), 200
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/delivery/complete', methods=['POST'])
@auth_required
def complete_delivery():
    """배달 완료 처리"""
    try:
        data = request.json
        delivery_id = data.get('deliveryId')
        
        if not delivery_id:
            return jsonify({"error": "deliveryId required"}), 400
        
        # DB에서 완료 처리
        if complete_delivery_in_db(delivery_id):
            return jsonify({"status": "success"}), 200
        else:
            return jsonify({"error": "Failed to complete"}), 500
            
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/delivery/status')  
def status():
    return jsonify({"status": "healthy"})

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
            "today_counts": today_counts
        }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"DB connection failed: {str(e)}"
        }), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    host = os.environ.get("HOST", "0.0.0.0")
    
    logging.info(f"Starting delivery service on {host}:{port}")
    app.run(host=host, port=port, debug=False)