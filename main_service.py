import requests
import json
import numpy as np
import logging
import os
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

# 구역별 구 매핑
ZONE_MAPPING = {
    "강북서부": ["은평구", "서대문구", "마포구"],
    "강북동부": ["도봉구", "노원구", "강북구", "성북구"],
    "강북중부": ["종로구", "중구", "용산구"],
    "강남서부": ["강서구", "양천구", "구로구", "영등포구", "동작구", "관악구", "금천구"],
    "강남동부": ["성동구", "광진구", "동대문구", "중랑구", "강동구", "송파구", "강남구", "서초구"]
}

# Flask 앱 설정
app = Flask(__name__)

# --- 백엔드 API 호출 함수들 ---
def get_parcel_from_backend(parcel_id):
    """백엔드에서 수거 정보 가져오기"""
    try:
        response = requests.get(f"{BACKEND_API_URL}/api/parcel/{parcel_id}")
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        logging.error(f"Error getting parcel from backend: {e}")
        return None

def assign_driver_to_parcel(parcel_id, driver_id):
    """백엔드에 기사 할당 요청"""
    try:
        payload = {"driverId": driver_id}
        response = requests.put(
            f"{BACKEND_API_URL}/api/parcel/{parcel_id}/assign",
            json=payload
        )
        return response.status_code == 200
    except Exception as e:
        logging.error(f"Error assigning driver: {e}")
        return False

def complete_parcel(parcel_id):
    """백엔드에 수거 완료 요청"""
    try:
        response = requests.put(
            f"{BACKEND_API_URL}/api/parcel/{parcel_id}/complete"
        )
        return response.status_code == 200
    except Exception as e:
        logging.error(f"Error completing parcel: {e}")
        return False

def get_driver_parcels(driver_id):
    """기사에게 할당된 수거 목록 가져오기"""
    try:
        response = requests.get(
            f"{BACKEND_API_URL}/api/driver/{driver_id}/parcels"
        )
        if response.status_code == 200:
            return response.json()
        return []
    except Exception as e:
        logging.error(f"Error getting driver parcels: {e}")
        return []

def get_zone_driver(zone):
    """구역별 기사 정보 가져오기"""
    try:
        response = requests.get(
            f"{BACKEND_API_URL}/api/zone/{zone}/driver"
        )
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        logging.error(f"Error getting zone driver: {e}")
        return None

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

def determine_zone_by_district(district):
    """구 이름으로 구역 결정"""
    for zone, districts in ZONE_MAPPING.items():
        if district in districts:
            return zone
    return None

# --- API 엔드포인트 ---

@app.route('/api/pickup/webhook', methods=['POST'])
def webhook_new_pickup():
    """백엔드에서 새 수거 추가시 호출하는 웹훅
    
    백엔드가 이미 DB에 저장한 수거 정보:
    - id: 고유번호
    - ownerId: 매장 ID
    - recipientAddr: 수신자 주소
    - pickupDate: 수거 예정일 (백엔드가 판단)
      - 7시 이전: 다음날 7시
      - 7시-12시: 당일
      - 12시 이후: 다음날 7시
    - status: PENDING
    - driverId: NULL (아직 미할당)
    """
    try:
        data = request.json
        parcel_id = data.get('parcelId')
        
        if not parcel_id:
            return jsonify({"error": "parcelId is required"}), 400
        
        # 백엔드에서 수거 정보 가져오기
        parcel = get_parcel_from_backend(parcel_id)
        if not parcel:
            return jsonify({"error": "Parcel not found"}), 404
        
        # 이미 기사 할당되었는지 확인
        if parcel.get('driverId'):
            return jsonify({"status": "already_processed"}), 200
        
        # 주소로 구역 결정
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
        
        zone = determine_zone_by_district(district)
        if not zone:
            return jsonify({"error": "Invalid district"}), 400
        
        # 해당 구역 기사 정보 가져오기
        driver_info = get_zone_driver(zone)
        if not driver_info:
            return jsonify({
                "status": "error",
                "message": f"No driver assigned to zone {zone}"
            }), 500
        
        # 백엔드에 기사 할당 요청
        if assign_driver_to_parcel(parcel_id, driver_info['id']):
            return jsonify({
                "status": "success",
                "parcelId": parcel_id,
                "zone": zone,
                "driverId": driver_info['id'],
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
        # 백엔드에서 기사의 미완료 수거 목록 가져오기
        parcels = get_driver_parcels(driver_id)
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
        
        # 백엔드에 완료 요청
        if complete_parcel(parcel_id):
            return jsonify({"status": "success"}), 200
        else:
            return jsonify({"error": "Failed to complete pickup"}), 500
            
    except Exception as e:
        logging.error(f"Error completing pickup: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/pickup/status')
def status():
    return jsonify({"status": "healthy"})

# --- 메인 실행 ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    
    logging.info(f"Starting TSP optimization service on {host}:{port}")
    app.run(host=host, port=port, debug=False)