import time
import json
import copy
import requests
import numpy as np
import logging
import os
from datetime import datetime, timedelta, time as datetime_time
from flask import Flask, request, jsonify
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

# 다른 스크립트에서 함수 임포트
from get_valhalla_matrix import get_time_distance_matrix
from get_valhalla_route import get_turn_by_turn_route
from database import get_db, get_db_session
from models import User, DriverInfo, Parcel, StoreInfo, UserType, DeliveryStatus
from auth import auth_required, get_current_driver, login

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pickup_service.log"),
        logging.StreamHandler()
    ]
)

# --- 설정 ---
HUB_LOCATION = {"lat": 37.5299, "lon": 126.9648, "name": "용산역"}
COSTING_MODEL = "auto"
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

# 스케줄러 설정
scheduler = BackgroundScheduler()

# --- 주소를 위도/경도로 변환하는 함수 (Valhalla 사용) ---
def address_to_coordinates(address):
    """주소를 위도/경도로 변환 (Valhalla geocoding 사용)"""
    try:
        # Valhalla search API 사용
        url = f"http://{VALHALLA_HOST}:{VALHALLA_PORT}/search"
        params = {
            "text": address,
            "focus.point.lat": 37.5665,  # 서울 중심
            "focus.point.lon": 126.9780,
            "boundary.country": "KR",
            "size": 1
        }
        
        response = requests.get(url, params=params, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("features") and len(data["features"]) > 0:
                coords = data["features"][0]["geometry"]["coordinates"]
                lat = coords[1]
                lon = coords[0]
                logging.info(f"Address geocoded: {address} -> ({lat}, {lon})")
                return lat, lon
        
        # Valhalla가 실패하면 구별 기본 좌표 사용
        logging.warning(f"Valhalla geocoding failed for: {address}")
        return get_default_coordinates(address)
            
    except Exception as e:
        logging.error(f"Error geocoding address: {e}")
        return get_default_coordinates(address)

def get_default_coordinates(address):
    """구별 기본 좌표 반환"""
    # 서울시 구별 중심 좌표
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
    
    # 주소에서 구 이름 찾기
    for district, coords in district_coords.items():
        if district in address:
            return coords
    
    # 기본값: 서울시청
    return (37.5665, 126.9780)

# --- 공통 함수 ---
def determine_zone_by_district(district: str) -> str:
    """구 이름으로 구역 결정"""
    for zone, districts in ZONE_MAPPING.items():
        if district in districts:
            return zone
    return "강남동부"  # 기본값

def is_realtime_period():
    """현재 시간이 실시간 처리 시간대(오전 7시~12시)인지 확인"""
    now = datetime.now(KST)
    current_time = now.time()
    return datetime_time(7, 0) <= current_time < datetime_time(12, 0)

def is_next_day_pickup(request_time=None):
    """수거 요청이 다음날 수거 대상인지 확인"""
    if request_time is None:
        request_time = datetime.now(KST)
    
    current_time = request_time.time()
    
    # 오전 7시 이전 또는 오후 12시 이후 요청은 다음날 수거
    if current_time < datetime_time(7, 0) or current_time >= datetime_time(12, 0):
        return True
    
    return False

# --- API 엔드포인트 ---
@app.route('/api/login', methods=['POST'])
def driver_login():
    """기사 로그인"""
    return login()

@app.route('/api/pickup/add', methods=['POST'])
@auth_required
def add_pickup():
    """새로운 수거 요청 추가"""
    try:
        data = request.json
        required_fields = ['productName', 'recipientName', 'recipientPhone', 'recipientAddr']
        
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400
        
        with get_db() as db:
            # 현재 로그인한 사용자 정보
            user = db.query(User).filter(User.id == request.current_user_id).first()
            if not user:
                return jsonify({"error": "사용자를 찾을 수 없습니다"}), 404
            
            # 매장 사용자만 수거 요청 생성 가능
            if user.userType != UserType.STORE:
                return jsonify({"error": "매장 계정만 수거 요청을 생성할 수 있습니다"}), 403
            
            # 주소를 위도/경도로 변환
            lat, lon = address_to_coordinates(data['recipientAddr'])
            
            # 수거 요청 생성
            now = datetime.now(KST)
            next_day = is_next_day_pickup(now)
            
            pickup_date = now
            if next_day:
                # 다음날 오전 7시로 설정
                pickup_date = now.replace(hour=7, minute=0, second=0) + timedelta(days=1)
            
            new_parcel = Parcel(
                ownerId=user.id,
                productName=data['productName'],
                size=data.get('size', '보통'),
                caution=data.get('caution', False),
                recipientName=data['recipientName'],
                recipientPhone=data['recipientPhone'],
                recipientAddr=data['recipientAddr'],
                detailAddress=data.get('detailAddress', ''),
                status=DeliveryStatus.PENDING,
                pickupDate=pickup_date
            )
            
            db.add(new_parcel)
            db.commit()
            db.refresh(new_parcel)
            
            # 구역 결정 (주소에서 구 추출)
            address_parts = data['recipientAddr'].split()
            district = None
            for part in address_parts:
                if part.endswith('구'):
                    district = part
                    break
            
            zone = determine_zone_by_district(district) if district else "미지정"
            
            return jsonify({
                "status": "success",
                "message": f"수거 요청이 {'내일' if next_day else '오늘'} 추가되었습니다",
                "parcelId": new_parcel.id,
                "zone": zone,
                "pickupDate": pickup_date.isoformat(),
                "coordinates": {"lat": lat, "lon": lon}
            }), 201
            
    except Exception as e:
        logging.error(f"Error adding pickup request: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/pickup/next', methods=['GET'])
@auth_required
def get_next_destination():
    """기사의 다음 최적 목적지 계산"""
    try:
        with get_db() as db:
            driver = get_current_driver(db)
            if not driver or driver["user"].userType != UserType.DRIVER:
                return jsonify({"error": "기사만 접근 가능합니다"}), 403
            
            driver_info = driver["driver_info"]
            
            # 해당 기사 구역의 구 리스트
            zone_districts = []
            for zone, districts in ZONE_MAPPING.items():
                if zone == driver_info.regionCity:  # regionCity가 구역명
                    zone_districts = districts
                    break
            
            # 해당 구역의 미완료 수거 조회
            pending_pickups = []
            all_pending = db.query(Parcel).filter(
                and_(
                    Parcel.status == DeliveryStatus.PENDING,
                    Parcel.pickupDate <= datetime.now(KST),
                    Parcel.trackingCode.notlike("DEL-%")  # 배달 건 제외
                )
            ).all()
            
            for parcel in all_pending:
                # 주소에서 구 추출
                address_parts = parcel.recipientAddr.split()
                for part in address_parts:
                    if part in zone_districts:
                        pending_pickups.append(parcel)
                        break
            
            if not pending_pickups:
                # 모든 수거 완료, 허브로 복귀
                # 마지막 완료 위치 찾기
                last_completed = db.query(Parcel).filter(
                    and_(
                        Parcel.driverId == driver_info.userId,
                        Parcel.status == DeliveryStatus.COMPLETED,
                        Parcel.completedAt >= datetime.now(KST).replace(hour=0, minute=0, second=0)
                    )
                ).order_by(Parcel.completedAt.desc()).first()
                
                if last_completed:
                    current_lat, current_lon = address_to_coordinates(last_completed.recipientAddr)
                    current_location = {"lat": current_lat, "lon": current_lon}
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
            last_completed = db.query(Parcel).filter(
                and_(
                    Parcel.driverId == driver_info.userId,
                    Parcel.status == DeliveryStatus.COMPLETED,
                    Parcel.completedAt >= datetime.now(KST).replace(hour=0, minute=0, second=0)
                )
            ).order_by(Parcel.completedAt.desc()).first()
            
            if last_completed:
                # 마지막 완료 위치가 현재 위치
                current_lat, current_lon = address_to_coordinates(last_completed.recipientAddr)
                current_location = {"lat": current_lat, "lon": current_lon}
            else:
                # 첫 수거면 허브에서 시작
                current_location = HUB_LOCATION
            
            # TSP 계산을 위한 위치 목록 (현재 위치에서 시작)
            locations = [current_location]
            for pickup in pending_pickups:
                # 각 수거지의 위도/경도 가져오기
                lat, lon = address_to_coordinates(pickup.recipientAddr)
                locations.append({
                    "lat": lat,
                    "lon": lon,
                    "parcel_id": pickup.id,
                    "name": pickup.productName,
                    "address": pickup.recipientAddr
                })
            
            # 매트릭스 계산
            if len(locations) > 1:
                location_coords = [{"lat": loc["lat"], "lon": loc["lon"]} for loc in locations]
                time_matrix, _ = get_time_distance_matrix(location_coords, costing=COSTING_MODEL)
                
                if time_matrix is not None:
                    # LKH로 최적 경로 계산
                    optimal_tour, _ = call_lkh_service(time_matrix)
                    
                    if optimal_tour and len(optimal_tour) > 1:
                        # 현재 위치 다음
                        next_idx = optimal_tour[1]
                        next_location = locations[next_idx]
                        
                        # 경로 정보 가져오기
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
@auth_required
def complete_pickup():
    """수거 완료 처리"""
    try:
        data = request.json
        parcel_id = data.get('parcelId')
        
        if not parcel_id:
            return jsonify({"error": "parcelId is required"}), 400
        
        with get_db() as db:
            driver = get_current_driver(db)
            if not driver or driver["user"].userType != UserType.DRIVER:
                return jsonify({"error": "기사만 접근 가능합니다"}), 403
            
            driver_info = driver["driver_info"]
            
            # 수거 건 조회
            parcel = db.query(Parcel).filter(Parcel.id == parcel_id).first()
            if not parcel:
                return jsonify({"error": "수거 건을 찾을 수 없습니다"}), 404
            
            # 이미 완료된 건인지 확인
            if parcel.status != DeliveryStatus.PENDING:
                return jsonify({"error": "이미 처리된 수거 건입니다"}), 400
            
            # 완료 처리
            parcel.status = DeliveryStatus.COMPLETED
            parcel.driverId = driver_info.userId
            parcel.completedAt = datetime.now(KST)
            
            db.commit()
            
            # 남은 수거 건 확인 (해당 기사의 구역만)
            zone_districts = []
            for zone, districts in ZONE_MAPPING.items():
                if zone == driver_info.regionCity:
                    zone_districts = districts
                    break
            
            remaining_count = 0
            all_pending = db.query(Parcel).filter(
                and_(
                    Parcel.status == DeliveryStatus.PENDING,
                    Parcel.pickupDate <= datetime.now(KST),
                    Parcel.trackingCode.notlike("DEL-%")
                )
            ).all()
            
            for pending in all_pending:
                address_parts = pending.recipientAddr.split()
                for part in address_parts:
                    if part in zone_districts:
                        remaining_count += 1
                        break
            
            return jsonify({
                "status": "success",
                "remaining_pickups": remaining_count
            }), 200
            
    except Exception as e:
        logging.error(f"Error completing pickup: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/pickup/status', methods=['GET'])
@auth_required
def get_status():
    """현재 시스템 상태 정보"""
    try:
        with get_db() as db:
            user = db.query(User).filter(User.id == request.current_user_id).first()
            
            if user.userType == UserType.DRIVER:
                # 기사용 상태 정보
                driver_info = db.query(DriverInfo).filter(
                    DriverInfo.userId == user.id
                ).first()
                
                # 해당 구역의 구 리스트
                zone_districts = []
                for zone, districts in ZONE_MAPPING.items():
                    if zone == driver_info.regionCity:
                        zone_districts = districts
                        break
                
                # 해당 구역의 대기 중인 수거
                pending_count = 0
                all_pending = db.query(Parcel).filter(
                    and_(
                        Parcel.status == DeliveryStatus.PENDING,
                        Parcel.pickupDate <= datetime.now(KST),
                        Parcel.trackingCode.notlike("DEL-%")
                    )
                ).all()
                
                for parcel in all_pending:
                    address_parts = parcel.recipientAddr.split()
                    for part in address_parts:
                        if part in zone_districts:
                            pending_count += 1
                            break
                
                completed_count = db.query(Parcel).filter(
                    and_(
                        Parcel.driverId == user.id,
                        Parcel.status == DeliveryStatus.COMPLETED,
                        Parcel.completedAt >= datetime.now(KST).replace(hour=0, minute=0, second=0)
                    )
                ).count()
                
                return jsonify({
                    "status": "success",
                    "driver": {
                        "name": user.name,
                        "zone": driver_info.regionCity,
                        "district": driver_info.regionDistrict,
                        "pending_pickups": pending_count,
                        "completed_today": completed_count
                    }
                }), 200
            else:
                # 관리자용 전체 상태
                total_pending = db.query(Parcel).filter(
                    and_(
                        Parcel.status == DeliveryStatus.PENDING,
                        Parcel.trackingCode.notlike("DEL-%")
                    )
                ).count()
                
                total_completed = db.query(Parcel).filter(
                    and_(
                        Parcel.status == DeliveryStatus.COMPLETED,
                        Parcel.trackingCode.notlike("DEL-%")
                    )
                ).count()
                
                return jsonify({
                    "status": "success",
                    "system": {
                        "total_pending": total_pending,
                        "total_completed": total_completed,
                        "is_realtime_period": is_realtime_period()
                    }
                }), 200
                
    except Exception as e:
        logging.error(f"Error getting status: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

# --- 내부 함수들 ---
def call_lkh_service(cost_matrix):
    """LKH 서비스 호출"""
    headers = {'Content-Type': 'application/json'}
    matrix_list = cost_matrix.tolist()
    payload = json.dumps({"matrix": matrix_list})
    timeout_seconds = 60
    
    if len(cost_matrix) <= 2:
        logging.info("Only 2 nodes remain. Direct path calculation.")
        if len(cost_matrix) == 1:
            return [0], 0.0
        else:
            return [0, 1], cost_matrix[0][1]
    
    try:
        response = requests.post(LKH_SERVICE_URL, headers=headers, data=payload, timeout=timeout_seconds)
        response.raise_for_status()
        result = response.json()
        
        tour = result.get("tour")
        cost = result.get("cost") or result.get("tour_length")
        
        if tour is None or cost is None:
            logging.error(f"LKH service returned invalid response: {result}")
            return None, None
        
        return tour, cost
        
    except Exception as e:
        logging.error(f"Error calling LKH service: {e}")
        return None, None

def process_tomorrow_pickups():
    """다음날 수거 예약 처리"""
    try:
        with get_db() as db:
            # 내일 예정된 수거 건들
            tomorrow = datetime.now(KST).replace(hour=0, minute=0, second=0) + timedelta(days=1)
            tomorrow_end = tomorrow + timedelta(days=1)
            
            tomorrow_pickups = db.query(Parcel).filter(
                and_(
                    Parcel.pickupDate >= tomorrow,
                    Parcel.pickupDate < tomorrow_end,
                    Parcel.status == DeliveryStatus.PENDING,
                    Parcel.trackingCode.notlike("DEL-%")
                )
            ).all()
            
            if not tomorrow_pickups:
                logging.info("No tomorrow's pickups to process")
                return
            
            logging.info(f"Processing {len(tomorrow_pickups)} pickups for tomorrow")
            
    except Exception as e:
        logging.error(f"Error processing tomorrow's pickups: {e}", exc_info=True)

def setup_scheduler():
    """스케줄러 설정"""
    scheduler.add_job(
        func=process_tomorrow_pickups,
        trigger="cron",
        hour=7,
        minute=0,
        timezone=KST,
        id='daily_pickup_processing'
    )
    
    scheduler.start()
    logging.info("Scheduler started: Tomorrow's pickups will be processed at 7 AM daily")

# --- 메인 실행 ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")
    
    # 서비스 로깅
    logging.info(f"Starting pickup service on {host}:{port}")
    
    # 스케줄러 시작
    setup_scheduler()
    
    app.run(host=host, port=port, debug=False)