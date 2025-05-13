import time
import json
import copy
import requests
import numpy as np
import logging
import os
from datetime import datetime, time as datetime_time
from flask import Flask, request, jsonify
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import and_, or_, func

# 다른 스크립트에서 함수 임포트
from get_valhalla_matrix import get_time_distance_matrix
from get_valhalla_route import get_turn_by_turn_route
from database import get_db, get_db_session
from models import User, DriverInfo, Parcel, StoreInfo, UserType, DeliveryStatus
from auth import auth_required, get_current_driver, login
from main_service import call_lkh_service, ZONE_MAPPING, KST, COSTING_MODEL, HUB_LOCATION, address_to_coordinates

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("delivery_service.log"),
        logging.StreamHandler()
    ]
)

# --- 설정 ---
DELIVERY_START_TIME = datetime_time(12, 0)  # 배달 시작 시간 (오후 12시)

# Flask 앱 설정
app = Flask(__name__)

# 스케줄러 설정
scheduler = BackgroundScheduler()

# --- API 엔드포인트 ---
@app.route('/api/delivery/import_pickups', methods=['POST'])
@auth_required
def import_pickups():
    """오늘 수거된 데이터를 배달 요청으로 가져오기"""
    try:
        with get_db() as db:
            # 현재 로그인한 사용자 확인
            user = db.query(User).filter(User.id == request.current_user_id).first()
            if not user or user.userType != UserType.ADMIN:
                return jsonify({"error": "관리자만 접근 가능합니다"}), 403
            
            # 오늘 완료된 수거 건 조회
            today_start = datetime.now(KST).replace(hour=0, minute=0, second=0)
            today_end = today_start.replace(hour=23, minute=59, second=59)
            
            completed_pickups = db.query(Parcel).filter(
                and_(
                    Parcel.status == DeliveryStatus.COMPLETED,
                    Parcel.completedAt >= today_start,
                    Parcel.completedAt <= today_end
                )
            ).all()
            
            if not completed_pickups:
                return jsonify({
                    "status": "info",
                    "message": "오늘 완료된 수거가 없습니다",
                    "imported_count": 0
                }), 200
            
            # 이미 배달 전환된 건 제외
            new_deliveries = []
            for pickup in completed_pickups:
                # 동일한 원본 수거에서 생성된 배달이 있는지 확인
                existing_delivery = db.query(Parcel).filter(
                    and_(
                        Parcel.trackingCode == f"DEL-{pickup.id}",
                        Parcel.pickupDate == today_start  # 오늘 날짜로 설정된 배달
                    )
                ).first()
                
                if not existing_delivery:
                    # 새로운 배달 생성
                    delivery = Parcel(
                        ownerId=pickup.ownerId,
                        productName=pickup.productName,
                        size=pickup.size,
                        caution=pickup.caution,
                        recipientName=pickup.recipientName,
                        recipientPhone=pickup.recipientPhone,
                        recipientAddr=pickup.recipientAddr,
                        detailAddress=pickup.detailAddress,
                        status=DeliveryStatus.PENDING,
                        trackingCode=f"DEL-{pickup.id}",  # 원본 수거 ID 참조
                        pickupDate=today_start  # 배달 날짜는 오늘로 설정
                    )
                    db.add(delivery)
                    new_deliveries.append(delivery)
            
            db.commit()
            
            # 구역별 통계
            zone_counts = {}
            for delivery in new_deliveries:
                # 주소에서 구 추출
                address_parts = delivery.recipientAddr.split()
                for part in address_parts:
                    if part.endswith('구'):
                        zone = None
                        for zone_name, districts in ZONE_MAPPING.items():
                            if part in districts:
                                zone = zone_name
                                break
                        if zone:
                            zone_counts[zone] = zone_counts.get(zone, 0) + 1
                        break
            
            return jsonify({
                "status": "success",
                "message": f"{len(new_deliveries)}개의 수거가 배달로 전환되었습니다",
                "imported_count": len(new_deliveries),
                "by_zone": zone_counts
            }), 200
            
    except Exception as e:
        logging.error(f"Error importing pickups: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/delivery/optimize', methods=['POST'])
@auth_required
def optimize_delivery_routes():
    """전체 배달 경로 최적화 (일괄 처리)"""
    try:
        with get_db() as db:
            # 현재 로그인한 사용자 확인 (관리자만)
            user = db.query(User).filter(User.id == request.current_user_id).first()
            if not user or user.userType != UserType.ADMIN:
                return jsonify({"error": "관리자만 접근 가능합니다"}), 403
            
            # 오늘 날짜의 미배송 배달 조회
            today = datetime.now(KST).date()
            pending_deliveries = db.query(Parcel).filter(
                and_(
                    Parcel.status == DeliveryStatus.PENDING,
                    func.date(Parcel.pickupDate) == today,
                    Parcel.trackingCode.like("DEL-%")  # 배달 건만
                )
            ).all()
            
            if not pending_deliveries:
                return jsonify({
                    "status": "info",
                    "message": "최적화할 배달이 없습니다"
                }), 200
            
            optimization_results = {}
            
            # 구역별로 분류
            zone_deliveries = {}
            for delivery in pending_deliveries:
                # 주소에서 구 추출하여 구역 결정
                address_parts = delivery.recipientAddr.split()
                zone = None
                for part in address_parts:
                    if part.endswith('구'):
                        for zone_name, districts in ZONE_MAPPING.items():
                            if part in districts:
                                zone = zone_name
                                break
                        break
                
                if zone:
                    if zone not in zone_deliveries:
                        zone_deliveries[zone] = []
                    zone_deliveries[zone].append(delivery)
            
            # 각 구역별로 TSP 최적화
            for zone_name, deliveries in zone_deliveries.items():
                locations = [HUB_LOCATION]  # 용산역에서 시작
                
                # 배달지 위치 추가
                for delivery in deliveries:
                    lat, lon = address_to_coordinates(delivery.recipientAddr)
                    locations.append({
                        "lat": lat,
                        "lon": lon,
                        "delivery_id": delivery.id,
                        "address": delivery.recipientAddr
                    })
                
                # 매트릭스 계산
                if len(locations) > 1:
                    location_coords = [{"lat": loc["lat"], "lon": loc["lon"]} for loc in locations]
                    time_matrix, _ = get_time_distance_matrix(location_coords, costing=COSTING_MODEL)
                    
                    if time_matrix is not None:
                        # LKH로 최적 경로 계산
                        optimal_tour, total_cost = call_lkh_service(time_matrix)
                        
                        if optimal_tour:
                            # 최적 순서대로 배달 정렬
                            optimized_route = []
                            for idx in optimal_tour[1:]:  # 허브 제외
                                if 0 < idx < len(locations):
                                    delivery_id = locations[idx]["delivery_id"]
                                    delivery = next(d for d in deliveries if d.id == delivery_id)
                                    optimized_route.append({
                                        "delivery_id": delivery_id,
                                        "address": delivery.recipientAddr,
                                        "recipient": delivery.recipientName,
                                        "order": len(optimized_route) + 1
                                    })
                            
                            # 해당 구역 기사에게 할당
                            driver = db.query(DriverInfo).filter(
                                DriverInfo.regionCity == zone_name
                            ).first()
                            
                            if driver:
                                # 배달 기사 할당
                                for delivery in deliveries:
                                    delivery.driverId = driver.userId
                                    delivery.status = DeliveryStatus.IN_PROGRESS
                                db.commit()
                            
                            optimization_results[zone_name] = {
                                "driver_id": driver.userId if driver else None,
                                "driver_name": driver.user.name if driver else "미할당",
                                "total_deliveries": len(deliveries),
                                "optimized_route": optimized_route,
                                "total_time": total_cost
                            }
            
            return jsonify({
                "status": "success",
                "optimization_results": optimization_results
            }), 200
            
    except Exception as e:
        logging.error(f"Error optimizing delivery routes: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/delivery/route/<int:driver_id>', methods=['GET'])
@auth_required  
def get_driver_route(driver_id):
    """특정 기사의 배달 경로 조회"""
    try:
        with get_db() as db:
            # 현재 로그인한 사용자 확인
            current_user = db.query(User).filter(User.id == request.current_user_id).first()
            
            # 기사 본인이거나 관리자만 조회 가능
            if current_user.id != driver_id and current_user.userType != UserType.ADMIN:
                return jsonify({"error": "권한이 없습니다"}), 403
            
            # 오늘 날짜의 해당 기사 배달 조회
            today = datetime.now(KST).date()
            driver_deliveries = db.query(Parcel).filter(
                and_(
                    Parcel.driverId == driver_id,
                    Parcel.status.in_([DeliveryStatus.IN_PROGRESS, DeliveryStatus.PENDING]),
                    func.date(Parcel.pickupDate) == today,
                    Parcel.trackingCode.like("DEL-%")
                )
            ).all()
            
            if not driver_deliveries:
                return jsonify({
                    "status": "info",
                    "message": "오늘 배달이 없습니다",
                    "deliveries": []
                }), 200
            
            # 배달 정보 정리
            deliveries = []
            for delivery in driver_deliveries:
                deliveries.append({
                    "delivery_id": delivery.id,
                    "tracking_code": delivery.trackingCode,
                    "recipient_name": delivery.recipientName,
                    "recipient_phone": delivery.recipientPhone,
                    "address": delivery.recipientAddr,
                    "detail_address": delivery.detailAddress,
                    "status": delivery.status.value,
                    "product_name": delivery.productName
                })
            
            return jsonify({
                "status": "success",
                "driver_id": driver_id,
                "total_deliveries": len(deliveries),
                "deliveries": deliveries
            }), 200
            
    except Exception as e:
        logging.error(f"Error getting driver route: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/delivery/complete', methods=['POST'])
@auth_required
def complete_delivery():
    """배달 완료 처리"""
    try:
        data = request.json
        delivery_id = data.get('deliveryId')
        
        if not delivery_id:
            return jsonify({"error": "deliveryId is required"}), 400
        
        with get_db() as db:
            # 현재 로그인한 기사 확인
            driver = get_current_driver(db)
            if not driver or driver["user"].userType != UserType.DRIVER:
                return jsonify({"error": "기사만 접근 가능합니다"}), 403
            
            driver_info = driver["driver_info"]
            
            # 배달 건 조회
            delivery = db.query(Parcel).filter(
                and_(
                    Parcel.id == delivery_id,
                    Parcel.driverId == driver_info.userId
                )
            ).first()
            
            if not delivery:
                return jsonify({"error": "배달 건을 찾을 수 없습니다"}), 404
            
            # 이미 완료된 건인지 확인
            if delivery.status == DeliveryStatus.COMPLETED:
                return jsonify({"error": "이미 완료된 배달입니다"}), 400
            
            # 완료 처리
            delivery.status = DeliveryStatus.COMPLETED
            delivery.completedAt = datetime.now(KST)
            
            # 배달 이미지 URL (있는 경우)
            if 'deliveryImageUrl' in data:
                delivery.deliveryImageUrl = data['deliveryImageUrl']
            
            db.commit()
            
            # 남은 배달 건 확인
            remaining_deliveries = db.query(Parcel).filter(
                and_(
                    Parcel.driverId == driver_info.userId,
                    Parcel.status == DeliveryStatus.IN_PROGRESS,
                    func.date(Parcel.pickupDate) == datetime.now(KST).date()
                )
            ).count()
            
            return jsonify({
                "status": "success",
                "remaining_deliveries": remaining_deliveries
            }), 200
            
    except Exception as e:
        logging.error(f"Error completing delivery: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/delivery/status', methods=['GET'])
@auth_required
def get_delivery_status():
    """배달 시스템 상태 확인"""
    try:
        with get_db() as db:
            user = db.query(User).filter(User.id == request.current_user_id).first()
            
            if user.userType == UserType.DRIVER:
                # 기사용 상태 정보
                driver_info = db.query(DriverInfo).filter(
                    DriverInfo.userId == user.id
                ).first()
                
                today = datetime.now(KST).date()
                
                # 오늘의 배달 통계
                pending_count = db.query(Parcel).filter(
                    and_(
                        Parcel.driverId == user.id,
                        Parcel.status == DeliveryStatus.IN_PROGRESS,
                        func.date(Parcel.pickupDate) == today
                    )
                ).count()
                
                completed_count = db.query(Parcel).filter(
                    and_(
                        Parcel.driverId == user.id,
                        Parcel.status == DeliveryStatus.COMPLETED,
                        func.date(Parcel.completedAt) == today
                    )
                ).count()
                
                return jsonify({
                    "status": "success",
                    "driver": {
                        "name": user.name,
                        "zone": driver_info.regionCity,
                        "pending_deliveries": pending_count,
                        "completed_today": completed_count
                    }
                }), 200
            else:
                # 관리자용 전체 상태
                today = datetime.now(KST).date()
                
                # 구역별 통계
                zone_stats = {}
                for zone_name in ZONE_MAPPING.keys():
                    # 해당 구역 기사들 조회
                    zone_drivers = db.query(DriverInfo).filter(
                        DriverInfo.regionCity == zone_name
                    ).all()
                    
                    pending_count = 0
                    completed_count = 0
                    
                    for driver in zone_drivers:
                        pending_count += db.query(Parcel).filter(
                            and_(
                                Parcel.driverId == driver.userId,
                                Parcel.status == DeliveryStatus.IN_PROGRESS,
                                func.date(Parcel.pickupDate) == today
                            )
                        ).count()
                        
                        completed_count += db.query(Parcel).filter(
                            and_(
                                Parcel.driverId == driver.userId,
                                Parcel.status == DeliveryStatus.COMPLETED,
                                func.date(Parcel.completedAt) == today
                            )
                        ).count()
                    
                    zone_stats[zone_name] = {
                        "total_drivers": len(zone_drivers),
                        "pending_deliveries": pending_count,
                        "completed_deliveries": completed_count
                    }
                
                return jsonify({
                    "status": "success",
                    "zones": zone_stats,
                    "delivery_start_time": DELIVERY_START_TIME.strftime("%H:%M")
                }), 200
                
    except Exception as e:
        logging.error(f"Error getting delivery status: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/delivery/next', methods=['GET'])
@auth_required
def get_next_delivery():
    """기사의 다음 배달지 계산"""
    try:
        with get_db() as db:
            driver = get_current_driver(db)
            if not driver or driver["user"].userType != UserType.DRIVER:
                return jsonify({"error": "기사만 접근 가능합니다"}), 403
            
            driver_info = driver["driver_info"]
            
            # 오늘의 미완료 배달 조회
            today = datetime.now(KST).date()
            pending_deliveries = db.query(Parcel).filter(
                and_(
                    Parcel.driverId == driver_info.userId,
                    Parcel.status == DeliveryStatus.IN_PROGRESS,
                    func.date(Parcel.pickupDate) == today
                )
            ).all()
            
            if not pending_deliveries:
                # 모든 배달 완료, 허브로 복귀
                # 마지막 완료 위치 찾기
                last_completed = db.query(Parcel).filter(
                    and_(
                        Parcel.driverId == driver_info.userId,
                        Parcel.status == DeliveryStatus.COMPLETED,
                        func.date(Parcel.completedAt) == today
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
                    "remaining_deliveries": 0
                }), 200
            
            # 현재 위치 결정
            last_completed = db.query(Parcel).filter(
                and_(
                    Parcel.driverId == driver_info.userId,
                    Parcel.status == DeliveryStatus.COMPLETED,
                    func.date(Parcel.completedAt) == today
                )
            ).order_by(Parcel.completedAt.desc()).first()
            
            if last_completed:
                # 마지막 완료 위치가 현재 위치
                current_lat, current_lon = address_to_coordinates(last_completed.recipientAddr)
                current_location = {"lat": current_lat, "lon": current_lon}
            else:
                # 첫 배달이면 허브에서 시작
                current_location = HUB_LOCATION
            
            # TSP로 다음 최적 배달지 계산
            locations = [current_location]  # 현재 위치에서 시작
            
            for delivery in pending_deliveries:
                lat, lon = address_to_coordinates(delivery.recipientAddr)
                locations.append({
                    "lat": lat,
                    "lon": lon,
                    "delivery_id": delivery.id,
                    "name": delivery.recipientName,
                    "address": delivery.recipientAddr
                })
            
            # 매트릭스 계산
            if len(locations) > 1:
                location_coords = [{"lat": loc["lat"], "lon": loc["lon"]} for loc in locations]
                time_matrix, _ = get_time_distance_matrix(location_coords, costing=COSTING_MODEL)
                
                if time_matrix is not None:
                    optimal_tour, _ = call_lkh_service(time_matrix)
                    
                    if optimal_tour and len(optimal_tour) > 1:
                        next_idx = optimal_tour[1]  # 현재 위치 다음
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
                            "remaining_deliveries": len(pending_deliveries)
                        }), 200
            
            # 첫 번째 배달지로
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
                "remaining_deliveries": len(pending_deliveries)
            }), 200
            
    except Exception as e:
        logging.error(f"Error getting next delivery: {e}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

# --- 스케줄러 함수들 ---
def auto_import_pickups():
    """자동으로 수거 완료 데이터 가져오기 (매일 12시)"""
    try:
        logging.info("Auto-importing pickups at noon")
        
        with get_db() as db:
            # 오늘 완료된 수거를 배달로 전환
            today_start = datetime.now(KST).replace(hour=0, minute=0, second=0)
            today_end = today_start.replace(hour=23, minute=59, second=59)
            
            completed_pickups = db.query(Parcel).filter(
                and_(
                    Parcel.status == DeliveryStatus.COMPLETED,
                    Parcel.completedAt >= today_start,
                    Parcel.completedAt <= today_end,
                    Parcel.trackingCode.notlike("DEL-%")  # 이미 배달로 전환된 것 제외
                )
            ).all()
            
            new_deliveries = 0
            for pickup in completed_pickups:
                delivery = Parcel(
                    ownerId=pickup.ownerId,
                    productName=pickup.productName,
                    size=pickup.size,
                    caution=pickup.caution,
                    recipientName=pickup.recipientName,
                    recipientPhone=pickup.recipientPhone,
                    recipientAddr=pickup.recipientAddr,
                    detailAddress=pickup.detailAddress,
                    status=DeliveryStatus.PENDING,
                    trackingCode=f"DEL-{pickup.id}",
                    pickupDate=today_start
                )
                db.add(delivery)
                new_deliveries += 1
            
            db.commit()
            logging.info(f"Auto-imported {new_deliveries} deliveries")
            
    except Exception as e:
        logging.error(f"Error in auto import: {e}", exc_info=True)

def setup_scheduler():
    """스케줄러 설정"""
    scheduler.add_job(
        func=auto_import_pickups,
        trigger="cron",
        hour=12,
        minute=0,
        timezone=KST,
        id='daily_delivery_import'
    )
    
    scheduler.start()
    logging.info("Scheduler started: Pickups will be imported daily at 12:00 PM")

# --- 메인 실행 ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    host = os.environ.get("HOST", "0.0.0.0")
    
    # 서비스 로깅
    logging.info(f"Starting delivery service on {host}:{port}")
    
    # 스케줄러 시작
    setup_scheduler()
    
    app.run(host=host, port=port, debug=False)