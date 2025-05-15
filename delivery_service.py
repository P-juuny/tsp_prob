import requests
import json
import numpy as np
import logging
import os
from datetime import datetime, time as datetime_time
from flask import Flask, request, jsonify
import pytz

# main_service에서 재활용 (call_lkh_service 제거)
from main_service import (
    address_to_coordinates,
    determine_zone_by_district,
    ZONE_MAPPING,
    HUB_LOCATION,
    COSTING_MODEL,
    KST
)

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

# Flask 앱 설정
app = Flask(__name__)

# --- API 엔드포인트 ---

@app.route('/api/delivery/import', methods=['POST'])
def import_todays_pickups():
    """오늘 수거 완료된 것들을 배달로 전환 (관리자용)"""
    try:
        # 백엔드에서 오늘 완료된 수거 목록 가져오기
        response = requests.get(f"{BACKEND_API_URL}/api/pickups/completed/today")
        if response.status_code != 200:
            return jsonify({"error": "Failed to get completed pickups"}), 500
        
        completed_pickups = response.json()
        
        # 각 수거를 배달로 전환
        converted_count = 0
        zone_stats = {}
        
        for pickup in completed_pickups:
            # 백엔드에 배달 전환 요청
            convert_resp = requests.post(
                f"{BACKEND_API_URL}/api/delivery/convert",
                json={"pickupId": pickup['id']}
            )
            
            if convert_resp.status_code == 200:
                converted_count += 1
                
                # 구역별 통계
                address = pickup['recipientAddr']
                for part in address.split():
                    if part.endswith('구'):
                        zone = determine_zone_by_district(part)
                        if zone:
                            zone_stats[zone] = zone_stats.get(zone, 0) + 1
                        break
        
        return jsonify({
            "status": "success",
            "converted": converted_count,
            "by_zone": zone_stats
        }), 200
        
    except Exception as e:
        logging.error(f"Error importing pickups: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/delivery/assign', methods=['POST'])
def assign_to_drivers():
    """배달 물건들을 구역별 기사에게 할당 (관리자용)"""
    try:
        # 미할당 배달 목록 가져오기
        response = requests.get(f"{BACKEND_API_URL}/api/deliveries/unassigned/today")
        if response.status_code != 200:
            return jsonify({"error": "Failed to get deliveries"}), 500
        
        unassigned = response.json()
        
        # 구역별로 분류
        zone_deliveries = {}
        for delivery in unassigned:
            address = delivery['recipientAddr']
            for part in address.split():
                if part.endswith('구'):
                    zone = determine_zone_by_district(part)
                    if zone:
                        if zone not in zone_deliveries:
                            zone_deliveries[zone] = []
                        zone_deliveries[zone].append(delivery)
                    break
        
        # 각 구역 기사에게 할당
        results = {}
        for zone, deliveries in zone_deliveries.items():
            # 해당 구역 기사 정보 가져오기
            driver_resp = requests.get(f"{BACKEND_API_URL}/api/zone/{zone}/driver")
            if driver_resp.status_code == 200:
                driver = driver_resp.json()
                
                # 배달 할당
                for delivery in deliveries:
                    requests.put(
                        f"{BACKEND_API_URL}/api/delivery/{delivery['id']}/assign",
                        json={"driverId": driver['id']}
                    )
                
                results[zone] = {
                    "driver_id": driver['id'],
                    "driver_name": driver['name'],
                    "count": len(deliveries)
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
        # 현재 로그인한 기사 정보
        driver_info = get_current_driver()
        driver_id = driver_info['id']
        
        # 백엔드에서 내 배달 목록 가져오기
        response = requests.get(f"{BACKEND_API_URL}/api/driver/{driver_id}/deliveries/today")
        if response.status_code != 200:
            return jsonify({"error": "Failed to get deliveries"}), 500
        
        my_deliveries = response.json()
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
            # LKH 서비스 직접 호출 (main_service.py와 동일한 방식)
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
        
        # 백엔드에 완료 요청
        response = requests.put(
            f"{BACKEND_API_URL}/api/delivery/{delivery_id}/complete"
        )
        
        if response.status_code == 200:
            return jsonify({"status": "success"}), 200
        else:
            return jsonify({"error": "Failed to complete"}), 500
            
    except Exception as e:
        logging.error(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    host = os.environ.get("HOST", "0.0.0.0")
    
    logging.info(f"Starting delivery service on {host}:{port}")
    app.run(host=host, port=port, debug=False)