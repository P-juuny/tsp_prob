from flask import Flask, request, jsonify
import requests
import json
import logging
import os
import csv
import threading
import time
import xml.etree.ElementTree as ET
import urllib.parse

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VALHALLA_URL = os.environ.get('VALHALLA_URL', 'http://valhalla:8002')
SEOUL_API_KEY = os.environ.get('SEOUL_API_KEY', '7a7a43624a736b7a32385a7a617270')
MAPPING_FILE = '/data/service_to_osm_mapping.csv'

KAKAO_API_KEY = os.environ.get('KAKAO_API_KEY', 'YOUR_KAKAO_API_KEY_HERE')
KAKAO_ADDRESS_API = "https://dapi.kakao.com/v2/local/search/address.json"
KAKAO_KEYWORD_API = "https://dapi.kakao.com/v2/local/search/keyword.json"

traffic_data = {}
service_to_osm = {}

class TrafficProxy:
    def __init__(self):
        self.load_mappings()
        self.traffic_update_interval = int(os.environ.get('TRAFFIC_UPDATE_INTERVAL', '300'))
        self.api_delay = 0.05
        
        self.start_traffic_updater()
    
    def load_mappings(self):
        try:
            if os.path.exists(MAPPING_FILE):
                with open(MAPPING_FILE, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    success_count = 0
                    error_count = 0
                    
                    for row_num, row in enumerate(reader, 1):
                        try:
                            service_id = str(row.get('service_link_id', '')).strip()
                            osm_way_id_str = str(row.get('osm_way_id', '')).strip()
                            
                            if not service_id or not osm_way_id_str:
                                logger.debug(f"행 {row_num}: 빈 값 스킵")
                                error_count += 1
                                continue
                            
                            if osm_way_id_str.lower() == 'nan':
                                logger.debug(f"행 {row_num}: NaN 값 스킵")
                                error_count += 1
                                continue

                            osm_way_id_float = float(osm_way_id_str)
                            osm_id = str(int(osm_way_id_float))
                            
                            service_to_osm[service_id] = osm_id
                            success_count += 1
                            
                        except (ValueError, TypeError) as e:
                            logger.debug(f"행 {row_num} 처리 중 오류: {e} (service: {service_id}, osm: {osm_way_id_str})")
                            error_count += 1
                            continue
                        except Exception as e:
                            logger.debug(f"행 {row_num} 예기치 않은 오류: {e}")
                            error_count += 1
                            continue
                    
                logger.info(f"매핑 로드 완료: 성공 {success_count}개, 실패 {error_count}개")
                logger.info(f"유효한 매핑: {len(service_to_osm)}개")
            else:
                logger.error(f"매핑 파일을 찾을 수 없습니다: {MAPPING_FILE}")
        except Exception as e:
            logger.error(f"매핑 파일 읽기 오류: {e}")
            logger.info(f"현재 로드된 매핑: {len(service_to_osm)}개")
    
    def fetch_traffic_data(self):
        global traffic_data
        logger.info("실시간 교통 데이터 수집 시작...")

        new_traffic_data = {}
        
        service_links = list(service_to_osm.keys())
        total_links = len(service_links)
        logger.info(f"총 서비스링크 수: {total_links}개")
        
        success_count = 0
        fail_count = 0
        
        for i, service_link in enumerate(service_links):
            try:
                url = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/xml/TrafficInfo/1/1/{service_link}"
                response = requests.get(url, timeout=5)
                
                if response.status_code == 200:
                    root = ET.fromstring(response.text)

                    result = root.find('RESULT/CODE')
                    if result is not None and result.text == 'INFO-000':
                        row = root.find('row')
                        if row is not None:
                            link_id_elem = row.find('link_id')
                            prcs_spd_elem = row.find('prcs_spd')
                            
                            if link_id_elem is not None and prcs_spd_elem is not None:
                                link_id = str(link_id_elem.text)
                                speed = float(prcs_spd_elem.text)

                                if link_id in service_to_osm:
                                    osm_id = service_to_osm[link_id]
                                    new_traffic_data[osm_id] = speed
                                    success_count += 1
                                    if success_count % 100 == 0:
                                        logger.info(f"수집 중... {success_count}개 완료")

                time.sleep(self.api_delay)
                
            except Exception as e:
                fail_count += 1
                continue
            
            if (i + 1) % 500 == 0:
                logger.info(f"진행률: {i+1}/{total_links} ({(i+1)/total_links*100:.1f}%)")

        traffic_data = new_traffic_data
        logger.info(f"교통 데이터 수집 완료: {len(traffic_data)}개 (성공: {success_count}, 실패: {fail_count})")

        if traffic_data:
            speeds = list(traffic_data.values())
            avg_speed = sum(speeds) / len(speeds)
            min_speed = min(speeds)
            max_speed = max(speeds)
            logger.info(f"교통 속도 분포: 평균 {avg_speed:.1f}km/h, 최소 {min_speed:.1f}km/h, 최대 {max_speed:.1f}km/h")
    
    def find_real_speed_for_segment(self, maneuver):
        if not traffic_data:
            return None

        street_names = maneuver.get('street_names', [])

        for street_name in street_names:
            if not street_name:
                continue

            street_name_lower = str(street_name).lower()
            
            if '강남대로' in street_name or 'gangnam' in street_name_lower:
                for osm_id, speed in traffic_data.items():
                    if speed > 0:
                        return speed

        if traffic_data:
            speeds = [s for s in traffic_data.values() if 5 <= s <= 100]
            if speeds:
                avg_speed = sum(speeds) / len(speeds)
                if avg_speed < 40:
                    return avg_speed
        
        return None
    
    def apply_real_traffic_to_response(self, valhalla_response, use_traffic=False):
        if not use_traffic or not traffic_data or 'trip' not in valhalla_response:
            if 'trip' in valhalla_response:
                valhalla_response['trip']['has_traffic'] = False
                valhalla_response['trip']['traffic_data_count'] = len(traffic_data)
                valhalla_response['trip']['real_traffic_applied'] = False
            return valhalla_response
        
        logger.info("Valhalla 응답 인터셉트 - 실시간 교통 속도 적용 시작")
        
        applied_segments = 0
        total_segments = 0
        total_original_time = 0
        total_new_time = 0
        
        try:
            for leg in valhalla_response['trip'].get('legs', []):
                leg_original_time = 0
                leg_new_time = 0
                
                for maneuver in leg.get('maneuvers', []):
                    total_segments += 1
                    
                    original_time = maneuver.get('time', 0)
                    segment_length = maneuver.get('length', 0)
                    
                    leg_original_time += original_time

                    real_speed_kmh = self.find_real_speed_for_segment(maneuver)
                    
                    if real_speed_kmh and real_speed_kmh > 0 and segment_length > 0:
                        new_time = (segment_length / real_speed_kmh) * 3600

                        maneuver['time'] = new_time
                        maneuver['original_time'] = original_time
                        maneuver['real_speed_applied'] = real_speed_kmh
                        
                        leg_new_time += new_time
                        applied_segments += 1
                        
                        logger.debug(f"실시간 속도 적용: {segment_length:.2f}km, "
                                   f"{original_time:.1f}s → {new_time:.1f}s "
                                   f"(실시간: {real_speed_kmh:.1f}km/h)")
                    else:
                        leg_new_time += original_time

                if 'summary' in leg:
                    leg['summary']['original_time'] = leg_original_time
                    leg['summary']['time'] = leg_new_time
                
                total_original_time += leg_original_time
                total_new_time += leg_new_time

            if 'summary' in valhalla_response['trip']:
                valhalla_response['trip']['summary']['original_time'] = total_original_time
                valhalla_response['trip']['summary']['time'] = total_new_time
                valhalla_response['trip']['summary']['traffic_time'] = total_new_time
        
        except Exception as e:
            logger.error(f"실시간 교통 적용 중 오류: {e}")

        valhalla_response['trip']['has_traffic'] = True
        valhalla_response['trip']['traffic_data_count'] = len(traffic_data)
        valhalla_response['trip']['real_traffic_applied'] = True
        valhalla_response['trip']['applied_segments'] = applied_segments
        valhalla_response['trip']['total_segments'] = total_segments
        
        if applied_segments > 0:
            time_change_pct = ((total_new_time - total_original_time) / total_original_time) * 100
            logger.info(f"실시간 교통 적용 완료: {applied_segments}/{total_segments} 구간, "
                       f"시간 변화: {time_change_pct:+.1f}%")
        else:
            logger.info("적용된 실시간 교통 구간 없음")
        
        return valhalla_response
    
    def start_traffic_updater(self):
        def update_loop():
            try:
                logger.info("첫 번째 교통 데이터 수집 시작...")
                self.fetch_traffic_data()
            except Exception as e:
                logger.error(f"초기 교통 데이터 수집 오류: {e}")

            while True:
                try:
                    logger.info(f"다음 업데이트까지 {self.traffic_update_interval}초 대기...")
                    time.sleep(self.traffic_update_interval)
                    logger.info("주기적 교통 데이터 업데이트 시작...")
                    self.fetch_traffic_data()
                except Exception as e:
                    logger.error(f"교통 데이터 업데이트 오류: {e}")
        
        thread = threading.Thread(target=update_loop, daemon=True)
        thread.start()
        logger.info("교통 데이터 자동 업데이트 스레드 시작됨")

    def kakao_geocoding(self, address):
        try:
            headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}

            params = {"query": address}
            response = requests.get(KAKAO_ADDRESS_API, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                documents = data.get("documents", [])
                
                if documents:
                    doc = documents[0]
                    lat = float(doc["y"])
                    lon = float(doc["x"])
                    address_name = doc.get("address_name", address)
                    
                    logger.info(f"카카오 주소 검색 성공: {address} -> ({lat}, {lon}) [{address_name}]")
                    return lat, lon, address_name, 0.95

            response = requests.get(KAKAO_KEYWORD_API, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                documents = data.get("documents", [])
                
                if documents:
                    doc = documents[0]
                    lat = float(doc["y"])
                    lon = float(doc["x"])
                    place_name = doc.get("place_name", address)
                    
                    logger.info(f"카카오 키워드 검색 성공: {address} -> ({lat}, {lon}) [{place_name}]")
                    return lat, lon, place_name, 0.85

            logger.warning(f"카카오 지오코딩 실패, 기본 좌표 사용: {address}")
            return self.get_default_coordinates_by_district(address)
            
        except Exception as e:
            logger.error(f"카카오 지오코딩 오류: {e}")
            return self.get_default_coordinates_by_district(address)

    def get_default_coordinates_by_district(self, address):
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
                logger.info(f"기본 좌표 사용: {address} -> ({lat}, {lon}) [{name}]")
                return lat, lon, name, 0.5

        logger.warning(f"구를 찾을 수 없어 서울시청 좌표 사용: {address}")
        return 37.5665, 126.9780, "서울시청", 0.1

proxy = TrafficProxy()

@app.route('/status', methods=['GET'])
def status():
    try:
        response = requests.get(f"{VALHALLA_URL}/status", timeout=5)
        return response.text, response.status_code, response.headers.items()
    except Exception as e:
        logger.error(f"Status check error: {e}")
        return jsonify({"error": "Valhalla unreachable"}), 503

@app.route('/route', methods=['POST'])
def proxy_route():
    try:
        original_request = request.json
        logger.info(f"Route request received")
        logger.info(f"교통 데이터 수집: {len(traffic_data)}개")

        costing_options = original_request.get('costing_options', {})
        costing = original_request.get('costing', 'auto')
        use_traffic = costing_options.get(costing, {}).get('use_live_traffic', False)

        response = requests.post(
            f"{VALHALLA_URL}/route",
            json=original_request,
            timeout=30
        )
        
        if response.status_code == 200:
            valhalla_result = response.json()

            modified_result = proxy.apply_real_traffic_to_response(valhalla_result, use_traffic)
            
            return jsonify(modified_result)
        else:
            logger.error(f"Valhalla error: {response.status_code}")
            return jsonify({"error": "Valhalla error"}), response.status_code
            
    except Exception as e:
        logger.error(f"Proxy error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/matrix', methods=['POST'])
def proxy_matrix_endpoint():
    try:
        original_request = request.json
        logger.info("Matrix request received")

        costing_options = original_request.get('costing_options', {})
        costing = original_request.get('costing', 'auto')
        use_traffic = costing_options.get(costing, {}).get('use_live_traffic', False)

        response = requests.post(
            f"{VALHALLA_URL}/sources_to_targets",
            json=original_request,
            timeout=60
        )
        
        if response.status_code == 200:
            valhalla_result = response.json()

            if use_traffic and traffic_data:
                modified_result = self.apply_traffic_to_matrix(valhalla_result)
                logger.info("Matrix에 실시간 교통 적용 완료")
                return jsonify(modified_result)
            else:
                logger.info("Matrix 기본 Valhalla 결과 사용")
                return jsonify(valhalla_result)
        else:
            logger.error(f"Matrix request failed: {response.status_code}")
            return response.text, response.status_code, response.headers.items()
    
    except Exception as e:
        logger.error(f"Matrix proxy error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/sources_to_targets', methods=['POST'])
def proxy_matrix():
    try:
        original_request = request.json

        response = requests.post(
            f"{VALHALLA_URL}/sources_to_targets",
            json=original_request,
            timeout=60
        )
        
        return jsonify(response.json())
    
    except Exception as e:
        logger.error(f"Matrix proxy error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    traffic_stats = {}
    if traffic_data:
        speeds = list(traffic_data.values())
        traffic_stats = {
            "avg_speed": sum(speeds) / len(speeds),
            "min_speed": min(speeds),
            "max_speed": max(speeds),
            "slow_roads": len([s for s in speeds if s < 20]),
            "fast_roads": len([s for s in speeds if s > 50])
        }
    
    return jsonify({
        "status": "healthy",
        "traffic_data_count": len(traffic_data),
        "traffic_stats": traffic_stats,
        "valhalla_url": VALHALLA_URL,
        "kakao_api_configured": bool(KAKAO_API_KEY and KAKAO_API_KEY != 'YOUR_KAKAO_API_KEY_HERE'),
        "geocoding_method": "kakao",
        "intercept_method": "response_modification"
    })

@app.route('/search', methods=['GET'])
def kakao_geocoding_search():
    try:
        text = request.args.get('text', '')
        logger.info(f"카카오 지오코딩 요청: {text}")
        
        if not text:
            return jsonify({"error": "text parameter required"}), 400

        lat, lon, location_name, confidence = proxy.kakao_geocoding(text)

        result = {
            "features": [{
                "geometry": {
                    "coordinates": [lon, lat]
                },
                "properties": {
                    "confidence": confidence,
                    "display_name": location_name,
                    "geocoding_method": "kakao"
                }
            }]
        }
        
        if confidence >= 0.8:
            logger.info(f"카카오 지오코딩 성공: {text} -> ({lat}, {lon}) 신뢰도: {confidence}")
        else:
            logger.warning(f"카카오 지오코딩 (낮은 신뢰도): {text} -> ({lat}, {lon}) 신뢰도: {confidence}")
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"카카오 지오코딩 오류: {e}")

        result = {
            "features": [{
                "geometry": {
                    "coordinates": [126.9780, 37.5665]
                },
                "properties": {
                    "confidence": 0.1,
                    "display_name": "서울시청 (기본값)",
                    "geocoding_method": "fallback"
                }
            }]
        }
        return jsonify(result), 200

@app.route('/traffic-debug', methods=['GET'])
def traffic_debug():
    if not traffic_data:
        return jsonify({"message": "교통 데이터 없음"}), 200
    
    speeds = list(traffic_data.values())
    sample_data = dict(list(traffic_data.items())[:10])

    speed_distribution = {
        "very_slow": len([s for s in speeds if s < 15]),
        "slow": len([s for s in speeds if 15 <= s < 30]), 
        "normal": len([s for s in speeds if 30 <= s < 50]),
        "fast": len([s for s in speeds if s >= 50])
    }
    
    return jsonify({
        "total_roads": len(traffic_data),
        "speed_stats": {
            "avg": sum(speeds) / len(speeds),
            "min": min(speeds),
            "max": max(speeds)
        },
        "speed_distribution": speed_distribution,
        "sample_data": sample_data,
        "method": "Valhalla 응답 인터셉트 후 실시간 속도로 시간 재계산"
    })

@app.route('/<path:path>', methods=['GET', 'POST'])
def proxy_all(path):
    try:
        if request.method == 'GET':
            response = requests.get(f"{VALHALLA_URL}/{path}", timeout=30)
        else:
            response = requests.post(
                f"{VALHALLA_URL}/{path}",
                json=request.json,
                headers=request.headers,
                timeout=30
            )
        
        return response.text, response.status_code, response.headers.items()
    except Exception as e:
        logger.error(f"Proxy error for {path}: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logger.info("Valhalla 응답 인터셉트 방식 Traffic Proxy 시작")
    logger.info(f"카카오 API 설정: {'OK' if KAKAO_API_KEY and KAKAO_API_KEY != 'YOUR_KAKAO_API_KEY_HERE' else 'API KEY 필요'}")
    app.run(host='0.0.0.0', port=8003, debug=False)