from flask import Flask, request, jsonify
import requests
import json
import logging
import os
import csv
import threading
import time
import xml.etree.ElementTree as ET

app = Flask(__name__)

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 설정
VALHALLA_URL = os.environ.get('VALHALLA_URL', 'http://valhalla:8002')
SEOUL_API_KEY = os.environ.get('SEOUL_API_KEY', '7a7a43624a736b7a32385a7a617270')
MAPPING_FILE = '/data/service_to_osm_mapping.csv'

# 글로벌 변수
traffic_data = {}  # OSM Way ID -> 속도 매핑
service_to_osm = {}  # 서비스링크 -> OSM 매핑

class TrafficProxy:
    def __init__(self):
        self.load_mappings()
        self.traffic_update_interval = int(os.environ.get('TRAFFIC_UPDATE_INTERVAL', '300'))  # 5분
        self.api_delay = 0.05  # API 호출 간격
        
        # 백그라운드 스레드 시작
        self.start_traffic_updater()
    
    def load_mappings(self):
        """서비스링크 -> OSM 매핑 로드 (CSV 직접 읽기)"""
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
                            
                            # 빈 값 체크
                            if not service_id or not osm_way_id_str:
                                logger.debug(f"행 {row_num}: 빈 값 스킵")
                                error_count += 1
                                continue
                            
                            # 'NaN' 체크
                            if osm_way_id_str.lower() == 'nan':
                                logger.debug(f"행 {row_num}: NaN 값 스킵")
                                error_count += 1
                                continue
                            
                            # float 변환 시도
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
        """서울시 실시간 교통 데이터 가져오기"""
        global traffic_data
        logger.info("실시간 교통 데이터 수집 시작...")
        
        # 새 데이터로 교체
        new_traffic_data = {}
        
        service_links = list(service_to_osm.keys())
        total_links = len(service_links)
        logger.info(f"총 서비스링크 수: {total_links}개")
        
        success_count = 0
        fail_count = 0
        
        # 전체 링크를 연속으로 처리
        for i, service_link in enumerate(service_links):
            try:
                # XML 형식으로 요청
                url = f"http://openapi.seoul.go.kr:8088/{SEOUL_API_KEY}/xml/TrafficInfo/1/1/{service_link}"
                response = requests.get(url, timeout=5)
                
                if response.status_code == 200:
                    # XML 파싱
                    root = ET.fromstring(response.text)
                    
                    # 에러 체크
                    result = root.find('RESULT/CODE')
                    if result is not None and result.text == 'INFO-000':
                        # 데이터 추출
                        row = root.find('row')
                        if row is not None:
                            link_id_elem = row.find('link_id')
                            prcs_spd_elem = row.find('prcs_spd')
                            
                            if link_id_elem is not None and prcs_spd_elem is not None:
                                link_id = str(link_id_elem.text)
                                speed = float(prcs_spd_elem.text)
                                
                                # OSM Way ID로 변환
                                if link_id in service_to_osm:
                                    osm_id = service_to_osm[link_id]
                                    new_traffic_data[osm_id] = speed
                                    success_count += 1
                                    if success_count % 100 == 0:
                                        logger.info(f"수집 중... {success_count}개 완료")
                
                # API 과부하 방지
                time.sleep(self.api_delay)
                
            except Exception as e:
                fail_count += 1
                continue
            
            # 진행 상황 표시 (500개마다)
            if (i + 1) % 500 == 0:
                logger.info(f"진행률: {i+1}/{total_links} ({(i+1)/total_links*100:.1f}%)")
        
        # 전역 변수 업데이트
        traffic_data = new_traffic_data
        logger.info(f"교통 데이터 수집 완료: {len(traffic_data)}개 (성공: {success_count}, 실패: {fail_count})")
        elapsed_time = total_links * (self.api_delay + 0.1)
        logger.info(f"예상 수집 시간: {elapsed_time:.0f}초 ({elapsed_time/60:.1f}분)")
    
    def modify_route_request(self, request_data):
        """라우팅 요청 수정 - 교통 데이터 반영"""
        # 교통 데이터가 있으면 적용
        if traffic_data:
            # 가장 간단한 방법: avoid_polygons 사용하지 않고 단순 시간 조정
            request_data['traffic_applied'] = True
        
        return request_data
    
    def calculate_real_time(self, route_response):
        """실제 교통 속도를 반영한 시간 재계산 - 상세 정보 보존"""
        if 'trip' not in route_response:
            return route_response
        
        # 🔧 수정: legs와 maneuvers 정보를 보존하면서 시간만 조정
        if traffic_data and 'legs' in route_response['trip']:
            avg_speed = sum(traffic_data.values()) / len(traffic_data)
            
            if avg_speed < 50:  # 50km/h 이하면 시간 증가
                factor = 50 / avg_speed
                
                # Trip summary 조정
                if 'summary' in route_response['trip']:
                    original_time = route_response['trip']['summary'].get('time', 0)
                    route_response['trip']['summary']['time'] = original_time * factor
                    route_response['trip']['summary']['traffic_time'] = original_time * factor
                
                # 각 leg별로 시간 조정 (상세 정보는 보존)
                for leg in route_response['trip']['legs']:
                    if 'summary' in leg:
                        leg_time = leg['summary'].get('time', 0)
                        leg['summary']['time'] = leg_time * factor
                    
                    # 각 maneuver별로 시간 조정 (instruction은 보존)
                    for maneuver in leg.get('maneuvers', []):
                        maneuver_time = maneuver.get('time', 0)
                        maneuver['time'] = maneuver_time * factor
        else:
            # 기존 로직 (legs가 없는 경우)
            if traffic_data:
                avg_speed = sum(traffic_data.values()) / len(traffic_data)
                if avg_speed < 50:  # 50km/h 이하면 시간 증가
                    factor = 50 / avg_speed
                    if 'summary' in route_response['trip']:
                        original_time = route_response['trip']['summary'].get('time', 0)
                        route_response['trip']['summary']['time'] = original_time * factor
                        route_response['trip']['summary']['traffic_time'] = original_time * factor
        
        return route_response
    
    def start_traffic_updater(self):
        """백그라운드에서 주기적으로 교통 데이터 업데이트"""
        def update_loop():
            # 시작 시 즉시 한 번 수집
            try:
                logger.info("첫 번째 교통 데이터 수집 시작...")
                self.fetch_traffic_data()
            except Exception as e:
                logger.error(f"초기 교통 데이터 수집 오류: {e}")
            
            # 주기적 업데이트
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

proxy = TrafficProxy()

@app.route('/status', methods=['GET'])
def status():
    """Valhalla 상태 전달 (pickup-service 헬스체크용)"""
    try:
        response = requests.get(f"{VALHALLA_URL}/status", timeout=5)
        return response.text, response.status_code, response.headers.items()
    except Exception as e:
        logger.error(f"Status check error: {e}")
        return jsonify({"error": "Valhalla unreachable"}), 503

@app.route('/route', methods=['POST'])
def proxy_route():
    """라우팅 요청 프록시 - 상세 정보 보존"""
    try:
        # 원본 요청 받기
        original_request = request.json
        logger.info(f"Route request received")
        logger.info(f"교통 데이터 수집: {len(traffic_data)}개")
        
        # 요청 수정
        modified_request = proxy.modify_route_request(original_request.copy())
        
        # Valhalla로 전달
        response = requests.post(
            f"{VALHALLA_URL}/route",
            json=modified_request,
            timeout=30
        )
        
        if response.status_code == 200:
            # 🔧 수정: 전체 응답 보존하면서 교통 정보만 추가
            result = response.json()
            
            # 기본 교통 정보 적용
            result = proxy.calculate_real_time(result)
            
            # 트래픽 정보 추가
            if 'trip' in result:
                result['trip']['has_traffic'] = True
                result['trip']['traffic_data_count'] = len(traffic_data)
                
                # 🔧 상세 정보 로깅
                if 'legs' in result['trip']:
                    logger.info(f"Route response: {len(result['trip']['legs'])} legs")
                    if result['trip']['legs']:
                        maneuvers_count = sum(len(leg.get('maneuvers', [])) for leg in result['trip']['legs'])
                        logger.info(f"Total maneuvers: {maneuvers_count}")
            
            return jsonify(result)
        else:
            logger.error(f"Valhalla error: {response.status_code}")
            return jsonify({"error": "Valhalla error"}), response.status_code
            
    except Exception as e:
        logger.error(f"Proxy error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/matrix', methods=['POST'])
def proxy_matrix_endpoint():
    """매트릭스 요청 프록시 (/matrix 엔드포인트 - get_valhalla_matrix.py가 사용)"""
    try:
        original_request = request.json
        logger.info("Matrix request received")
        
        # Valhalla의 sources_to_targets로 전달
        response = requests.post(
            f"{VALHALLA_URL}/sources_to_targets",
            json=original_request,
            timeout=60
        )
        
        if response.status_code == 200:
            logger.info("Matrix response successful")
            return jsonify(response.json())
        else:
            logger.error(f"Matrix request failed: {response.status_code}")
            return response.text, response.status_code, response.headers.items()
    
    except Exception as e:
        logger.error(f"Matrix proxy error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/sources_to_targets', methods=['POST'])
def proxy_matrix():
    """매트릭스 요청 프록시 (Valhalla가 사용하는 엔드포인트명)"""
    try:
        original_request = request.json
        
        # Valhalla로 직접 전달
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
    """헬스체크"""
    return jsonify({
        "status": "healthy",
        "traffic_data_count": len(traffic_data),
        "valhalla_url": VALHALLA_URL
    })

# 추가: Valhalla가 지원하는 모든 엔드포인트를 프록시로 전달
@app.route('/<path:path>', methods=['GET', 'POST'])
def proxy_all(path):
    """모든 다른 요청을 Valhalla로 전달"""
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
    app.run(host='0.0.0.0', port=8003, debug=False)  # debug=False로 변경