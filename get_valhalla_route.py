import requests
import json
import time
import logging
import argparse
import os

# 명령줄 인자 파싱
parser = argparse.ArgumentParser(description="Valhalla 경로 유틸리티")
parser.add_argument("--host", default=os.environ.get("VALHALLA_HOST", "localhost"), 
                    help="Valhalla 호스트 (기본값: localhost 또는 환경변수 VALHALLA_HOST)")
parser.add_argument("--port", type=int, default=int(os.environ.get("VALHALLA_PORT", "8002")), 
                    help="Valhalla 포트 (기본값: 8002 또는 환경변수 VALHALLA_PORT)")
args = parser.parse_args()

# 로깅 설정
logging.basicConfig(level=logging.INFO)

def get_turn_by_turn_route(start_loc, end_loc, costing="auto", use_traffic=True):
    """
    Valhalla /route API를 호출하여 두 지점 간의 Turn-by-Turn 경로를 가져옵니다.

    Args:
        start_loc (dict): {'lat': 위도, 'lon': 경도} 형식의 시작점
        end_loc (dict): {'lat': 위도, 'lon': 경도} 형식의 도착점
        costing (str): 사용할 비용 모델
        use_traffic (bool): 실시간 교통 데이터 사용 여부

    Returns:
        dict: Valhalla API의 경로 결과 (JSON)
              None: 오류 발생 시
    """
    if not start_loc or not end_loc:
         logging.error("Start and end locations are required.")
         return None

    # VALHALLA_HOST와 VALHALLA_PORT 사용 (프록시를 거치도록)
    host = os.environ.get("VALHALLA_HOST", args.host)
    port = int(os.environ.get("VALHALLA_PORT", args.port))
    valhalla_url = f"http://{host}:{port}"

    payload = {
        "locations": [start_loc, end_loc],
        "costing": costing,
        "directions_options": {
            "units": "kilometers", # 거리 단위
            "language": "ko-KR", # 경로 안내 언어 (한국어)
            "narrative": True,           # 추가
            "banner_instructions": True, # 추가
            "voice_instructions": True   # 추가
        },
        "costing_options": {
            costing: {
                "use_live_traffic": use_traffic
            }
        },
        "directions_type": "maneuvers", # 경로 안내 타입 (기본값)
        "shape_match": "edge_walk",     # 추가
        "filters": {                    # 추가
            "attributes": ["edge.way_id", "edge.names", "edge.length"],
            "action": "include"
        }
        # 필요시 추가 옵션: avoid_locations, date_time 등
    }

    headers = {'Content-type': 'application/json'}
    max_retries = 3
    retry_delay = 2
    timeout_seconds = 30 # 타임아웃 설정

    for attempt in range(max_retries):
        try:
            logging.info(f"Requesting route from {start_loc} to {end_loc} (Attempt {attempt+1}/{max_retries})...")
            logging.info(f"교통량 데이터 사용: {use_traffic}")
            response = requests.post(f"{valhalla_url}/route", json=payload, headers=headers, timeout=timeout_seconds)
            response.raise_for_status()
            route_data = response.json()
            # 응답에 trip 정보가 있는지 확인
            if 'trip' not in route_data:
                 logging.warning(f"Valhalla response successful but missing 'trip' data: {route_data}")
                 # 경로 못찾음 응답일 수 있음
                 return None # 또는 route_data 그대로 반환? 요구사항에 따라 결정
            return route_data

        except requests.exceptions.Timeout:
             logging.error(f"Valhalla /route API request timed out after {timeout_seconds}s (Attempt {attempt + 1}/{max_retries}).")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error querying Valhalla /route API (Attempt {attempt + 1}/{max_retries}): {e}")
        except json.JSONDecodeError as e:
             logging.error(f"Error decoding Valhalla route response: {e}")
             try:
                 logging.error(f"Response text: {response.text}")
             except:
                 pass
             return None # JSON 디코딩 에러는 재시도 의미 없을 수 있음
        except Exception as e:
             logging.error(f"Unexpected error during route calculation: {e}", exc_info=True)
             return None # 예기치 않은 오류 시 재시도 중단

        if attempt < max_retries - 1:
            logging.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        else:
            logging.error("Max retries reached. Failed to get route.")
            return None
            
if __name__ == '__main__':
    # 테스트용 예시 좌표
    start_location = {"lat": 37.5665, "lon": 126.9780} # 서울 시청
    end_location = {"lat": 37.5796, "lon": 126.9770}   # 경복궁

    print(f"Requesting route from {start_location} to {end_location}...")
    route_info = get_turn_by_turn_route(start_location, end_location)

    if route_info and 'trip' in route_info:
        print("\nRoute calculation successful!")
        # 경로 요약 정보 출력
        summary = route_info['trip']['summary']
        print(f"Total Time: {summary.get('time', 0):.0f} seconds")
        print(f"Total Distance: {summary.get('length', 0):.2f} km")

        # 첫 몇 개의 경로 안내 출력
        print("\nFirst few maneuvers:")
        legs = route_info['trip'].get('legs', [])
        if legs:
            maneuvers = legs[0].get('maneuvers', [])
            for i, maneuver in enumerate(maneuvers[:5]): # 처음 5개만 출력
                 instruction = maneuver.get('instruction', 'N/A')
                 distance = maneuver.get('length', 0)
                 time_sec = maneuver.get('time', 0)
                 print(f"  {i+1}. {instruction} ({distance:.2f} km, {time_sec:.0f} sec)")
        else:
             print("No legs found in the route.")
    else:
        print("\nFailed to get route or route data incomplete.")