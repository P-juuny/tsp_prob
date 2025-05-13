import requests
import json
import numpy as np
import time
import logging
import argparse
import os

# 명령줄 인자 파싱
parser = argparse.ArgumentParser(description="Valhalla 매트릭스 유틸리티")
parser.add_argument("--host", default=os.environ.get("VALHALLA_HOST", "localhost"), 
                    help="Valhalla 호스트 (기본값: localhost 또는 환경변수 VALHALLA_HOST)")
parser.add_argument("--port", type=int, default=int(os.environ.get("VALHALLA_PORT", "8002")), 
                    help="Valhalla 포트 (기본값: 8002 또는 환경변수 VALHALLA_PORT)")
args = parser.parse_args()

# 로깅 설정
logging.basicConfig(level=logging.INFO)

def get_time_distance_matrix(locations, costing="auto", use_traffic=True):
    """
    Valhalla API를 호출하여 시간 및 거리 매트릭스를 가져옵니다.
    
    Args:
        locations (list): [{'lat': 위도, 'lon': 경도}, ...] 형식의 위치 리스트
        costing (str): 사용할 비용 모델 (e.g., "auto", "truck", "bicycle", "pedestrian")
        use_traffic (bool): 실시간 교통 데이터 사용 여부

    Returns:
        tuple: (time_matrix, distance_matrix)
               time_matrix (numpy.ndarray): 초 단위 이동 시간 매트릭스 (N x N)
               distance_matrix (numpy.ndarray): 킬로미터 단위 이동 거리 매트릭스 (N x N)
               (None, None): 오류 발생 시
    """
    if not locations or len(locations) < 2:
        logging.error("Error: Need at least two locations for matrix calculation.")
        return None, None

    # VALHALLA_HOST와 VALHALLA_PORT 사용 (프록시를 거치도록)
    host = os.environ.get("VALHALLA_HOST", args.host)
    port = int(os.environ.get("VALHALLA_PORT", args.port))
    valhalla_url = f"http://{host}:{port}"

    n = len(locations)
    payload = {
        "sources": locations,
        "targets": locations,
        "costing": costing,
        "units": "kilometers",
        "costing_options": {
            costing: {
                "use_live_traffic": use_traffic
            }
        }
    }

    headers = {'Content-type': 'application/json'}
    max_retries = 3
    retry_delay = 2 # 초
    timeout_seconds = 60 # 타임아웃 설정 (매트릭스 계산은 오래 걸릴 수 있음)

    for attempt in range(max_retries):
        try:
            logging.info(f"Requesting matrix from Valhalla at {valhalla_url} (Attempt {attempt + 1}/{max_retries})...")
            logging.info(f"교통량 데이터 사용: {use_traffic}")
            
            # matrix 엔드포인트 사용 (프록시가 처리)
            response = requests.post(f"{valhalla_url}/matrix", json=payload, headers=headers, timeout=timeout_seconds)
            response.raise_for_status() # 오류 발생 시 예외 발생
            data = response.json()

            # 결과 파싱 (시간과 거리 분리)
            time_matrix = np.full((n, n), -1.0, dtype=float)
            distance_matrix = np.full((n, n), -1.0, dtype=float)
            found_routes = 0

            if 'sources_to_targets' in data:
                for i, source_data in enumerate(data['sources_to_targets']):
                    if source_data: # source_data가 None이 아닌 경우
                        for j, target_data in enumerate(source_data):
                            if target_data and target_data.get('time') is not None and target_data.get('distance') is not None:
                                time_matrix[i, j] = target_data['time']
                                distance_matrix[i, j] = target_data['distance']
                                found_routes += 1
                            else:
                                # 경로가 없는 경우 (매우 큰 값 또는 특정 값으로 처리)
                                logging.warning(f"No route found between location {i} and {j}. Assigning large penalty.")
                                time_matrix[i, j] = 9999999  # 매우 큰 시간 (LKH가 피하도록)
                                distance_matrix[i, j] = 9999999 # 매우 큰 거리
                    else:
                        logging.warning(f"No target data found for source {i}. Assigning large penalties for this row.")
                        time_matrix[i, :] = 9999999
                        distance_matrix[i, :] = 9999999

            # 매트릭스가 제대로 채워졌는지 확인
            if found_routes == 0:
                logging.error("Failed to calculate any routes between locations.")
                return None, None
            elif np.any(time_matrix == -1.0) or np.any(distance_matrix == -1.0):
                # 모든 경로가 발견되지 않은 경우 처리
                logging.warning("Some routes could not be calculated. Matrix might be incomplete.")

            logging.info("Matrix calculation successful.")
            return time_matrix, distance_matrix

        except requests.exceptions.Timeout:
            logging.error(f"Valhalla API request timed out after {timeout_seconds}s (Attempt {attempt + 1}/{max_retries}).")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error querying Valhalla API (Attempt {attempt + 1}/{max_retries}): {e}")
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding Valhalla response: {e}")
            try:
                logging.error(f"Response text: {response.text}")
            except:
                pass
            # JSON 디코딩 에러는 재시도 의미 없을 수 있음
            return None, None
        except Exception as e:
            logging.error(f"Unexpected error during matrix calculation: {e}", exc_info=True)
            return None, None # 예기치 않은 오류 시 재시도 중단

        if attempt < max_retries - 1:
            logging.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
        else:
            logging.error("Max retries reached. Failed to get matrix.")
            return None, None

if __name__ == '__main__':
    # 테스트용 예시 좌표 (서울 시내)
    example_locations = [
        {"lat": 37.5665, "lon": 126.9780}, # 서울 시청
        {"lat": 37.5796, "lon": 126.9770}, # 경복궁
        {"lat": 37.5512, "lon": 126.9882}, # 명동역
        {"lat": 37.5326, "lon": 127.0246}  # 강남역 (테스트용)
    ]
    print(f"Requesting matrix for {len(example_locations)} locations...")
    times, distances = get_time_distance_matrix(example_locations)

    if times is not None and distances is not None:
        print("\nTime Matrix (seconds):")
        print(np.round(times).astype(int))

        print("\nDistance Matrix (kilometers):")
        print(np.round(distances, 2))
    else:
        print("\nFailed to get matrix.")