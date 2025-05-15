from flask import Flask, request, jsonify
import numpy as np
import logging
import os
from run_lkh_internal import solve_tsp_with_lkh

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # FileHandler 제거
    ]
)
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"})

@app.route('/solve', methods=['POST'])
def solve_tsp():
    try:
        data = request.json
        
        # 'distances' 또는 'matrix' 필드 지원
        if 'distances' in data:
            distances = data['distances']
        elif 'matrix' in data:
            distances = data['matrix']
        else:
            return jsonify({"error": "Missing 'distances' or 'matrix' field"}), 400
        
        # 거리 행렬 형식 확인
        if not isinstance(distances, list) or not all(isinstance(row, list) for row in distances):
            return jsonify({"error": "Invalid distance matrix format"}), 400
        
        # 정방 행렬 확인
        n = len(distances)
        if n == 0 or any(len(row) != n for row in distances):
            return jsonify({"error": "Distance matrix must be square"}), 400
            
        # numpy 배열로 변환
        distance_matrix = np.array(distances)
        
        # 2개 이하 노드는 특별 처리 (LKH가 처리하지 못함)
        if n <= 2:
            logging.info(f"특별 처리: {n}개 노드")
            if n == 1:
                return jsonify({"tour": [0], "tour_length": 0.0})
            else:  # n == 2
                return jsonify({"tour": [0, 1], "tour_length": float(distance_matrix[0][1])})
        
        # LKH 파라미터 추출
        max_trials = data.get('max_trials', 1000)
        time_limit = data.get('time_limit', 300)
        seed = data.get('seed', 1)
        
        # TSP 풀기
        logging.info(f"TSP 해결 중 (노드 수: {n}, max_trials: {max_trials}, time_limit: {time_limit})")
        
        try:
            tour, tour_length = solve_tsp_with_lkh(
                distance_matrix, 
                runs=5  # 더 빠른 실행을 위해 runs 줄임
            )
            
            if tour is None:
                logging.error(f"LKH 실행 실패: tour is None")
                return jsonify({"error": "LKH solver returned None"}), 500
                
            logging.info(f"TSP 해결 완료: 경로 길이 = {tour_length:.2f}, 노드 수 = {len(tour)}")
            
            return jsonify({
                "tour": tour,
                "tour_length": float(tour_length)
            })
            
        except Exception as e:
            logging.error(f"LKH 실행 중 오류: {str(e)}", exc_info=True)
            return jsonify({"error": f"LKH execution error: {str(e)}"}), 500
        
    except Exception as e:
        logging.error(f"Error solving TSP: {str(e)}", exc_info=True)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logging.info("LKH TSP 서비스 시작...")
    app.run(host='0.0.0.0', port=5001)