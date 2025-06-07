import subprocess
import os
import numpy as np
import tempfile

LKH_EXECUTABLE = "/usr/local/bin/LKH" # Dockerfile에서 설정한 경로

def solve_tsp_with_lkh(time_matrix, initial_tour=None, runs=5):
    """
    주어진 시간 매트릭스를 사용하여 LKH로 TSP를 해결합니다.

    Args:
        time_matrix (numpy.ndarray): 노드 간 이동 시간 매트릭스 (N x N). 정수형이어야 함.
        initial_tour (list, optional): 초기 해로 사용할 노드 순서 리스트 (0-based index). Defaults to None.
        runs (int): LKH 실행 횟수. 높을수록 좋은 해를 찾을 확률 증가. Defaults to 5.

    Returns:
        tuple: (optimal_tour, optimal_cost)
               optimal_tour (list): 최적 경로의 노드 순서 (0-based index). 시작 노드로 돌아오는 경로.
               optimal_cost (float): 최적 경로의 총 시간 (초).
               None: 오류 발생 시
    """
    n = time_matrix.shape[0]
    if n == 0:
        return [], 0.0
    if n == 1:
        return [0], 0.0

    # 🔧 10개 노드까지 최적화된 파라미터 설정
    if n <= 5:
        runs = max(8, runs)
        time_limit = 15
        max_trials = 3000
    elif n <= 10:
        runs = max(15, runs)
        time_limit = 30
        max_trials = 8000
    elif n <= 20:
        runs = max(20, runs)
        time_limit = 45
        max_trials = 12000
    else:
        runs = max(25, runs)
        time_limit = 90
        max_trials = 20000

    # LKH는 정수 가중치를 선호하므로, 초 단위 시간을 정수로 변환
    int_time_matrix = np.round(time_matrix).astype(int)

    # LKH 입력 파일 생성 (임시 파일 사용)
    with tempfile.TemporaryDirectory() as tempdir:
        problem_filename = os.path.join(tempdir, "problem.tsp")
        param_filename = os.path.join(tempdir, "params.par")
        output_filename = os.path.join(tempdir, "output.tour")
        initial_tour_filename = os.path.join(tempdir, "initial.tour") if initial_tour else None

        # 1. Problem file (.tsp) 생성
        with open(problem_filename, 'w') as f:
            f.write(f"NAME : dynamic_tsp_{n}\n")
            f.write(f"TYPE : TSP\n")
            f.write(f"COMMENT : Dynamic TSP for delivery\n")
            f.write(f"DIMENSION : {n}\n")
            f.write(f"EDGE_WEIGHT_TYPE : EXPLICIT\n")
            f.write(f"EDGE_WEIGHT_FORMAT: FULL_MATRIX\n")
            f.write("EDGE_WEIGHT_SECTION\n")
            for i in range(n):
                f.write(" ".join(map(str, int_time_matrix[i])) + "\n")
            f.write("EOF\n")

        # 2. Initial tour file (.tour) 생성 (제공된 경우)
        if initial_tour_filename and initial_tour:
             # LKH tour 파일 형식: 1-based index, -1로 종료
            with open(initial_tour_filename, 'w') as f:
                f.write(f"NAME : initial_tour_{n}\n")
                f.write(f"TYPE : TOUR\n")
                f.write(f"DIMENSION : {n}\n")
                f.write("TOUR_SECTION\n")
                for node_index in initial_tour:
                    f.write(f"{node_index + 1}\n") # 0-based -> 1-based
                f.write("-1\n")
                f.write("EOF\n")

        # 3. 🔧 간소화된 Parameter file (.par) 생성
        with open(param_filename, 'w') as f:
            f.write(f"PROBLEM_FILE = {problem_filename}\n")
            f.write(f"OUTPUT_TOUR_FILE = {output_filename}\n")
            f.write(f"RUNS = {runs}\n")
            f.write(f"TIME_LIMIT = {time_limit}\n")
            f.write(f"MAX_TRIALS = {max_trials}\n")
            f.write("TRACE_LEVEL = 1\n")
            
            # 🔧 10개 노드까지 최적화된 품질 파라미터
            f.write("INITIAL_TOUR_ALGORITHM = NEAREST-NEIGHBOR\n")
            f.write("MOVE_TYPE = 5\n")
            f.write("PATCHING_C = 3\n")
            
            if n <= 5:
                f.write("MAX_CANDIDATES = 5\n")
                f.write("INITIAL_PERIOD = 100\n")
            elif n <= 10:
                f.write("MAX_CANDIDATES = 8\n")
                f.write("INITIAL_PERIOD = 80\n")
                f.write("KICK_TYPE = 4\n")
            else:
                f.write("MAX_CANDIDATES = 10\n")
                f.write("INITIAL_PERIOD = 50\n")
                f.write("KICK_TYPE = 4\n")
                f.write("KICKS = 1\n")
            
            if initial_tour_filename and initial_tour:
                f.write(f"INITIAL_TOUR_FILE = {initial_tour_filename}\n")

        # 4. LKH 실행
        try:
            process = subprocess.run([LKH_EXECUTABLE, param_filename], capture_output=True, text=True, check=True, timeout=time_limit + 30)

        except FileNotFoundError:
            print(f"Error: LKH executable not found at {LKH_EXECUTABLE}")
            return None, None
        except subprocess.CalledProcessError as e:
            print(f"Error running LKH: {e}")
            print(f"LKH stdout:\n{e.stdout}")
            print(f"LKH stderr:\n{e.stderr}")
            return None, None
        except subprocess.TimeoutExpired as e:
             print(f"Error: LKH execution timed out ({e.timeout} seconds).")
             print(f"LKH stdout so far:\n{e.stdout}")
             print(f"LKH stderr so far:\n{e.stderr}")
             return None, None

        # 5. Output tour file (.tour) 파싱
        try:
            with open(output_filename, 'r') as f:
                lines = f.readlines()

            # 비용 파싱
            optimal_cost = -1.0
            cost_line = next((line for line in process.stdout.split('\n') if "Cost.min =" in line or "Cost =" in line), None)
            if cost_line:
                 try:
                     optimal_cost_str = cost_line.split('=')[-1].strip()
                     optimal_cost = float(optimal_cost_str)
                 except ValueError:
                    print(f"Warning: Could not parse cost from LKH output line: {cost_line}")
            else:
                 print("Warning: Could not find cost information in LKH standard output.")

            # 경로 파싱
            tour_section_start = -1
            for i, line in enumerate(lines):
                if line.strip() == "TOUR_SECTION":
                    tour_section_start = i + 1
                    break

            if tour_section_start == -1:
                print(f"Error: Could not find TOUR_SECTION in {output_filename}")
                return None, None

            optimal_tour = []
            for line in lines[tour_section_start:]:
                node_str = line.strip()
                if node_str == "-1" or node_str == "EOF":
                    break
                try:
                    node_index_1based = int(node_str)
                    optimal_tour.append(node_index_1based - 1) # 1-based -> 0-based
                except ValueError:
                    print(f"Warning: Skipping invalid node index in tour file: {node_str}")
                    continue

            if not optimal_tour:
                 print(f"Error: No valid tour found in {output_filename}")
                 return None, None

            # 경로 유효성 검사
            if len(optimal_tour) != n or set(optimal_tour) != set(range(n)):
                 print(f"Error: Parsed tour is invalid. Expected {n} unique nodes, got {len(optimal_tour)}: {optimal_tour}")

            # 비용이 파싱되지 않았으면 경로 기반으로 비용 재계산
            if optimal_cost < 0 and len(optimal_tour) == n:
                print("Recalculating tour cost from the matrix...")
                calculated_cost = 0.0
                for i in range(n):
                    from_node = optimal_tour[i]
                    to_node = optimal_tour[(i + 1) % n] # 마지막 노드에서 시작 노드로 돌아옴
                    calculated_cost += time_matrix[from_node, to_node]
                optimal_cost = calculated_cost
                print(f"Recalculated cost: {optimal_cost}")

            return optimal_tour, optimal_cost

        except FileNotFoundError:
            print(f"Error: LKH output file not found at {output_filename}")
            return None, None
        except Exception as e:
            print(f"Error parsing LKH output: {e}")
            return None, None