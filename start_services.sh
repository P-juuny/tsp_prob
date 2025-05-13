#!/bin/bash
echo "===== TSP 서비스 시작 (다중 기사 모드) ====="

# 색상 설정
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[0;33m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# 모드 설정
SERVICE_MODE=${1:-multi_driver}  # 기본값: multi_driver

# 필요한 디렉토리 생성
echo -e "${BLUE}[1/5] 필요한 디렉토리 생성 중...${NC}"
mkdir -p logs
mkdir -p data
mkdir -p traffic_data
mkdir -p valhalla_data/valhalla_tiles

# valhalla.json 파일 존재 확인
if [ ! -f "valhalla.json" ]; then
    echo -e "${RED}[오류] valhalla.json 파일이 없습니다. 먼저 생성해주세요.${NC}"
    exit 1
fi

# 환경 변수 설정
export SERVICE_MODE=$SERVICE_MODE
echo -e "${YELLOW}서비스 모드: $SERVICE_MODE${NC}"

# Docker Compose 실행
echo -e "${BLUE}[2/5] Docker Compose 시작 중...${NC}"
docker-compose down  # 기존 컨테이너 정리
docker-compose up -d

# 각 서비스 상태 확인
echo -e "${BLUE}[3/5] 서비스 상태 확인 중...${NC}"

# Valhalla 서비스 대기
echo -e "${YELLOW}Valhalla 서비스 준비 대기 중...${NC}"
MAX_ATTEMPTS=60
ATTEMPT=1
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    if curl -s http://localhost:8002/status > /dev/null; then
        echo -e "${GREEN}[성공] Valhalla 서비스가 준비되었습니다.${NC}"
        break
    else
        echo "시도 $ATTEMPT/$MAX_ATTEMPTS: Valhalla 서비스 확인 중..."
        sleep 5
        ATTEMPT=$((ATTEMPT + 1))
    fi
done

# LKH 서비스 대기
echo -e "${YELLOW}LKH 서비스 준비 대기 중...${NC}"
ATTEMPT=1
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    if curl -s http://localhost:5001/health > /dev/null; then
        echo -e "${GREEN}[성공] LKH 서비스가 준비되었습니다.${NC}"
        break
    else
        echo "시도 $ATTEMPT/$MAX_ATTEMPTS: LKH 서비스 확인 중..."
        sleep 5
        ATTEMPT=$((ATTEMPT + 1))
    fi
done

# Traffic Proxy 대기
echo -e "${YELLOW}Traffic Proxy 준비 대기 중...${NC}"
sleep 10  # Traffic Proxy 초기화 시간 허용

# Pickup Service 대기
echo -e "${YELLOW}Pickup Service 준비 대기 중...${NC}"
ATTEMPT=1
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    if [ "$SERVICE_MODE" = "multi_driver" ]; then
        if curl -s http://localhost:5000/api/zone/status > /dev/null; then
            echo -e "${GREEN}[성공] Pickup Service (다중 기사 모드)가 준비되었습니다.${NC}"
            break
        fi
    else
        if curl -s http://localhost:5000/api/pickup/status > /dev/null; then
            echo -e "${GREEN}[성공] Pickup Service (단일 기사 모드)가 준비되었습니다.${NC}"
            break
        fi
    fi
    echo "시도 $ATTEMPT/$MAX_ATTEMPTS: Pickup Service 확인 중..."
    sleep 5
    ATTEMPT=$((ATTEMPT + 1))
done

# 서비스 상태 요약
echo -e "${BLUE}[4/5] 서비스 상태 요약${NC}"
echo -e "${GREEN}✓ Valhalla (경로 서비스): http://localhost:8002${NC}"
echo -e "${GREEN}✓ LKH (TSP 최적화): http://localhost:5001${NC}"
echo -e "${GREEN}✓ Traffic Proxy (교통 정보): 백그라운드 실행 중${NC}"
echo -e "${GREEN}✓ Pickup Service ($SERVICE_MODE): http://localhost:5000${NC}"

# 서비스 모드에 따른 추가 정보
echo -e "${BLUE}[5/5] 서비스 정보${NC}"
if [ "$SERVICE_MODE" = "multi_driver" ]; then
    echo -e "${PURPLE}=== 다중 기사 모드 ===${NC}"
    echo "구역별 배달기사 시스템:"
    echo "  - 강북서부: 은평구, 마포구, 서대문구 (3명)"
    echo "  - 강북동부: 도봉구, 노원구, 강북구, 성북구 (4명)"
    echo "  - 강북중부: 종로구, 중구, 용산구 외 (3명)"
    echo "  - 강남서부: 강서구, 양천구, 구로구 외 (5명)"
    echo "  - 강남동부: 성동구, 강남구, 송파구 외 (5명)"
    echo ""
    echo -e "${YELLOW}주요 API 엔드포인트:${NC}"
    echo "  - 구역 상태: http://localhost:5000/api/zone/status"
    echo "  - 픽업 추가: http://localhost:5000/api/pickup/add"
    echo "  - 구역별 최적화: http://localhost:5000/api/zone/optimize"
    echo "  - 기사별 다음 목적지: http://localhost:5000/api/pickup/next?zone=강북서부&driver_id=강북서부_driver_1"
else
    echo -e "${PURPLE}=== 단일 기사 모드 ===${NC}"
    echo "단일 배달기사 최적화 시스템"
    echo ""
    echo -e "${YELLOW}주요 API 엔드포인트:${NC}"
    echo "  - 시스템 상태: http://localhost:5000/api/pickup/status"
    echo "  - 픽업 추가: http://localhost:5000/api/pickup/add"
    echo "  - 다음 목적지: http://localhost:5000/api/pickup/next"
    echo "  - 수거 완료: http://localhost:5000/api/pickup/complete"
fi

echo ""
echo -e "${YELLOW}테스트 실행:${NC}"
echo "  python test.py $SERVICE_MODE"
echo ""
echo -e "${YELLOW}로그 모니터링:${NC}"
echo "  docker-compose logs -f"
echo ""
echo -e "${YELLOW}서비스 중지:${NC}"
echo "  docker-compose down"
echo ""

# 실시간 로그 모니터링 옵션
read -p "실시간 로그를 모니터링하시겠습니까? (y/N): " -r MONITOR_LOGS
if [[ $MONITOR_LOGS =~ ^[Yy]$ ]]; then
    echo -e "${BLUE}실시간 로그 모니터링 시작...${NC}"
    docker-compose logs -f
fi