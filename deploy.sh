#!/bin/bash

# 색상 설정
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

echo -e "${GREEN}=== TSP 서비스 배포 시작 ===${NC}"

# Docker 설치 확인
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker가 설치되어 있지 않습니다. 설치해주세요.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose가 설치되어 있지 않습니다. 설치해주세요.${NC}"
    exit 1
fi

# 필요한 디렉토리 생성
echo -e "${YELLOW}필요한 디렉토리 생성 중...${NC}"
mkdir -p logs/pickup logs/delivery logs/lkh
mkdir -p data
mkdir -p valhalla_data

# 환경 변수 확인
if [ ! -f .env ]; then
    echo -e "${RED}.env 파일이 없습니다. .env.example을 참고하여 생성해주세요.${NC}"
    exit 1
fi

# 기존 컨테이너 정리
echo -e "${YELLOW}기존 컨테이너 정리 중...${NC}"
docker-compose down

# 이미지 빌드
echo -e "${YELLOW}Docker 이미지 빌드 중...${NC}"
docker-compose build

# 서비스 시작
echo -e "${YELLOW}서비스 시작 중...${NC}"
docker-compose up -d

# 상태 확인
echo -e "${YELLOW}서비스 상태 확인 중...${NC}"
sleep 10

docker-compose ps

echo -e "${GREEN}배포 완료!${NC}"
echo "서비스 상태 확인: docker-compose ps"
echo "로그 확인: docker-compose logs -f"