import os, asyncio, subprocess, socket, time, uuid, sys
from typing import List, Tuple, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from pathlib import Path

# 셋업
# orchestrator/.env 파일 명시적으로 로드
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)
PUBLIC_HOST = os.getenv("PUBLIC_HOST")

if not PUBLIC_HOST:
    # 서버 기동 시점에 바로 알려주어 운용 중 500을 방지
    print("[FATAL] PUBLIC_HOST is not set in root .env", file=sys.stderr)

app = FastAPI()
sessions = {}  # 단일 프로세스/개발 단계용 인메모리 레지스트리

# 포트 정책
NOVNC_BASE = 6081
NOVNC_MAX = 65535
NOVNC_SCAN_STEPS = 5000  # 최대 스캔 개수


# 모델
class SessionResponse(BaseModel):
    session_id: str
    novnc_url: str


class SessionDetailResponse(BaseModel):
    session_id: str
    compose_project: str
    novnc_port: int
    url: str
    status: str
    started_at: float


class ErrorResponse(BaseModel):
    detail: str


# ──────────────────────────────────────────────────────────────────────────────
# 유틸: bind-probe로 포트 비어있는지 확인(가장 견고)
def port_free_bind(port: int) -> bool:
    """Return True iff binding 0.0.0.0:port is possible (i.e., really free)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False


def pick_novnc_port() -> int:
    """Scan from NOVNC_BASE upward with bind-probe; return first truly free port."""
    # (NOVNC_BASE..NOVNC_BASE+NOVNC_SCAN_STEPS-1) 범위 스캔
    upper = min(NOVNC_BASE + NOVNC_SCAN_STEPS - 1, NOVNC_MAX)
    for p in range(NOVNC_BASE, upper + 1):
        if port_free_bind(p):
            return p
    raise RuntimeError("no free noVNC port in the scanned range")


# ──────────────────────────────────────────────────────────────────────────────
# 유틸: compose 에러가 '포트 점유' 계열인지 감지
def is_port_allocated_error(stdout: str, stderr: str) -> bool:
    text = f"{stdout}\n{stderr}".lower()
    # moby/compose가 흔히 내는 메시지 패턴들
    patterns = [
        "address already in use",
        "port is already allocated",
        "ports are not available",
        "bind: address already in use",
        "failed to listen on",  # 보호적 패턴
    ]
    return any(p in text for p in patterns)


# ──────────────────────────────────────────────────────────────────────────────
# 유틸: 특정 세션의 agent 컨테이너 ID들(다중 매칭 포함) 조회
def get_agent_ids(session_id: str) -> List[str]:
    cmd = [
        "docker",
        "ps",
        "-aq",
        "--filter",
        f"label=bu.session={session_id}",
        "--filter",
        "label=bu.role=agent",
    ]
    out = subprocess.check_output(cmd).decode().strip()
    if not out:
        return []
    return [line for line in out.splitlines() if line.strip()]


# ──────────────────────────────────────────────────────────────────────────────
# 백그라운드: agent 종료 감지 & 세션 정리
async def wait_agent_and_cleanup(project: str, session_id: str):
    # 1) agent 컨테이너가 생성될 때까지 대기 (최대 60초)
    for _ in range(60):
        ids = get_agent_ids(session_id)
        if ids:
            break
        await asyncio.sleep(1)

    # 2) 하나라도 Running이면 계속 대기. 중간에 신규 ID가 생길 수 있으므로 주기적으로 갱신
    while True:
        ids = get_agent_ids(session_id)
        if not ids:
            break  # agent 컨테이너가 사라짐
        # 하나라도 Running이면 계속
        still_running = False
        for cid in ids:
            try:
                running = (
                    subprocess.check_output(
                        ["docker", "inspect", "-f", "{{.State.Running}}", cid]
                    )
                    .decode()
                    .strip()
                    .lower()
                )
                if running == "true":
                    still_running = True
                    break
            except subprocess.CalledProcessError:
                # 이미 사라진 컨테이너일 수 있음 → 무시
                continue
        if not still_running:
            break
        await asyncio.sleep(1)

    # 3) 종료 후 10초 대기 → 세션 전체 정리
    await asyncio.sleep(10)
    subprocess.call(
        [
            "docker",
            "compose",
            "-f",
            "../vnc/docker-compose.yml",
            "-p",
            project,
            "down",
            "-v",
        ]
    )
    sessions.pop(session_id, None)


# ──────────────────────────────────────────────────────────────────────────────
# compose up 실행: 포트 레이스 컨디션 대비 재시도 지원
async def compose_up_with_retry(
    project: str, base_env: dict, max_retries: int = 3
) -> Tuple[int, str]:
    """
    Compose up -d --build 를 실행하되,
    - NOVNC_PORT를 bind-probe로 고르고
    - 포트 점유로 실패하면 NOVNC_PORT를 바꿔 재시도
    Returns: (novnc_port, url)
    """
    last_err = ""
    for attempt in range(1, max_retries + 1):
        novnc_port = pick_novnc_port()
        env = base_env.copy()
        env["NOVNC_PORT"] = str(novnc_port)

        proc = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                "../vnc/docker-compose.yml",
                "-p",
                project,
                "up",
                "-d",
                "--build",
            ],
            env=env,
            capture_output=True,
            text=True,
        )

        if proc.returncode == 0:
            # 성공
            if not PUBLIC_HOST:
                # URL 생성이 불가하므로 방어적 에러
                raise HTTPException(
                    status_code=500, detail="PUBLIC_HOST is not configured"
                )
            url = f"http://{PUBLIC_HOST}:{novnc_port}/vnc.html?autoconnect=true&resize=scale"
            return novnc_port, url

        # 실패 → 포트 점유 원인인지 판단
        last_err = (proc.stdout or "") + "\n" + (proc.stderr or "")
        if (
            is_port_allocated_error(proc.stdout or "", proc.stderr or "")
            and attempt < max_retries
        ):
            # 다른 포트로 재시도
            continue
        else:
            break

    # 모든 시도 실패
    raise HTTPException(
        status_code=500,
        detail=f"Failed to start session (attempts={max_retries}). Last error:\n{last_err}",
    )


# ──────────────────────────────────────────────────────────────────────────────
# 핸들러들
@app.post(
    "/sessions",
    response_model=SessionResponse,
    responses={
        500: {
            "model": ErrorResponse,
            "description": "Server configuration error or Docker failure",
        }
    },
)
async def start_session():
    # 세션 식별자 & compose 프로젝트명
    session_id = uuid.uuid4().hex[:8]
    project = f"bu_{session_id}"

    # compose 환경
    base_env = os.environ.copy()
    base_env.update(
        {
            "SESSION_ID": session_id,
            "ENV_FILE": "../agent/.env",  # agent용 .env 경로 (compose 파일 기준 상대경로)
            # NOVNC_PORT는 compose_up_with_retry에서 동적으로 지정
        }
    )

    # compose up with retry (포트 레이스 컨디션 방어)
    novnc_port, url = await compose_up_with_retry(project, base_env, max_retries=3)

    # 인메모리 세션 레지스트리 기록
    sessions[session_id] = {
        "session_id": session_id,
        "compose_project": project,
        "novnc_port": novnc_port,
        "url": url,
        "status": "running",
        "started_at": time.time(),
    }

    # agent 종료 감지 & 정리 태스크
    asyncio.create_task(wait_agent_and_cleanup(project, session_id))

    return {"session_id": session_id, "novnc_url": url}


@app.get(
    "/sessions/{session_id}",
    response_model=SessionDetailResponse,
    responses={404: {"model": ErrorResponse, "description": "Session not found"}},
)
def get_session(session_id: str):
    data = sessions.get(session_id)
    if not data:
        raise HTTPException(status_code=404, detail="Session not found")
    return SessionDetailResponse(**data)
