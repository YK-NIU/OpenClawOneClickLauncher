# -*- coding: utf-8 -*-

import json
import locale
import os
import re
import socket
import subprocess
import threading
import time
import webbrowser
from dataclasses import dataclass, asdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import parse as urlparse
from urllib import request as urlrequest


APP_NAME = '\u5c0f\u6cb3\u72f8\u517b\u9f99\u867e'
APP_DIR = Path(os.environ.get('APPDATA', str(Path.home()))) / 'OpenClawLauncher'
CONFIG_PATH = APP_DIR / 'config.json'
DEFAULT_NPM_PREFIX = APP_DIR / 'npm'

GATEWAY_HOST = '127.0.0.1'
GATEWAY_PORT = 18789

# Alibaba DashScope (Bailian) OpenAI-compatible endpoint
DASHSCOPE_BASE_URL = 'https://dashscope.aliyuncs.com/compatible-mode/v1'

# User requirement: only these three models, with fallback order.
DEFAULT_MODELS = [
    'qwen3.5-plus',
    'MiniMax/MiniMax-M2.5',
    'deepseek-v3.2',
]


@dataclass
class LauncherConfig:
    api_key: str = ''
    channel: str = 'stable'  # stable | beta | dev
    use_cn_registry: bool = True
    auto_install_deps: bool = True  # Windows: auto install Node.js + Git when missing


def _safe_mkdir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def load_config() -> LauncherConfig:
    try:
        if CONFIG_PATH.exists():
            data = json.loads(CONFIG_PATH.read_text(encoding='utf-8'))
            if isinstance(data, dict):
                allowed = {}
                for k in ['api_key', 'channel', 'use_cn_registry', 'auto_install_deps']:
                    if k in data:
                        allowed[k] = data.get(k)
                return LauncherConfig(**allowed)
    except Exception:
        pass
    return LauncherConfig()


def save_config(cfg: LauncherConfig) -> None:
    _safe_mkdir(APP_DIR)
    CONFIG_PATH.write_text(json.dumps(asdict(cfg), ensure_ascii=False, indent=2), encoding='utf-8')


def _preferred_encoding() -> str:
    try:
        return locale.getpreferredencoding(False) or 'utf-8'
    except Exception:
        return 'utf-8'


def _creationflags_no_window() -> int:
    if os.name != 'nt':
        return 0
    return subprocess.CREATE_NO_WINDOW


def _npm_prefix() -> Path:
    return DEFAULT_NPM_PREFIX


def _openclaw_cmd_path(prefix: Path) -> Path:
    if os.name == 'nt':
        return prefix / 'openclaw.cmd'
    return prefix / 'bin' / 'openclaw'


def _win_cmdline(args: list[str]) -> list[str]:
    # Run through cmd.exe on Windows so .cmd shims work reliably.
    if os.name != 'nt':
        return args
    cmdline = subprocess.list2cmdline(args)
    return ['cmd.exe', '/d', '/s', '/c', cmdline]


def _fmt_cmd(args: list[str]) -> str:
    if os.name == 'nt':
        return subprocess.list2cmdline(args)
    return ' '.join(args)


def _redact(s: str) -> str:
    s = re.sub(r'(#token=)[^\s]+', r'\1<redacted>', s)
    return s


def _with_env(cfg: LauncherConfig) -> dict:
    env = dict(os.environ)

    if cfg.use_cn_registry:
        env['npm_config_registry'] = 'https://registry.npmmirror.com/'

    env['npm_config_progress'] = 'false'
    env['npm_config_loglevel'] = 'info'

    prefix = _npm_prefix()
    env['npm_config_prefix'] = str(prefix)
    env['PATH'] = str(prefix) + os.pathsep + env.get('PATH', '')

    # Provide DashScope key to OpenClaw and any OpenAI-compatible clients.
    if cfg.api_key:
        env['DASHSCOPE_API_KEY'] = cfg.api_key
        env['CUSTOM_API_KEY'] = cfg.api_key

    return env


class LogBuffer:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._lines: list[str] = []
        self._max_lines = 2500

    def write(self, line: str) -> None:
        line = _redact(line)
        with self._lock:
            self._lines.append(line)
            if len(self._lines) > self._max_lines:
                self._lines = self._lines[-self._max_lines :]

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self._lines)


LOG = LogBuffer()




def _decode_subprocess_bytes(b: bytes) -> str:
    # Many modern Windows CLIs (notably winget) emit UTF-8 even when the system
    # ANSI code page is cp936. Decode UTF-8 first, then fall back.
    if not b:
        return ''

    for enc in ('utf-8', _preferred_encoding(), 'gbk', 'cp936'):
        try:
            return b.decode(enc)
        except Exception:
            continue

    return b.decode('utf-8', errors='replace')


def _iter_decoded_lines(stream):
    while True:
        raw = stream.readline()
        if not raw:
            break
        if isinstance(raw, str):
            yield raw
        else:
            yield _decode_subprocess_bytes(raw)


def _run_and_stream(args: list[str], env: dict, cwd: str | None, on_line) -> int:
    on_line(f'$ {_fmt_cmd(args)}')

    try:
        p = subprocess.Popen(
            args,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
            creationflags=_creationflags_no_window(),
        )
    except FileNotFoundError as e:
        raise RuntimeError(f'Executable not found: {args[0]}') from e

    assert p.stdout is not None
    for line in _iter_decoded_lines(p.stdout):
        on_line(line.rstrip())

    return p.wait()


def _run_capture(args: list[str], env: dict, cwd: str | None) -> tuple[int, str]:
    try:
        p = subprocess.Popen(
            args,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
            creationflags=_creationflags_no_window(),
        )
    except FileNotFoundError as e:
        raise RuntimeError(f'Executable not found: {args[0]}') from e

    out_b, _ = p.communicate()
    if isinstance(out_b, str):
        out_s = out_b
    else:
        out_s = _decode_subprocess_bytes(out_b or b'')
    return p.returncode, out_s or ''


def _check_node(on_line) -> None:
    try:
        out = subprocess.check_output(
            ['node', '--version'],
            text=True,
            encoding=_preferred_encoding(),
            errors='replace',
            env=dict(os.environ),
        )
    except FileNotFoundError as e:
        raise RuntimeError('未检测到 Node.js。OpenClaw 官方要求 Node 22+。请先安装 Node.js 后再继续。') from e

    v = out.strip().lstrip('v')
    major = int(v.split('.')[0])
    on_line(f'Node: v{v}')
    if major < 22:
        raise RuntimeError(f'当前 Node 版本为 v{v}，OpenClaw 需要 Node 22+。请升级后重试。')





def _prepend_path_dir(d: str) -> None:
    d = (d or '').strip()
    if not d:
        return
    cur = os.environ.get('PATH', '') or ''
    parts = cur.split(os.pathsep) if cur else []
    if d in parts:
        return
    os.environ['PATH'] = d + (os.pathsep + cur if cur else '')


def _add_common_windows_tools_to_path() -> None:
    if os.name != 'nt':
        return
    candidates = [
        r'C:\Program Files\nodejs',
        r'C:\Program Files\Git\cmd',
        r'C:\Program Files\Git\bin',
    ]
    home = Path.home()
    candidates.extend(
        [
            str(home / 'AppData' / 'Local' / 'Programs' / 'Git' / 'cmd'),
            str(home / 'AppData' / 'Local' / 'Programs' / 'Git' / 'bin'),
            str(home / 'AppData' / 'Local' / 'Programs' / 'nodejs'),
        ]
    )
    for d in candidates:
        try:
            if Path(d).exists():
                _prepend_path_dir(d)
        except Exception:
            pass


def _check_git(on_line) -> None:
    # Some npm dependency specs require invoking git.exe (git+https, etc).
    _add_common_windows_tools_to_path()
    try:
        out = subprocess.check_output(
            ['git', '--version'],
            text=True,
            encoding=_preferred_encoding(),
            errors='replace',
            env=dict(os.environ),
        )
    except FileNotFoundError as e:
        raise RuntimeError(
            '未检测到 Git (git.exe)。部分 npm 依赖需要 Git。请安装 Git for Windows 后重试。'
        ) from e

    on_line(out.strip())


def _extract_npm_log_path(lines: list[str]) -> str:
    for s in lines:
        m = re.search(r'A complete log of this run can be found in:\s*(\S+)', s)
        if m:
            return m.group(1).strip()
    return ''


def _fmt_install_failure(lines: list[str]) -> str:
    joined = chr(10).join(lines)

    if ('syscall spawn git' in joined) or ('path git' in joined) or (('spawn git' in joined) and ('ENOENT' in joined)):
        msg = 'Install failed: npm cannot find git.exe (a dependency requires Git).'
    elif ('connect EACCES' in joined) or ('ECONNREFUSED' in joined) or ('ETIMEDOUT' in joined):
        msg = 'Install failed: network connection was refused/blocked/timeout.'
    elif ('EACCES' in joined) or ('EPERM' in joined):
        msg = 'Install failed: permission denied or file is in use.'
    else:
        msg = 'Install failed. See logs for details.'

    log_path = _extract_npm_log_path(lines)
    if log_path:
        msg += chr(10) + 'npm debug log: ' + str(log_path)
    return msg


def _winget_available() -> bool:
    if os.name != 'nt':
        return False
    try:
        _ = subprocess.check_output(
            ['winget', '--version'],
            text=True,
            encoding=_preferred_encoding(),
            errors='replace',
            env=dict(os.environ),
            creationflags=_creationflags_no_window(),
        )
        return True
    except Exception:
        return False


def _winget_install(pkg_id: str, on_line) -> int:
    env = dict(os.environ)
    base = [
        'winget',
        'install',
        '--id',
        pkg_id,
        '-e',
        '--source',
        'winget',
        '--silent',
        '--accept-package-agreements',
        '--accept-source-agreements',
    ]
    args = base + ['--disable-interactivity']
    on_line(f'Attempting auto install via winget: {pkg_id}')
    code = _run_and_stream(_win_cmdline(args), env=env, cwd=None, on_line=on_line)
    if code == 0:
        return 0
    on_line('winget install failed; retrying without --disable-interactivity')
    return _run_and_stream(_win_cmdline(base), env=env, cwd=None, on_line=on_line)


def _ensure_node_ready(cfg: LauncherConfig, on_line) -> None:
    _add_common_windows_tools_to_path()
    try:
        _check_node(on_line)
        return
    except Exception as e:
        if os.name != 'nt' or not getattr(cfg, 'auto_install_deps', False):
            raise
        on_line('未检测到 Node.js 或版本过低，尝试通过 winget 自动安装/升级...')
        if not _winget_available():
            raise RuntimeError('缺少 Node.js 且 winget 不可用，无法自动安装。请手动安装 Node.js 22+ 后重试。') from e
        rc = _winget_install('OpenJS.NodeJS.LTS', on_line)
        if rc != 0:
            _ = _winget_install('OpenJS.NodeJS', on_line)
        _add_common_windows_tools_to_path()
        _check_node(on_line)

def _ensure_git_ready(cfg: LauncherConfig, on_line) -> None:
    _add_common_windows_tools_to_path()
    try:
        _check_git(on_line)
        return
    except Exception as e:
        if os.name != 'nt' or not getattr(cfg, 'auto_install_deps', False):
            raise
        on_line('Git not available; trying to install via winget...')
        if not _winget_available():
            raise RuntimeError('Git missing and winget is unavailable; cannot auto-install. Please install Git for Windows manually.') from e
        _ = _winget_install('Git.Git', on_line)
        _add_common_windows_tools_to_path()
        _check_git(on_line)


def ensure_openclaw_installed(cfg: LauncherConfig, on_line, update: bool) -> Path:
    prefix = _npm_prefix()
    cmd = _openclaw_cmd_path(prefix)

    _safe_mkdir(prefix)

    _ensure_node_ready(cfg, on_line)

    env = _with_env(cfg)
    on_line(f'npm prefix: {prefix}')
    if cfg.use_cn_registry:
        on_line('npm registry: https://registry.npmmirror.com/')

    if cmd.exists() and not update:
        on_line(f'Detected OpenClaw: {cmd}')
        _run_and_stream(_win_cmdline([str(cmd), '--version']), env=env, cwd=None, on_line=on_line)
        return cmd

    _ensure_git_ready(cfg, on_line)

    env = _with_env(cfg)

    pkg = 'openclaw@latest'
    if cfg.channel == 'beta':
        pkg = 'openclaw@beta'
    elif cfg.channel == 'dev':
        pkg = 'openclaw@dev'

    _run_and_stream(_win_cmdline(['npm', '--version']), env=env, cwd=None, on_line=on_line)

    install_lines: list[str] = []

    def tee(line: str) -> None:
        install_lines.append(line)
        on_line(line)

    code = _run_and_stream(
        _win_cmdline(
            [
                'npm',
                'install',
                '-g',
                pkg,
                '--prefix',
                str(prefix),
                '--no-audit',
                '--no-fund',
                '--progress=false',
                '--loglevel=info',
            ]
        ),
        env=env,
        cwd=None,
        on_line=tee,
    )

    if code != 0:
        raise RuntimeError(_fmt_install_failure(install_lines))

    if not cmd.exists():
        raise RuntimeError(f'已执行 npm 安装，但未找到 openclaw 启动文件: {cmd}')

    _run_and_stream(_win_cmdline([str(cmd), '--version']), env=env, cwd=None, on_line=on_line)
    return cmd

def ensure_openclaw_uninstalled(cfg: LauncherConfig, on_line) -> None:
    prefix = _npm_prefix()
    cmd = _openclaw_cmd_path(prefix)

    # Need Node.js to run npm uninstall.
    _ensure_node_ready(cfg, on_line)
    env = _with_env(cfg)

    on_line(f'npm prefix: {prefix}')

    if _tcp_connect_ok(GATEWAY_HOST, GATEWAY_PORT):
        on_line(f'WARN: 检测到 Gateway 仍在监听 {GATEWAY_HOST}:{GATEWAY_PORT}。卸载不会强制结束后台进程。')

    if cmd.exists():
        on_line(f'Detected OpenClaw: {cmd}')
    else:
        on_line('未检测到已安装的 OpenClaw（仍会尝试清理安装目录）。')

    _run_and_stream(_win_cmdline(['npm', '--version']), env=env, cwd=None, on_line=on_line)

    code = _run_and_stream(
        _win_cmdline([
            'npm','uninstall','-g','openclaw','--prefix',str(prefix),
            '--no-audit','--no-fund','--progress=false','--loglevel=info',
        ]),
        env=env,
        cwd=None,
        on_line=on_line,
    )
    if code != 0:
        on_line(f'WARN: npm uninstall 返回码 {code}，将继续尝试清理本地文件。')

    try:
        import shutil
        if prefix.exists():
            shutil.rmtree(prefix, ignore_errors=False)
            on_line(f'已删除安装目录: {prefix}')
    except Exception as e:
        on_line(f'WARN: 删除安装目录失败: {e}')

    on_line('卸载完成。')


def ensure_openclaw_setup(cfg: LauncherConfig, openclaw_cmd: Path, on_line) -> None:
    env = _with_env(cfg)

    cfg_file = Path.home() / '.openclaw' / 'openclaw.json'
    if cfg_file.exists():
        on_line(f'OpenClaw config exists: {cfg_file}')
        return

    lines: list[str] = []

    def tee(line: str) -> None:
        lines.append(line)
        on_line(line)

    code = _run_and_stream(
        _win_cmdline([str(openclaw_cmd), 'setup', '--mode', 'local', '--non-interactive']),
        env=env,
        cwd=None,
        on_line=tee,
    )
    if code == 0:
        return

    joined = '\n'.join(lines)
    if ('explicit risk acknowledgement' in joined) or ('accept-risk' in joined):
        on_line('setup non-interactive requires risk acknowledgement; falling back to onboard --accept-risk')
        extra_flags: list[str] = []
        rc_help, help_out = _run_capture(_win_cmdline([str(openclaw_cmd), 'onboard', '--help']), env=env, cwd=None)
        if rc_help == 0 and ('--skip-health' in help_out):
            extra_flags.append('--skip-health')
        code2 = _run_and_stream(
            _win_cmdline(
                [
                    str(openclaw_cmd),
                    'onboard',
                    '--non-interactive',
                    '--accept-risk',
                    '--mode',
                    'local',
                    '--flow',
                    'quickstart',
                    '--auth-choice',
                    'skip',
                    '--skip-ui',
                    '--skip-channels',
                    '--skip-search',
                    '--skip-skills',
                    '--no-install-daemon',
                    '--gateway-bind',
                    'loopback',
                    '--gateway-port',
                    str(GATEWAY_PORT),
                    *extra_flags,
                ]
            ),
            env=env,
            cwd=None,
            on_line=on_line,
        )
        if code2 != 0:
            raise RuntimeError('OpenClaw onboard(用于初始化配置)失败。请查看日志。')
        return

    raise RuntimeError('OpenClaw setup 失败。请查看日志。')


def _tcp_connect_ok(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.25):
            return True
    except Exception:
        return False


def _spawn_detached(args: list[str], env: dict, on_line) -> None:
    if os.name != 'nt':
        subprocess.Popen(args, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL)
        return

    creationflags = subprocess.CREATE_NO_WINDOW | subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    try:
        subprocess.Popen(
            args,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
    except Exception as e:
        on_line(f'WARN: Failed to spawn detached process: {e}')
        raise


def ensure_gateway_running(cfg: LauncherConfig, openclaw_cmd: Path, on_line) -> None:
    env = _with_env(cfg)

    if _tcp_connect_ok(GATEWAY_HOST, GATEWAY_PORT):
        on_line(f'Gateway already listening: {GATEWAY_HOST}:{GATEWAY_PORT}')
        return

    on_line('Starting gateway...')
    gateway_args = _win_cmdline(
        [
            str(openclaw_cmd),
            'gateway',
            '--allow-unconfigured',
            '--bind',
            'loopback',
            '--port',
            str(GATEWAY_PORT),
            '--force',
            '--auth',
            'none',
            'run',
        ]
    )
    _spawn_detached(gateway_args, env=env, on_line=on_line)

    deadline = time.time() + 25.0
    while time.time() < deadline:
        if _tcp_connect_ok(GATEWAY_HOST, GATEWAY_PORT):
            on_line('Gateway is up.')
            return
        time.sleep(0.4)

    raise RuntimeError('Gateway 未能在预期时间内启动（端口仍不可达）。')


def _extract_default_model_id_from_status(status_json_text: str) -> str:
    try:
        data = json.loads(status_json_text)
        for k in ['defaultModel', 'default_model', 'defaultModelId', 'default_model_id']:
            v = data.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for path in [
            ('models', 'default'),
            ('models', 'defaultModel'),
            ('models', 'defaultModelId'),
        ]:
            cur = data
            ok = True
            for seg in path:
                if isinstance(cur, dict) and seg in cur:
                    cur = cur[seg]
                else:
                    ok = False
                    break
            if ok and isinstance(cur, str) and cur.strip():
                return cur.strip()
    except Exception:
        pass

    m = re.search(r'([A-Za-z0-9_.-]+/qwen3\.5-plus)', status_json_text)
    if m:
        return m.group(1)
    return ''


def ensure_bailian_configured(cfg: LauncherConfig, openclaw_cmd: Path, on_line) -> None:
    if not cfg.api_key:
        raise RuntimeError('请先填写百炼 API Key。')

    env = _with_env(cfg)
    primary = DEFAULT_MODELS[0]

    extra_flags: list[str] = []
    rc_help, help_out = _run_capture(_win_cmdline([str(openclaw_cmd), 'onboard', '--help']), env=env, cwd=None)
    if rc_help == 0 and ('--skip-health' in help_out):
        extra_flags.append('--skip-health')

    # Onboard for Bailian.
    on_line('Running non-interactive onboard for Bailian...')
    args = [
        str(openclaw_cmd),
        'onboard',
        '--non-interactive',
        '--accept-risk',
        '--secret-input-mode',
        'ref',
        '--auth-choice',
        'custom-api-key',
        '--custom-base-url',
        DASHSCOPE_BASE_URL,
        '--custom-model-id',
        primary,
        '--custom-compatibility',
        'openai',
        '--skip-ui',
        '--skip-channels',
        '--skip-search',
        '--skip-skills',
        '--no-install-daemon',
        *extra_flags,
    ]

    code = _run_and_stream(_win_cmdline(args), env=env, cwd=None, on_line=on_line)
    if code != 0:
        raise RuntimeError('OpenClaw 百炼初始化(一键配置)失败。请查看日志。')

    # Configure default model + fallbacks order.
    on_line('Configuring default model + fallbacks...')
    rc, status_out = _run_capture(_win_cmdline([str(openclaw_cmd), 'models', 'status', '--json']), env=env, cwd=None)
    if rc != 0:
        on_line('WARN: openclaw models status --json failed; skipping fallbacks configuration.')
        return

    default_id = _extract_default_model_id_from_status(status_out)
    if not default_id:
        on_line('WARN: Could not determine default model id from status; skipping fallbacks configuration.')
        return

    prefix = None
    if default_id.endswith('/' + primary):
        prefix = default_id[: -len(primary)]
    elif default_id.endswith(primary):
        prefix = default_id[: -len(primary)]

    if prefix is None:
        on_line(f'WARN: Unexpected default model id: {default_id}; skipping fallbacks configuration.')
        return

    full_primary = prefix + primary
    full_fallbacks = [prefix + m for m in DEFAULT_MODELS[1:]]

    _run_and_stream(_win_cmdline([str(openclaw_cmd), 'models', 'set', full_primary]), env=env, cwd=None, on_line=on_line)

    rc2, help_out = _run_capture(_win_cmdline([str(openclaw_cmd), 'models', 'fallbacks', '--help']), env=env, cwd=None)
    if rc2 != 0 and ('fallback' not in help_out.lower()):
        on_line('WARN: openclaw models fallbacks not available; skipping fallbacks configuration.')
        return

    _run_and_stream(_win_cmdline([str(openclaw_cmd), 'models', 'fallbacks', 'clear']), env=env, cwd=None, on_line=on_line)
    for m in full_fallbacks:
        code = _run_and_stream(_win_cmdline([str(openclaw_cmd), 'models', 'fallbacks', 'add', m]), env=env, cwd=None, on_line=on_line)
        if code != 0:
            on_line(f'WARN: Failed to add fallback: {m}')


def _extract_dashboard_url(lines: list[str]) -> str:
    for s in lines:
        m = re.search(r'Dashboard URL:\s*(https?://\S+)', s)
        if m:
            return m.group(1).strip()
    return ''


def _wait_http_ready(url: str, on_line) -> None:
    base, _frag = urlparse.urldefrag(url)

    deadline = time.time() + 20.0
    last_err = None
    while time.time() < deadline:
        try:
            with urlrequest.urlopen(base, timeout=2) as r:
                _ = r.status
            return
        except Exception as e:
            last_err = e
            time.sleep(0.5)

    if last_err is not None:
        on_line(f'WARN: Dashboard URL still not responding: {last_err}')


def open_dashboard(cfg: LauncherConfig, openclaw_cmd: Path, on_line) -> None:
    env = _with_env(cfg)

    collected: list[str] = []

    def collect(line: str) -> None:
        collected.append(line)
        on_line(line)

    code = _run_and_stream(_win_cmdline([str(openclaw_cmd), 'dashboard', '--no-open']), env=env, cwd=None, on_line=collect)
    if code != 0:
        raise RuntimeError('获取 Dashboard URL 失败。请查看日志。')

    url = _extract_dashboard_url(collected)
    if not url:
        raise RuntimeError('未从日志中解析到 Dashboard URL。')

    on_line('Waiting dashboard HTTP ready...')
    _wait_http_ready(url, on_line)

    webbrowser.open(url)
    on_line('Browser opened.')


class TaskRunner:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._busy = False

    def busy(self) -> bool:
        with self._lock:
            return self._busy

    def _set_busy(self, v: bool) -> None:
        with self._lock:
            self._busy = v

    def start_async(self, fn) -> bool:
        if self.busy():
            return False

        self._set_busy(True)

        def runner():
            try:
                fn()
            except Exception as e:
                LOG.write(str(e))
            finally:
                self._set_busy(False)

        threading.Thread(target=runner, daemon=True).start()
        return True


TASKS = TaskRunner()


def _html_escape(s: str) -> str:
    return (
        s.replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&#39;')
    )


def _page_index(msg: str = '') -> bytes:
    cfg = load_config()
    busy = '运行中' if TASKS.busy() else '空闲'
    gateway = f'{GATEWAY_HOST}:{GATEWAY_PORT}'
    has_key = bool((cfg.api_key or '').strip())
    api_state = '已保存' if has_key else '未配置'
    api_open = 'open' if not has_key else ''
    disabled = 'disabled' if TASKS.busy() else ''
    checked_cn = 'selected' if cfg.use_cn_registry else ''
    checked_global = '' if cfg.use_cn_registry else 'selected'
    checked_auto = 'checked' if getattr(cfg, 'auto_install_deps', True) else ''
    models = ''.join([f"<li><code>{_html_escape(m)}</code></li>" for m in DEFAULT_MODELS])

    note = ''
    if msg:
        note = f"<div class='note'><b>{_html_escape(msg)}</b></div>"

    html = (
        "<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'/>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'/>"
        f"<title>{_html_escape(APP_NAME)}</title>"
        "<style>"
        ":root{--bg1:#faf7f0;--bg2:#f2f7ff;--panel:#ffffffcc;--ink:#0f172a;--muted:#475569;--line:#e2e8f0;"
        "--shadow:0 18px 60px rgba(15,23,42,.10);--primary:#0b5cff;--primary2:#0847c6;--danger:#b42318;--danger2:#7a271a;--radius:18px;}"
        "*{box-sizing:border-box;}"
        "body{margin:0;color:var(--ink);font-family:Segoe UI,Arial;background:radial-gradient(1200px 600px at 20% -10%, var(--bg2), transparent),radial-gradient(900px 500px at 110% 10%, #fff1f2, transparent),linear-gradient(180deg, var(--bg1), #ffffff);}"
        ".wrap{max-width:980px;margin:24px auto;padding:0 14px;}"
        "header{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:14px;}"
        ".brand h1{margin:0;font-size:22px;letter-spacing:.2px;}"
        ".brand .sub{margin-top:6px;color:var(--muted);font-size:13px;line-height:1.5;}"
        ".right{display:flex;align-items:center;gap:10px;flex-wrap:wrap;justify-content:flex-end;}"
        ".pill{display:inline-flex;align-items:center;gap:8px;border:1px solid var(--line);background:#fff;border-radius:999px;padding:8px 12px;box-shadow:0 8px 28px rgba(15,23,42,.06);}"
        ".dot{width:9px;height:9px;border-radius:99px;background:#16a34a;}"
        ".dot.busy{background:#f59e0b;}"
        "code{background:#f1f5f9;padding:2px 6px;border-radius:8px;border:1px solid #e2e8f0;}"
        ".panel{border:1px solid var(--line);background:var(--panel);backdrop-filter:blur(6px);border-radius:var(--radius);box-shadow:var(--shadow);padding:16px;}"
        ".note{margin:12px 0;padding:12px 14px;border-radius:14px;border:1px solid #fed7aa;background:#fff7ed;color:#9a3412;}"
        "form{margin:0;}"
        "label{display:block;font-size:13px;color:var(--muted);margin-bottom:8px;}"
        "input,select,button{font:inherit;}"
        "input,select{width:100%;padding:12px 12px;border-radius:14px;border:1px solid #cbd5e1;background:#fff;}"
        "button{border-radius:14px;border:1px solid transparent;padding:12px 14px;cursor:pointer;}"
        "button[disabled]{opacity:.55;cursor:not-allowed;}"
        ".row{display:flex;gap:10px;align-items:end;flex-wrap:wrap;}"
        ".grow{flex:1;min-width:260px;}"
        ".btn-primary{background:linear-gradient(180deg,var(--primary),var(--primary2));color:#fff;}"
        ".btn-ghost{background:#fff;border:1px solid var(--line);color:var(--ink);}"
        ".btn-danger{background:linear-gradient(180deg,var(--danger),var(--danger2));color:#fff;}"
        ".hint{margin-top:8px;color:var(--muted);font-size:12px;line-height:1.5;}"
        "a{color:#1d4ed8;text-decoration:none;}"
        "a:hover{text-decoration:underline;}"
        "details{border:1px solid var(--line);border-radius:16px;background:#fff;padding:10px 12px;}"
        "summary{cursor:pointer;color:var(--ink);font-weight:600;}"
        ".muted{color:var(--muted);font-weight:500;}"
        ".grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;}"
        "@media (max-width:720px){.grid2{grid-template-columns:1fr;}}"
        "</style></head><body>"
        "<div class='wrap'>"
        "<header>"
        "  <div class='brand'>"
        f"    <h1>{_html_escape(APP_NAME)}</h1>"
        "    <div class='sub'>安装与启动入口。首次启动会自动安装必要组件，之后仅作为快捷启动。</div>"
        "  </div>"
        "  <div class='right'>"
        f"    <div class='pill'><span class='dot {'busy' if TASKS.busy() else ''}'></span><b>Gateway</b> <code>{_html_escape(gateway)}</code> <span class='muted'>{busy}</span></div>"
        "    <div class='pill'><a href='/logs'>查看日志</a></div>"
        "  </div>"
        "</header>"
        f"{note}"
        "<div class='panel'>"
        "  <form method='POST' action='/do'>"
        "    <details " + api_open + ">"
        "      <summary>API Key 配置 <span class='muted'>(" + api_state + ")</span></summary>"
        "      <div style='height:10px'></div>"
        "      <div class='row'>"
        "        <div class='grow'>"
        "          <label>百炼 API Key</label>"
        "          <input type='password' name='api_key' placeholder='DASHSCOPE_API_KEY / sk-***' " + disabled + "/>"
        "        </div>"
        "        <button class='btn-ghost' type='submit' name='action' value='save' " + disabled + ">保存 API Key</button>"
        "      </div>"
        "      <div class='hint'>"
        "        <a href='https://bailian.console.aliyun.com/cn-beijing/?spm=5176.29597918.resourceCenter.1.3baa133cDgIhms&tab=model#/api-key' target='_blank' rel='noreferrer'>创建 API Key</a>"
        "      </div>"

        "    </details>"

        "    <div class='row' style='margin-top:14px'>"
        "      <button class='btn-primary' type='submit' name='action' value='start' " + disabled + " style='min-width:160px'>启动</button>"
        "      <button class='btn-ghost' type='submit' name='action' value='update' " + disabled + ">升级</button>"
        "      <button class='btn-danger' type='submit' name='action' value='uninstall' " + disabled + " onclick='return confirm(&quot;确定要卸载 OpenClaw 吗？这会删除启动器安装目录下的 OpenClaw 程序文件。&quot;)' >卸载</button>"
        "    </div>"

        "    <details style='margin-top:14px'>"
        "      <summary>高级设置 <span class='muted'>(默认无需修改)</span></summary>"
        "      <div style='height:10px'></div>"
        "      <div class='grid2'>"
        "        <div>"
        "          <label>更新通道</label>"
        "          <select name='channel' " + disabled + " >"
        "            <option value='stable' " + ("selected" if cfg.channel=='stable' else "") + ">stable</option>"
        "            <option value='beta' " + ("selected" if cfg.channel=='beta' else "") + ">beta</option>"
        "            <option value='dev' " + ("selected" if cfg.channel=='dev' else "") + ">dev</option>"
        "          </select>"
        "        </div>"
        "        <div>"
        "          <label>npm 镜像</label>"
        "          <select name='registry' " + disabled + " >"
        "            <option value='cn' " + checked_cn + ">国内镜像 (npmmirror)</option>"
        "            <option value='global' " + checked_global + ">官方源</option>"
        "          </select>"
        "        </div>"
        "      </div>"
        "      <div style='height:10px'></div>"
        "      <label style='display:flex;gap:10px;align-items:center'>"
        "        <input type='hidden' name='auto_install' value='0'/>"
        "        <input type='checkbox' name='auto_install' value='1' " + checked_auto + " " + disabled + "/>"
        "        自动在线安装依赖 (Node.js, Git)"
        "      </label>"
        "      <div class='hint'>如遇权限问题，可用管理员权限运行启动器或手动安装依赖。</div>"
        "      <div style='height:10px'></div>"
        "      <details>"
        "        <summary>默认模型顺序 <span class='muted'>(主用 -> 备用)</span></summary>"
        "        <ul style='margin:10px 0 0 18px'>"
        f"          {models}"
        "        </ul>"
        "      </details>"
        "    </details>"

        "  </form>"
        "</div>"
        "</div></body></html>"
    )
    return html.encode('utf-8')


def _page_logs() -> bytes:
    lines = LOG.snapshot()
    pre = '\n'.join([_html_escape(x) for x in lines[-800:]])
    busy = '运行中' if TASKS.busy() else '空闲'
    html = (
        "<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'/>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'/>"
        "<meta http-equiv='refresh' content='1'/>"
        f"<title>{_html_escape(APP_NAME)} - 日志</title>"
        "<style>body{font-family:Consolas,Segoe UI,Arial;max-width:980px;margin:24px auto;padding:0 12px;}"
        "pre{background:#0b1020;color:#e6e8ee;padding:12px;border-radius:12px;overflow:auto;white-space:pre-wrap;}"
        "a{color:#2563eb;}"
        "</style></head><body>"
        f"<h2>{_html_escape(APP_NAME)} 日志</h2>"
        f"<p>状态: <b>{busy}</b> <a href='/'>返回</a></p>"
        "<pre>" + pre + "</pre>"
        "</body></html>"
    )
    return html.encode('utf-8')


def _send_html(handler: BaseHTTPRequestHandler, body: bytes, code: int = 200) -> None:
    handler.send_response(code)
    handler.send_header('Content-Type', 'text/html; charset=utf-8')
    handler.send_header('Cache-Control', 'no-store')
    handler.send_header('Content-Length', str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _parse_form(body: bytes) -> dict:
    try:
        s = body.decode('utf-8', errors='replace')
        q = urlparse.parse_qs(s, keep_blank_values=True)
        out = {}
        for k, v in q.items():
            if not v:
                continue
            # If multiple values exist (e.g. hidden + checkbox), keep the last one.
            out[k] = v[-1]
        return out
    except Exception:
        return {}

class Handler(BaseHTTPRequestHandler):
    server_version = 'OpenClawLauncher/1.0'

    def log_message(self, fmt, *args) -> None:
        return

    def do_GET(self) -> None:
        if self.path.startswith('/logs'):
            _send_html(self, _page_logs())
            return

        if self.path == '/' or self.path.startswith('/?'):
            _send_html(self, _page_index())
            return

        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        if self.path != '/do':
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        try:
            n = int(self.headers.get('Content-Length', '0') or '0')
        except Exception:
            n = 0
        raw = self.rfile.read(n) if n > 0 else b''
        form = _parse_form(raw)

        action = (form.get('action') or '').strip().lower()
        cfg = load_config()

        # Persist config (only if fields exist in the form).
        if 'api_key' in form:
            api_key = (form.get('api_key') or '').strip()
            if api_key:
                cfg.api_key = api_key
        if 'channel' in form:
            channel = (form.get('channel') or '').strip()
            if channel in ['stable', 'beta', 'dev']:
                cfg.channel = channel
        if 'registry' in form:
            registry = (form.get('registry') or '').strip()
            if registry in ['cn', 'global']:
                cfg.use_cn_registry = (registry == 'cn')
        if 'auto_install' in form:
            auto_install = (form.get('auto_install') or '').strip()
            cfg.auto_install_deps = auto_install in ['1', 'on', 'true', 'yes']
        save_config(cfg)

        if action == 'save':
            _send_html(self, _page_index('API Key 已保存。'))
            return

        if action == 'update':
            def job():
                cfg2 = load_config()
                LOG.write('=== Upgrade OpenClaw ===')
                cmd = ensure_openclaw_installed(cfg2, on_line=LOG.write, update=True)
                LOG.write(f'OpenClaw ready: {cmd}')

            started = TASKS.start_async(job)
            msg = '已开始升级，请打开日志查看进度。' if started else '正在执行其他任务，请稍后再试。'
            _send_html(self, _page_index(msg))
            return

        if action == 'uninstall':
            def job():
                cfg2 = load_config()
                LOG.write('=== Uninstall OpenClaw ===')
                ensure_openclaw_uninstalled(cfg2, on_line=LOG.write)
                LOG.write('Done.')

            started = TASKS.start_async(job)
            msg = '已开始卸载，请打开日志查看进度。' if started else '正在执行其他任务，请稍后再试。'
            _send_html(self, _page_index(msg))
            return

        if action == 'start':
            def job():
                cfg2 = load_config()
                LOG.write('=== Start OpenClaw (Bailian) ===')
                cmd = ensure_openclaw_installed(cfg2, on_line=LOG.write, update=False)
                ensure_openclaw_setup(cfg2, cmd, LOG.write)
                ensure_bailian_configured(cfg2, cmd, LOG.write)
                ensure_gateway_running(cfg2, cmd, LOG.write)
                open_dashboard(cfg2, cmd, LOG.write)
                LOG.write('Done.')

            started = TASKS.start_async(job)
            msg = '已开始启动，请打开日志查看进度（稍后会自动打开 Dashboard）。' if started else '正在执行其他任务，请稍后再试。'
            _send_html(self, _page_index(msg))
            return

        _send_html(self, _page_index('未知操作。'))


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return int(s.getsockname()[1])


def main() -> int:
    _safe_mkdir(APP_DIR)

    port = _pick_free_port()
    httpd = ThreadingHTTPServer(('127.0.0.1', port), Handler)

    url = f'http://127.0.0.1:{port}/'
    LOG.write(f'{APP_NAME} running: {url}')
    webbrowser.open(url)

    try:
        httpd.serve_forever(poll_interval=0.5)
        return 0
    except KeyboardInterrupt:
        return 0


if __name__ == '__main__':
    raise SystemExit(main())



