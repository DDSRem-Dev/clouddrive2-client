#!/usr/bin/env python3
"""
CloudDrive2 Webhook 接收端：接收文件系统变更与挂载点变更的 POST 请求。

与配置中的 URL 对应：
  - POST /file_notify?device_name=...&user_name=...   → 文件变更 (create/delete/rename)
  - POST /mount_notify?device_name=...&user_name=...&type=... → 挂载点变更 (mount/unmount)

启动：在项目根目录执行  python example/webhook_server.py [--port 8080]
"""

"""
# global variables
# {device_name} - The name of the device
# {user_name} - Current user name
# {version} - The version of the application

# global parameters
[global_params]
# The base URL of the server to send the data to, the server must be able to receive POST requests
base_url = "http://127.0.0.1:8080"
# Whether the webhook is enabled
enabled = true
# Datetime output format for all time variables (event_time, send_time). If not set, default is epoch seconds.
# Supported examples:
#   time_format = "epoch"                       # default
#   time_format = "utc:%Y-%m-%d %H:%M:%S"       # custom UTC format
#   time_format = "local:%Y-%m-%d %H:%M:%S"     # custom Local time format
#   time_format = "rfc3339"                     # UTC RFC3339
#   time_format = "rfc3339_local"               # Local RFC3339
# time_format = "epoch"

# The default http headers
[global_params.default_headers]
content-type = "application/json"
user-agent = "clouddrive2/{version}"
authorization = "basic usernamepassword"

# File system watcher webhook configuration
[file_system_watcher]
# The URL of the server to send the data to, the server must be able to receive POST requests
url = "{base_url}/file_notify?device_name={device_name}&user_name={user_name}"
# http method, can be "GET" or "POST", if not specified, the default value is POST
method = "POST"
# Whether the file system watcher is enabled
enabled = true
# The body to be sent to the server can be a JSON string
# The following is a sample template for the data to be sent
# the json string should contain at least an array of file changes, each file change item shoud contain the following fields
# {event_category} - The category of the event is "file"
# {event_name} - The name of the event is "notify"
# {action} - The action of file change (create, delete, rename)
# {is_dir} - Whether the file is a directory (false: file, true: directory)
# {source_file} - The source file's path of the action
# {destination_file} - The destination file's path of the action, only valid for action rename(move)
body = '''
{
    "device_name": "{device_name}",
    "user_name": "{user_name}",
    "version": "{version}",
    "event_category": "{event_category}",
    "event_name": "{event_name}",
    "event_time": "{event_time}",
    "send_time": "{send_time}",
    "data": [
            {
                "action": "{action}",
                "is_dir": "{is_dir}",
                "source_file": "{source_file}",
                "destination_file": "{destination_file}"
            }
    ]
}
'''
# specify additional http headers for the file system watcher request if required, headers with same name will override in the default headers
[file_system_watcher.headers]
additional_header = "value"

# mount point watcher configuration
[mount_point_watcher]
# The URL of the server to send the data to, the server must be able to receive POST requests
url = "{base_url}/mount_notify?device_name={device_name}&user_name={user_name}&type={event_name}"
# http method
method = "POST"
# Whether the mount point watcher is enabled
enabled = true
# The body to be sent to the server can be a JSON string
# The following is a sample template for the data to be sent
# the mount_point_watcher should contain at least an array of mount point changes, each mount point change item shoud contain the following fields
# {event_category} - The category of the event is "mount_point"
# {event_name} - The name of the event is "mount" or "unmount"
# {action} - The action of mount point change (mount, unmount)
# {mount_point} - The mount point's path of the action
# {status} - The status of the action (true for success, false for failed)
# {reason} - The failed reason, empty string means success
body = '''
{
    "device_name": "{device_name}",
    "user_name": "{user_name}",
    "version": "{version}",
    "event_category": "{event_category}",
    "event_name": "{event_name}",
    "event_time": "{event_time}",
    "send_time": "{send_time}",
    "data": [
            {
                "action": "{action}",
                "mount_point": "{mount_point}",
                "status": "{status}",
                "reason": "{reason}"
            }
    ]
}
'''
# specify additional http headers for the file system watcher request if required, headers with same name will override in the default headers
[mount_point_watcher.headers]
user-agent = "clouddrive2/{version}/mount"
"""


import json
import argparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length) if content_length else b""
            data = json.loads(body.decode("utf-8")) if body else {}
        except (ValueError, json.JSONDecodeError) as e:
            self._send(400, {"error": str(e)})
            return

        if path == "/file_notify":
            self._log_file_notify(query, data)
        elif path == "/mount_notify":
            self._log_mount_notify(query, data)
        else:
            self._send(404, {"error": "path not found"})
            return

        self._send(200, {"ok": True})

    def _log_file_notify(self, query: dict, data: dict):
        device = query.get("device_name", [""])[0]
        user = query.get("user_name", [""])[0]
        print(f"[file_notify] device={device!r} user={user!r}")
        for item in data.get("data", []):
            action = item.get("action", "")
            is_dir = item.get("is_dir", False)
            src = item.get("source_file", "")
            dst = item.get("destination_file", "")
            print(f"  - action={action!r} is_dir={is_dir} source={src!r} dest={dst!r}")
        if not data.get("data"):
            print("  (no items)")
        print()

    def _log_mount_notify(self, query: dict, data: dict):
        device = query.get("device_name", [""])[0]
        user = query.get("user_name", [""])[0]
        event_type = query.get("type", [""])[0]
        print(f"[mount_notify] device={device!r} user={user!r} type={event_type!r}")
        for item in data.get("data", []):
            action = item.get("action", "")
            mount_point = item.get("mount_point", "")
            status = item.get("status", False)
            reason = item.get("reason", "")
            print(
                f"  - action={action!r} mount_point={mount_point!r} status={status} reason={reason!r}"
            )
        if not data.get("data"):
            print("  (no items)")
        print()

    def _send(self, status: int, body: dict):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode("utf-8"))

    def log_message(self, format, *args):
        print(f"[HTTP] {args[0]}")


def main():
    parser = argparse.ArgumentParser(description="CloudDrive2 Webhook 接收端")
    parser.add_argument("--port", type=int, default=8080, help="监听端口 (默认 8080)")
    parser.add_argument("--host", default="", help="监听地址 (默认 '' 即所有接口)")
    args = parser.parse_args()

    server = HTTPServer((args.host, args.port), WebhookHandler)
    print(f"Webhook 接收端: http://127.0.0.1:{args.port}")
    print("  POST /file_notify  - 文件系统变更")
    print("  POST /mount_notify - 挂载点变更")
    print("Ctrl+C 退出\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已退出")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
