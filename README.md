# clouddrive2-client

[![PyPI version](https://img.shields.io/pypi/v/clouddrive2-client)](https://pypi.org/project/clouddrive2-client/)
[![Python version](https://img.shields.io/pypi/pyversions/clouddrive2-client)](https://pypi.org/project/clouddrive2-client/)
[![License](https://img.shields.io/github/license/DDSRem-Dev/clouddrive2-client)](./LICENSE)

CloudDrive2 gRPC Python 客户端库。

## 安装

```bash
pip install clouddrive2-client
```

## 快速开始

```python
from clouddrive2_client import CloudDriveClient

# 连接到 CloudDrive2 服务
client = CloudDriveClient("localhost:19798")

# 认证
client.authenticate("admin", "password")

# 获取系统信息
info = client.get_system_info()

# 列出目录文件
files = client.get_sub_files("/")

# 创建文件夹
client.create_folder("/", "new_folder")

# 获取空间信息
space = client.get_space_info("/")

# 关闭连接
client.close()
```

## License

[MIT](./LICENSE)
