from collections import deque
from typing import Any, Iterator, List, Optional, Tuple

from grpc import insecure_channel

from .proto import clouddrive_pb2
from .proto import clouddrive_pb2_grpc
from google.protobuf import empty_pb2


class CloudDriveClient:
    """
    CloudDrive gRPC 客户端
    """

    def __init__(self, address: str) -> None:
        """
        初始化 CloudDrive 客户端。

        :param address: 服务器地址
        """
        self.channel = insecure_channel(address)
        self.stub = clouddrive_pb2_grpc.CloudDriveFileSrvStub(self.channel)
        self.jwt_token: Optional[str] = None

    def close(self) -> None:
        """
        关闭 gRPC 通道
        """
        self.channel.close()

    def authenticate(self, username: str, password: str) -> bool:
        """
        认证并获取 JWT 令牌。

        :param username: 用户名
        :param password: 密码
        :return: 认证成功返回 True，否则 False
        """
        request = clouddrive_pb2.GetTokenRequest(userName=username, password=password)
        response = self.stub.GetToken(request)
        if response.success:
            self.jwt_token = response.token
            return True
        return False

    def _create_authorized_metadata(self) -> List[tuple]:
        """
        创建带 Bearer 的元数据，用于需认证的 RPC。
        """
        if not self.jwt_token:
            return []
        return [("authorization", f"Bearer {self.jwt_token}")]

    def get_system_info(self):
        """
        获取系统信息（无需认证）。
        """
        return self.stub.GetSystemInfo(empty_pb2.Empty())

    def get_sub_files(self, path: str, force_refresh: bool = False) -> Iterator:
        """
        列出目录中的文件。

        :param path: 目录路径
        :param force_refresh: 是否强制刷新缓存
        :yield: CloudDriveFile
        """
        request = clouddrive_pb2.ListSubFileRequest(
            path=path, forceRefresh=force_refresh
        )
        metadata = self._create_authorized_metadata()
        for response in self.stub.GetSubFiles(request, metadata=metadata):
            for f in response.subFiles:
                yield f

    def get_search_results(
        self,
        path: str,
        search_for: str,
        force_refresh: bool = False,
        fuzzy_match: bool = False,
    ) -> Iterator:
        """
        在指定路径下搜索文件或目录。

        :param path: 搜索根路径
        :param search_for: 搜索关键词
        :param force_refresh: 是否强制刷新缓存
        :param fuzzy_match: 是否模糊匹配
        :yield: CloudDriveFile
        """
        request = clouddrive_pb2.SearchRequest(
            path=path,
            searchFor=search_for,
            forceRefresh=force_refresh,
            fuzzyMatch=fuzzy_match,
        )
        metadata = self._create_authorized_metadata()
        for response in self.stub.GetSearchResults(request, metadata=metadata):
            for f in response.subFiles:
                yield f

    def walk(
        self,
        top: str,
        min_depth: int = 0,
        max_depth: int = -1,
        refresh: bool = False,
    ) -> Iterator[Tuple[str, Any]]:
        """
        BFS 遍历目录树

        :param top: 根路径（目录）
        :param min_depth: 最小深度，达到该深度后才可能 yield（0=含根）
        :param max_depth: 最大深度，-1 表示不限制
        :param refresh: 是否强制刷新目录缓存（传给 get_sub_files）
        :yield: (path, CloudDriveFile)，path 为该条目的完整路径
        """
        dq: deque = deque()
        push, pop = dq.append, dq.popleft
        top_path = (top or "/").rstrip("/") or "/"
        path = self.find_file_by_path(top_path)
        if not path or not getattr(path, "fullPathName", None):
            return
        push((0, path))
        while dq:
            depth, path = pop()
            path_str = path.fullPathName or top_path
            if min_depth <= 0:
                yield (path_str, path)
                min_depth = 1
            if depth == 0 and (
                not getattr(path, "isDirectory", False) or (0 <= max_depth <= depth)
            ):
                return
            depth += 1
            dir_path = path_str if path_str.endswith("/") else (path_str + "/")
            for child in self.get_sub_files(dir_path, force_refresh=refresh):
                child_path = child.fullPathName or ""
                if depth >= min_depth:
                    yield (child_path, child)
                if getattr(child, "isDirectory", False) and (
                    max_depth < 0 or depth < max_depth
                ):
                    push((depth, child))

    def find_file_by_path(self, path: str):
        """
        按完整路径查找文件或目录。

        :param path: 完整路径
        :return: CloudDriveFile 或空响应，不存在时可能抛异常或返回空
        """
        request = clouddrive_pb2.FindFileByPathRequest(parentPath="", path=path)
        metadata = self._create_authorized_metadata()
        return self.stub.FindFileByPath(request, metadata=metadata)

    def get_space_info(self, path: str = "/"):
        """
        获取指定路径下的空间信息（总/已用/可用）。

        :param path: 路径，通常为根或挂载点
        :return: SpaceInfo (totalSpace, usedSpace, freeSpace)
        """
        request = clouddrive_pb2.FileRequest(path=path)
        metadata = self._create_authorized_metadata()
        return self.stub.GetSpaceInfo(request, metadata=metadata)

    def get_file_detail_properties(self, path: str):
        """
        获取文件或目录的详细属性。

        :param path: 文件或目录路径
        :return: FileDetailProperties
        """
        request = clouddrive_pb2.FileRequest(path=path)
        metadata = self._create_authorized_metadata()
        return self.stub.GetFileDetailProperties(request, metadata=metadata)

    def get_cloud_memberships(self, path: str = "/"):
        """
        获取指定路径对应云盘的成员/权限信息。

        :param path: 路径，通常为根或挂载点
        :return: CloudMemberships
        """
        request = clouddrive_pb2.FileRequest(path=path)
        metadata = self._create_authorized_metadata()
        return self.stub.GetCloudMemberships(request, metadata=metadata)

    def create_folder(self, parent_path: str, folder_name: str):
        """
        在父目录下创建文件夹。

        :param parent_path: 父目录路径
        :param folder_name: 新文件夹名称
        :return: CreateFolderResult
        """
        request = clouddrive_pb2.CreateFolderRequest(
            parentPath=parent_path, folderName=folder_name
        )
        metadata = self._create_authorized_metadata()
        return self.stub.CreateFolder(request, metadata=metadata)

    def create_encrypted_folder(
        self,
        parent_path: str,
        folder_name: str,
        password: str,
        save_password: bool = True,
    ):
        """
        在父目录下创建加密文件夹。

        :param parent_path: 父目录路径
        :param folder_name: 新文件夹名称
        :param password: 加密密码
        :param save_password: 是否保存密码到本地（否则重启后需重新解锁）
        :return: CreateFolderResult
        """
        request = clouddrive_pb2.CreateEncryptedFolderRequest(
            parentPath=parent_path,
            folderName=folder_name,
            password=password,
            savePassword=save_password,
        )
        metadata = self._create_authorized_metadata()
        return self.stub.CreateEncryptedFolder(request, metadata=metadata)

    def unlock_encrypted_file(
        self, path: str, password: str, permanent_unlock: bool = False
    ):
        """
        解锁加密文件或文件夹。

        :param path: 加密文件或文件夹路径
        :param password: 密码
        :param permanent_unlock: 是否永久解锁（保存密码，重启后无需再输）
        :return: FileOperationResult
        """
        request = clouddrive_pb2.UnlockEncryptedFileRequest(
            path=path, password=password, permanentUnlock=permanent_unlock
        )
        metadata = self._create_authorized_metadata()
        return self.stub.UnlockEncryptedFile(request, metadata=metadata)

    def lock_encrypted_file(self, path: str):
        """
        锁定加密文件或文件夹（清除已保存的密码）。

        :param path: 加密文件或文件夹路径
        :return: FileOperationResult
        """
        request = clouddrive_pb2.FileRequest(path=path)
        metadata = self._create_authorized_metadata()
        return self.stub.LockEncryptedFile(request, metadata=metadata)

    def delete_file(self, file_path: str):
        """
        删除文件或文件夹（放入回收站，若云盘支持）。

        :param file_path: 文件或文件夹路径
        :return: FileOperationResult
        """
        request = clouddrive_pb2.FileRequest(path=file_path)
        metadata = self._create_authorized_metadata()
        return self.stub.DeleteFile(request, metadata=metadata)

    def delete_file_permanently(self, file_path: str):
        """
        永久删除文件或文件夹（不经过回收站）。

        :param file_path: 文件或文件夹路径
        :return: FileOperationResult
        """
        request = clouddrive_pb2.FileRequest(path=file_path)
        metadata = self._create_authorized_metadata()
        return self.stub.DeleteFilePermanently(request, metadata=metadata)

    def delete_files(self, paths: List[str]):
        """
        批量删除文件或文件夹（放入回收站）。

        :param paths: 路径列表
        :return: FileOperationResult
        """
        request = clouddrive_pb2.MultiFileRequest(path=paths)
        metadata = self._create_authorized_metadata()
        return self.stub.DeleteFiles(request, metadata=metadata)

    def delete_files_permanently(self, paths: List[str]):
        """
        批量永久删除文件或文件夹。

        :param paths: 路径列表
        :return: FileOperationResult
        """
        request = clouddrive_pb2.MultiFileRequest(path=paths)
        metadata = self._create_authorized_metadata()
        return self.stub.DeleteFilesPermanently(request, metadata=metadata)

    def rename_file(self, file_path: str, new_name: str):
        """
        重命名文件或目录。

        :param file_path: 当前路径
        :param new_name: 新名称
        :return: FileOperationResult
        """
        request = clouddrive_pb2.RenameFileRequest(
            theFilePath=file_path, newName=new_name
        )
        metadata = self._create_authorized_metadata()
        return self.stub.RenameFile(request, metadata=metadata)

    def rename_files(self, renames: List[Tuple[str, str]]):
        """
        批量重命名文件或目录。

        :param renames: [(当前路径, 新名称), ...]
        :return: FileOperationResult
        """
        request = clouddrive_pb2.RenameFilesRequest(
            renameFiles=[
                clouddrive_pb2.RenameFileRequest(theFilePath=p, newName=n)
                for p, n in renames
            ]
        )
        metadata = self._create_authorized_metadata()
        return self.stub.RenameFiles(request, metadata=metadata)

    def move_file(self, the_file_paths: List[str], dest_path: str):
        """
        移动文件或目录到目标路径。

        :param the_file_paths: 要移动的路径列表（云盘内路径）
        :param dest_path: 目标目录路径
        :return: FileOperationResult
        """
        request = clouddrive_pb2.MoveFileRequest(
            theFilePaths=the_file_paths, destPath=dest_path
        )
        metadata = self._create_authorized_metadata()
        return self.stub.MoveFile(request, metadata=metadata)

    def copy_file(self, the_file_paths: List[str], dest_path: str):
        """
        复制文件或目录到目标路径。

        :param the_file_paths: 要复制的路径列表（云盘内路径）
        :param dest_path: 目标目录路径
        :return: FileOperationResult
        """
        request = clouddrive_pb2.CopyFileRequest(
            theFilePaths=the_file_paths, destPath=dest_path
        )
        metadata = self._create_authorized_metadata()
        return self.stub.CopyFile(request, metadata=metadata)

    def get_download_url(
        self,
        path: str,
        preview: bool = False,
        lazy_read: bool = False,
        get_direct_url: bool = True,
    ):
        """
        获取文件下载 URL。

        :param path: 文件路径
        :param preview: 是否预览
        :param lazy_read: 是否延迟读取
        :param get_direct_url: 是否尝试获取直链
        :return: DownloadUrlPathInfo
        """
        request = clouddrive_pb2.GetDownloadUrlPathRequest(
            path=path,
            preview=preview,
            lazy_read=lazy_read,
            get_direct_url=get_direct_url,
        )
        metadata = self._create_authorized_metadata()
        return self.stub.GetDownloadUrlPath(request, metadata=metadata)

    def start_remote_upload(
        self,
        file_path: str,
        file_size: int,
        known_hashes: Optional[dict] = None,
        client_can_calculate_hashes: bool = True,
    ):
        """
        启动远程上传会话。

        :param file_path: 云端目标路径（含文件名）
        :param file_size: 文件大小（字节）
        :param known_hashes: 已知哈希，key 为 HashType (1=Md5, 2=Sha1)，value 为十六进制字符串
        :param client_can_calculate_hashes: 客户端可本地计算哈希
        :return: RemoteUploadStarted，含 upload_id
        """
        request = clouddrive_pb2.StartRemoteUploadRequest(
            file_path=file_path,
            file_size=file_size,
            known_hashes=known_hashes or {},
            client_can_calculate_hashes=client_can_calculate_hashes,
        )
        metadata = self._create_authorized_metadata()
        return self.stub.StartRemoteUpload(request, metadata=metadata)

    def remote_upload_channel(self, device_id: str = "moviepilot"):
        """
        打开远程上传通道，服务端通过流下发 read_data / hash_data / status_changed。

        :param device_id: 设备标识
        :return: 流式迭代器，产出 RemoteUploadChannelReply
        """
        request = clouddrive_pb2.RemoteUploadChannelRequest(device_id=device_id)
        metadata = self._create_authorized_metadata()
        return self.stub.RemoteUploadChannel(request, metadata=metadata)

    def remote_read_data(
        self,
        upload_id: str,
        offset: int,
        length: int,
        data: bytes,
        is_last_chunk: bool,
        lazy_read: bool = False,
    ):
        """
        响应服务端的读数据请求，上传文件块。

        :param upload_id: 上传会话 ID
        :param offset: 偏移量
        :param length: 请求长度
        :param data: 文件块数据
        :param is_last_chunk: 是否为最后一块
        :param lazy_read: 是否延迟读取
        :return: RemoteReadDataReply (success, error_message, bytes_received, is_last_chunk)
        """
        request = clouddrive_pb2.RemoteReadDataUpload(
            upload_id=upload_id,
            offset=offset,
            length=length,
            lazy_read=lazy_read,
            data=data,
            is_last_chunk=is_last_chunk,
        )
        metadata = self._create_authorized_metadata()
        return self.stub.RemoteReadData(request, metadata=metadata)

    def remote_upload_control_cancel(self, upload_id: str) -> None:
        """
        取消远程上传。

        :param upload_id: 上传ID
        """
        request = clouddrive_pb2.RemoteUploadControlRequest(
            upload_id=upload_id,
            cancel=clouddrive_pb2.CancelRemoteUpload(),
        )
        metadata = self._create_authorized_metadata()
        self.stub.RemoteUploadControl(request, metadata=metadata)

    def remote_hash_progress(
        self,
        upload_id: str,
        bytes_hashed: int,
        total_bytes: int,
        hash_type: int,
        hash_value: Optional[str] = None,
        block_hashes: Optional[list] = None,
    ):
        """
        上报本地计算的哈希进度或结果。

        :param upload_id: 上传会话 ID
        :param bytes_hashed: 已哈希字节数
        :param total_bytes: 总字节数
        :param hash_type: CloudDriveFile.HashType (1=Md5, 2=Sha1)
        :param hash_value: 最终哈希值（十六进制字符串），可选
        :param block_hashes: 分块 MD5 等，可选
        """
        request = clouddrive_pb2.RemoteHashProgressUpload(
            upload_id=upload_id,
            bytes_hashed=bytes_hashed,
            total_bytes=total_bytes,
            hash_type=hash_type,
            hash_value=hash_value or "",
            block_hashes=block_hashes or [],
        )
        metadata = self._create_authorized_metadata()
        return self.stub.RemoteHashProgress(request, metadata=metadata)

    def get_all_tasks_count(self):
        """
        获取所有任务数量（下载、上传、复制等）。

        :return: GetAllTasksCountResult
        """
        metadata = self._create_authorized_metadata()
        return self.stub.GetAllTasksCount(empty_pb2.Empty(), metadata=metadata)

    def get_download_file_count(self):
        """
        获取当前下载任务数量。

        :return: GetDownloadFileCountResult
        """
        metadata = self._create_authorized_metadata()
        return self.stub.GetDownloadFileCount(empty_pb2.Empty(), metadata=metadata)

    def get_download_file_list(self):
        """
        获取下载任务列表。

        :return: GetDownloadFileListResult
        """
        metadata = self._create_authorized_metadata()
        return self.stub.GetDownloadFileList(empty_pb2.Empty(), metadata=metadata)

    def get_upload_file_count(self):
        """
        获取当前上传任务数量。

        :return: GetUploadFileCountResult
        """
        metadata = self._create_authorized_metadata()
        return self.stub.GetUploadFileCount(empty_pb2.Empty(), metadata=metadata)

    def get_upload_file_list(
        self,
        get_all: bool = True,
        items_per_page: int = 0,
        page_number: int = 0,
    ):
        """
        获取上传任务列表。

        :param get_all: 是否获取全部（为 True 时忽略分页）
        :param items_per_page: 每页条数
        :param page_number: 页码
        :return: GetUploadFileListResult
        """
        request = clouddrive_pb2.GetUploadFileListRequest(
            getAll=get_all,
            itemsPerPage=items_per_page,
            pageNumber=page_number,
        )
        metadata = self._create_authorized_metadata()
        return self.stub.GetUploadFileList(request, metadata=metadata)

    def cancel_all_upload_files(self) -> None:
        """
        取消所有上传任务。
        """
        metadata = self._create_authorized_metadata()
        self.stub.CancelAllUploadFiles(empty_pb2.Empty(), metadata=metadata)

    def cancel_upload_files(self, keys: List[str]) -> None:
        """
        取消指定 key 的上传任务。

        :param keys: 上传任务 key 列表（来自 get_upload_file_list）
        """
        request = clouddrive_pb2.MultpleUploadFileKeyRequest(keys=keys)
        metadata = self._create_authorized_metadata()
        self.stub.CancelUploadFiles(request, metadata=metadata)

    def pause_all_upload_files(self) -> None:
        """
        暂停所有上传任务。
        """
        metadata = self._create_authorized_metadata()
        self.stub.PauseAllUploadFiles(empty_pb2.Empty(), metadata=metadata)

    def pause_upload_files(self, keys: List[str]) -> None:
        """
        暂停指定 key 的上传任务。

        :param keys: 上传任务 key 列表
        """
        request = clouddrive_pb2.MultpleUploadFileKeyRequest(keys=keys)
        metadata = self._create_authorized_metadata()
        self.stub.PauseUploadFiles(request, metadata=metadata)

    def resume_all_upload_files(self) -> None:
        """
        恢复所有上传任务。
        """
        metadata = self._create_authorized_metadata()
        self.stub.ResumeAllUploadFiles(empty_pb2.Empty(), metadata=metadata)

    def resume_upload_files(self, keys: List[str]) -> None:
        """
        恢复指定 key 的上传任务。

        :param keys: 上传任务 key 列表
        """
        request = clouddrive_pb2.MultpleUploadFileKeyRequest(keys=keys)
        metadata = self._create_authorized_metadata()
        self.stub.ResumeUploadFiles(request, metadata=metadata)

    def get_copy_tasks(self):
        """
        获取复制任务列表。

        :return: GetCopyTaskResult
        """
        metadata = self._create_authorized_metadata()
        return self.stub.GetCopyTasks(empty_pb2.Empty(), metadata=metadata)

    def get_merge_tasks(self):
        """
        获取合并任务列表。

        :return: GetMergeTasksResult
        """
        metadata = self._create_authorized_metadata()
        return self.stub.GetMergeTasks(empty_pb2.Empty(), metadata=metadata)

    def cancel_merge_task(self, source_path: str, dest_path: str) -> None:
        """
        取消合并任务。

        :param source_path: 源路径
        :param dest_path: 目标路径
        """
        request = clouddrive_pb2.CancelMergeTaskRequest(
            sourcePath=source_path, destPath=dest_path
        )
        metadata = self._create_authorized_metadata()
        self.stub.CancelMergeTask(request, metadata=metadata)

    def cancel_copy_task(self, source_path: str, dest_path: str) -> None:
        """
        取消复制任务。

        :param source_path: 源路径
        :param dest_path: 目标路径
        """
        request = clouddrive_pb2.CopyTaskRequest(
            sourcePath=source_path, destPath=dest_path
        )
        metadata = self._create_authorized_metadata()
        self.stub.CancelCopyTask(request, metadata=metadata)

    def pause_copy_task(
        self, source_path: str, dest_path: str, pause: bool = True
    ) -> None:
        """
        暂停或恢复单个复制任务。

        :param source_path: 源路径
        :param dest_path: 目标路径
        :param pause: True 为暂停，False 为恢复
        """
        request = clouddrive_pb2.PauseCopyTaskRequest(
            sourcePath=source_path, destPath=dest_path, pause=pause
        )
        metadata = self._create_authorized_metadata()
        self.stub.PauseCopyTask(request, metadata=metadata)

    def restart_copy_task(self, source_path: str, dest_path: str) -> None:
        """
        重启复制任务。

        :param source_path: 源路径
        :param dest_path: 目标路径
        """
        request = clouddrive_pb2.CopyTaskRequest(
            sourcePath=source_path, destPath=dest_path
        )
        metadata = self._create_authorized_metadata()
        self.stub.RestartCopyTask(request, metadata=metadata)

    def remove_completed_copy_tasks(self) -> None:
        """
        移除已完成的复制任务记录。
        """
        metadata = self._create_authorized_metadata()
        self.stub.RemoveCompletedCopyTasks(empty_pb2.Empty(), metadata=metadata)

    def remove_all_copy_tasks(self):
        """
        移除所有复制任务。

        :return: BatchOperationResult
        """
        metadata = self._create_authorized_metadata()
        return self.stub.RemoveAllCopyTasks(empty_pb2.Empty(), metadata=metadata)

    def remove_copy_tasks(self, task_keys: List[str]):
        """
        移除指定 key 的复制任务。

        :param task_keys: 任务 key 列表（来自 get_copy_tasks）
        :return: BatchOperationResult
        """
        request = clouddrive_pb2.CopyTaskBatchRequest(taskKeys=task_keys)
        metadata = self._create_authorized_metadata()
        return self.stub.RemoveCopyTasks(request, metadata=metadata)

    def pause_all_copy_tasks(self, pause: bool = True):
        """
        暂停或恢复所有复制任务。

        :param pause: True 为暂停，False 为恢复
        :return: BatchOperationResult
        """
        request = clouddrive_pb2.PauseAllCopyTasksRequest(pause=pause)
        metadata = self._create_authorized_metadata()
        return self.stub.PauseAllCopyTasks(request, metadata=metadata)

    def pause_copy_tasks(self, task_keys: List[str], pause: bool = True):
        """
        暂停或恢复指定复制任务。

        :param task_keys: 任务 key 列表
        :param pause: True 为暂停，False 为恢复
        :return: BatchOperationResult
        """
        request = clouddrive_pb2.PauseCopyTasksRequest(taskKeys=task_keys, pause=pause)
        metadata = self._create_authorized_metadata()
        return self.stub.PauseCopyTasks(request, metadata=metadata)

    def resume_all_copy_tasks(self):
        """
        恢复所有复制任务。

        :return: BatchOperationResult
        """
        metadata = self._create_authorized_metadata()
        return self.stub.ResumeAllCopyTasks(empty_pb2.Empty(), metadata=metadata)

    def resume_copy_tasks(self, task_keys: List[str]):
        """
        恢复指定复制任务。

        :param task_keys: 任务 key 列表
        :return: BatchOperationResult
        """
        request = clouddrive_pb2.CopyTaskBatchRequest(taskKeys=task_keys)
        metadata = self._create_authorized_metadata()
        return self.stub.ResumeCopyTasks(request, metadata=metadata)
