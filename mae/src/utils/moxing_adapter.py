# Copyright 2022 Huawei Technologies Co., Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""
Moxing adapter.
"""

import os


_global_sync_count = 0


def get_device_id():
    """Get device id."""
    device_id = os.getenv('DEVICE_ID', '0')
    return int(device_id)


def get_device_num():
    """Get device num."""
    device_num = os.getenv('RANK_SIZE', '1')
    return int(device_num)


def get_rank_id():
    """Get the id of process rank."""
    global_rank_id = os.getenv('RANK_ID', '0')
    return int(global_rank_id)


def get_job_id():
    """Get the id of the job."""
    job_id = os.getenv('JOB_ID')
    job_id = job_id if job_id != "" else "default"
    return job_id


def sync_data(from_path, to_path):
    """
    Synchronize the data.
    If the first url is the remote url and the second url is the local path,
    download the data from the remote obs to the local directory,
    and vice versa upload the data from the local directory to the remote obs.

    Args:
        from_path (str): Download from the path.
        to_path (str): Download to the path.
    """
    import moxing as mox
    import time
    global _global_sync_count
    sync_lock = "/tmp/copy_sync.lock" + str(_global_sync_count)
    _global_sync_count += 1

    # Up to 8 devices per server
    if get_device_id() % min(get_device_num(), 8) == 0 and not os.path.exists(sync_lock):
        mox.file.copy_parallel(from_path, to_path)
        try:
            os.mknod(sync_lock)
        except IOError:
            print("IO Error")
    while True:
        if os.path.exists(sync_lock):
            break
        time.sleep(1)


