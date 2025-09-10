import json
import time
import argparse
import os
import logging
import random
import concurrent.futures
from threading import Lock

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

from utils import reserve

def get_user_credentials(action):
    """从环境变量中获取用户凭证"""
    if not action:
        return None, None
    usernames = os.environ.get("USERNAMES")
    passwords = os.environ.get("PASSWORDS")
    if not usernames or not passwords:
        logging.error("USERNAMES or PASSWORDS not found in environment variables.")
        return None, None
    return usernames, passwords


get_current_time = lambda action: (
    time.strftime("%H:%M:%S", time.localtime(time.time() + 8 * 3600))
    if action
    else time.strftime("%H:%M:%S", time.localtime(time.time()))
)
get_current_dayofweek = lambda action: (
    time.strftime("%A", time.localtime(time.time() + 8 * 3600))
    if action
    else time.strftime("%A", time.localtime(time.time()))
)

# 🚀 性能优化参数设置
SLEEPTIME = 0.05  # 进一步减少到0.05秒
ENDTIME = "22:01:00"
START_TIME = "22:00:00"

ENABLE_SLIDER = True
MAX_ATTEMPT = 1  # 减少到2次尝试，快速失败
RESERVE_NEXT_DAY = True
MAX_LOOP_ATTEMPTS = 2  # 改为 3，只运行三次
MAX_WORKERS = 1  # 并发线程数

# 🚀 成功率监控（线程安全）
success_rate_monitor = {
    'total_attempts': 0,
    'successful_attempts': 0,
    'start_time': None,
    'lock': Lock()
}

def monitor_success_rate(success):
    """线程安全的成功率监控"""
    with success_rate_monitor['lock']:
        success_rate_monitor['total_attempts'] += 1
        if success:
            success_rate_monitor['successful_attempts'] += 1
        
        if success_rate_monitor['total_attempts'] > 0:
            rate = success_rate_monitor['successful_attempts'] / success_rate_monitor['total_attempts'] * 100
            logging.info(f"📊 Current success rate: {rate:.1f}% ({success_rate_monitor['successful_attempts']}/{success_rate_monitor['total_attempts']})")

def execute_single_task(s, username, task, action, task_id):
    """
    优化后的单任务执行 - 复用已登录的session
    s: 已经登录成功的 reserve 对象
    username: 用户名 (用于日志记录)
    """
    times = task["time"]
    roomid = task["roomid"]
    seatid = task["seatid"]
    
    start_time = time.time()
    logging.info(f"🎯 {username} -- {times} -- {seatid} try (Task {task_id})")
    
    try:
        # 不再需要创建 reserve 对象和登录，直接使用传入的 s 对象
        s.requests.headers.update({"Host": "office.chaoxing.com"})
        success = s.submit(times, roomid, seatid, action)
        
        elapsed = time.time() - start_time
        if success:
            logging.info(f"✅ {username} - {times} - {seatid} SUCCESS in {elapsed:.2f}s (Task {task_id})")
        else:
            logging.info(f"❌ {username} - {times} - {seatid} FAILED in {elapsed:.2f}s (Task {task_id})")
        
        monitor_success_rate(success)
        return success
        
    except Exception as e:
        elapsed = time.time() - start_time
        logging.error(f"💥 Task {task_id} error in {elapsed:.2f}s: {e}")
        monitor_success_rate(False)
        return False

def login_and_reserve_concurrent(users, usernames, passwords, action, success_list=None):
    """
    🔥 优化后的执行逻辑 - 支持并发和会话复用
    """
    logging.info(
        f"🔧 Optimized settings: \nSLEEPTIME: {SLEEPTIME}\nENDTIME: {ENDTIME}\nENABLE_SLIDER: {ENABLE_SLIDER}\nRESERVE_NEXT_DAY: {RESERVE_NEXT_DAY}\nMAX_WORKERS: {MAX_WORKERS}"
    )
    
    if action and usernames and len(usernames.split(",")) != len(users):
        raise Exception("user number should match the number of config")
    
    current_dayofweek = get_current_dayofweek(action)
    
    # 🔥 收集所有需要执行的任务, 并按用户分组
    user_tasks_map = {}
    task_counter = 0

    for index, user in enumerate(users):
        # 提取用户名和密码
        username = user["username"]
        password = user["password"]
        if action and usernames:
            username, password = (
                usernames.split(",")[index],
                passwords.split(",")[index],
            )
        
        if username not in user_tasks_map:
            user_tasks_map[username] = {"password": password, "tasks": []}

        # 提取任务
        tasks = user.get("tasks", [])
        if not tasks and "time" in user: # 兼容旧格式
            tasks = [{
                "time": user["time"],
                "roomid": user["roomid"],
                "seatid": user["seatid"] if isinstance(user["seatid"], list) else [user["seatid"]],
                "daysofweek": user["daysofweek"]
            }]
        
        for task in tasks:
            if current_dayofweek in task["daysofweek"]:
                task_info = {
                    "task": task,
                    "task_id": task_counter,
                }
                user_tasks_map[username]["tasks"].append(task_info)
                task_counter += 1

    total_tasks = task_counter
    if success_list is None:
        success_list = [False] * total_tasks
    
    if total_tasks == 0:
        logging.info("Today not set to reserve")
        return success_list
    
    logging.info(f"🚀 Using {'CONCURRENT' if MAX_WORKERS > 1 else 'SEQUENTIAL'} execution for {total_tasks} tasks with {MAX_WORKERS} workers")
    
    start_execution_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task_id = {}
        
        # 遍历每个用户，登录后提交其所有任务
        for username, user_data in user_tasks_map.items():
            password = user_data["password"]
            tasks_to_run = user_data["tasks"]
            
            if not tasks_to_run:
                continue

            # 为每个用户只登录一次
            logging.info(f"Logging in for user: {username}")
            s = reserve(
                sleep_time=SLEEPTIME,
                max_attempt=MAX_ATTEMPT,
                enable_slider=ENABLE_SLIDER,
                reserve_next_day=RESERVE_NEXT_DAY,
            )
            s.get_login_status()
            login_result = s.login(username, password)

            if not login_result[0]:
                logging.error(f"❌ Login failed for {username}: {login_result[1]}")
                # 将该用户的所有任务标记为失败
                for task_info in tasks_to_run:
                    success_list[task_info["task_id"]] = False
                    monitor_success_rate(False)
                continue # 继续处理下一个用户

            # 登录成功, 提交该用户的所有任务
            for task_info in tasks_to_run:
                task_id = task_info["task_id"]
                if not success_list[task_id]:
                    future = executor.submit(
                        execute_single_task,
                        s, # 传入已登录的session
                        username,
                        task_info["task"],
                        action,
                        task_id
                    )
                    future_to_task_id[future] = task_id
        
        # 等待所有提交的任务完成
        for future in concurrent.futures.as_completed(future_to_task_id):
            task_id = future_to_task_id[future]
            try:
                result = future.result()
                success_list[task_id] = result
            except Exception as e:
                logging.error(f"❌ Concurrent task execution error for task {task_id}: {e}")
                success_list[task_id] = False
                monitor_success_rate(False)

    execution_time = time.time() - start_execution_time
    logging.info(f"⚡ Execution completed in {execution_time:.2f}s")
    
    return success_list


def main(users, action=False):
    # 🚀 初始化成功率监控
    success_rate_monitor['start_time'] = time.time()
    
    current_time = get_current_time(action)
    logging.info(f"🚀 Optimized program started at {current_time}, action {'on' if action else 'off'}")
    
    # 🚀 精确等待启动时间
    while current_time < START_TIME:
        try:
            start_time_obj = time.strptime(START_TIME, "%H:%M:%S")
            current_time_obj = time.strptime(current_time, "%H:%M:%S")

            # Convert to seconds from midnight
            start_seconds = start_time_obj.tm_hour * 3600 + start_time_obj.tm_min * 60 + start_time_obj.tm_sec
            current_seconds = current_time_obj.tm_hour * 3600 + current_time_obj.tm_min * 60 + current_time_obj.tm_sec

            remaining_seconds = start_seconds - current_seconds
            
            if remaining_seconds > 1:
                logging.info(f"Waiting for START_TIME ({START_TIME})... Current time: {current_time}")
                time.sleep(1) 
            else:
                time.sleep(0.001)
        except ValueError:
            logging.error("Invalid time format in START_TIME. Please use HH:MM:SS.")
            return

        current_time = get_current_time(action)
    
    logging.info(f"🎯 Start time reached! Beginning HIGH-SPEED reservation process at {current_time}")
    
    attempt_times = 0
    
    usernames, passwords = None, None
    if action:
        usernames, passwords = get_user_credentials(action)
    success_list = None
    current_dayofweek = get_current_dayofweek(action)
    
    # 计算今天需要执行的任务总数
    today_reservation_num = 0
    for user in users:
        tasks = user.get("tasks", [])
        if not tasks and "time" in user: # 兼容旧格式
             tasks = [user]
        
        today_reservation_num += sum(
            1 for task in tasks if current_dayofweek in task.get("daysofweek", [])
        )
    
    if today_reservation_num == 0:
        logging.info("Today not set to reserve, exiting...")
        return
    
    logging.info(f"📋 Total tasks to complete today: {today_reservation_num}")
    
    # 修改循环条件：只执行 MAX_LOOP_ATTEMPTS 次，不管成功失败
    while attempt_times < MAX_LOOP_ATTEMPTS:
        attempt_times += 1
        attempt_start_time = time.time()
        logging.info(f"🔄 Starting HIGH-SPEED attempt {attempt_times}")
        
        try:
            success_list = login_and_reserve_concurrent(
                users, usernames, passwords, action, success_list
            )
        except Exception as e:
            logging.error(f"💥 An error occurred: {e}")
            current_time = get_current_time(action)
            logging.info(
                f"attempt time {attempt_times}, time now {current_time}, "
                f"success list {success_list}"
            )
            continue
        
        current_time = get_current_time(action)
        successful_tasks = sum(success_list) if success_list else 0
        attempt_duration = time.time() - attempt_start_time
        
        logging.info(
            f"⚡ Attempt {attempt_times} completed in {attempt_duration:.2f}s, time now {current_time}, "
            f"success list {success_list} ({successful_tasks}/{today_reservation_num} tasks completed)"
        )
        
        # 检查是否全部预约成功
        if success_list and successful_tasks == today_reservation_num:
            total_duration = time.time() - success_rate_monitor['start_time']
            logging.info(f"🎉 ALL RESERVATIONS COMPLETED SUCCESSFULLY in {total_duration:.2f}s!")
            # 即使全部成功，也要继续执行剩余的尝试次数
        
        # 检查本轮是否有任何成功的预约
        if success_list and successful_tasks > 0:
            logging.info(f"✅ Some reservations succeeded, continuing...")
        else:
            logging.warning(f"❌ No reservations succeeded in this attempt.")
        
        # 🚀 优化：更短的休息时间
        if attempt_times < MAX_LOOP_ATTEMPTS:  # 只有不是最后一次才休息
            time.sleep(0.1)
    
    # 最终状态报告
    total_duration = time.time() - success_rate_monitor['start_time']
    
    logging.info(f"⏰ Completed all {MAX_LOOP_ATTEMPTS} attempts.")
    
    final_success_count = sum(success_list) if success_list else 0
    final_success_rate = (success_rate_monitor['successful_attempts'] / 
                         max(success_rate_monitor['total_attempts'], 1) * 100)
    
    if final_success_count > 0:
        logging.info(f"✅ Final result: {final_success_count}/{today_reservation_num} reservations completed successfully in {total_duration:.2f}s!")
    else:
        logging.info(f"❌ Final result: No reservations were successful in {total_duration:.2f}s.")
    
    logging.info(f"📊 Overall success rate: {final_success_rate:.1f}% ({success_rate_monitor['successful_attempts']}/{success_rate_monitor['total_attempts']})")

def debug(users, action=False):
    """
    调试模式，立即执行一次预约任务，忽略 START_TIME
    """
    logging.info("🐛 Running in DEBUG mode. Bypassing start time check and executing one attempt.")
    usernames, passwords = get_user_credentials(action)

    try:
        success_list = login_and_reserve_concurrent(
            users, usernames, passwords, action
        )
        successful_tasks = sum(success_list) if success_list else 0
        logging.info(f"🐛 DEBUG run finished. Success list: {success_list}. Successful tasks: {successful_tasks}")
    except Exception as e:
        logging.error(f"💥 An error occurred during DEBUG run: {e}", exc_info=True)


def get_roomid(args1, args2):
    """获取指定教学楼的房间ID"""
    s = reserve()
    username, password = get_user_credentials(args2)
    if not username or not password:
        with open(args1, "r+") as data:
            usersdata = json.load(data)["reserve"]
            username, password = usersdata[0]['username'], usersdata[0]['password']
    
    s.get_login_status()
    s.login(username, password)
    encode = input("Please input the encode of the building: ")
    s.roomid(encode)


if __name__ == "__main__":
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    parser = argparse.ArgumentParser(prog="Chao Xing seat auto reserve - HIGH-SPEED Concurrent Version")
    parser.add_argument("-u", "--user", default=config_path, help="user config file")
    parser.add_argument(
        "-m",
        "--method",
        default="reserve",
        choices=["reserve", "debug", "room"],
        help="for debug",
    )
    parser.add_argument(
        "-a",
        "--action",
        action="store_true",
        help="use --action to enable in github action",
    )
    args = parser.parse_args()
    func_dict = {"reserve": main, "debug": debug, "room": get_roomid}
    
    try:
        with open(args.user, "r", encoding="utf-8") as data:
            usersdata = json.load(data)["reserve"]
    except FileNotFoundError:
        logging.error(f"Config file not found at: {args.user}")
        exit(1)
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from the config file: {args.user}")
        exit(1)

    
    logging.info("🚀 Starting HIGH-SPEED concurrent seat reservation system...")
    func_dict[args.method](usersdata, args.action)
