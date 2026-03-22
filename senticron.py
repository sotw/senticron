import asyncio
import re
import psutil
import time
from datetime import datetime, timedelta
from typing import List, Dict
import logging
import shlex
import subprocess
import sys
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CronJob:
    def __init__(self, minute: str, hour: str, day: str, month: str, weekday: str, 
                 command: str, priority: int, redirect_output: bool):
        self.minute = minute
        self.hour = hour
        self.day = day
        self.month = month
        self.weekday = weekday
        self.command = command
        self.priority = priority
        self.redirect_output = redirect_output
        
    def __str__(self):
        redirect_str = " (redirect)" if self.redirect_output else ""
        return f"CronJob('{self.minute} {self.hour} {self.day} {self.month} {self.weekday}' -> {self.command}, priority={self.priority}{redirect_str})"

class CPUMonitor:
    def __init__(self, sample_interval: float = 10.0):
        self.sample_interval = sample_interval
        self.cpu_usage = 0.0
        self.running = False

    async def start_monitoring(self):
        """Start CPU monitoring in background"""
        logger.info("Starting CPU monitor")
        self.running = True
        counter = 0
        while self.running:
            try:
                self.cpu_usage = psutil.cpu_percent(interval=0.1)
                counter += 1
                if counter % 300 == 0:  # Log every 30 seconds (300 * 0.1s)
                    logger.info(f"CPU usage: {self.cpu_usage:.1f}%")
                await asyncio.sleep(self.sample_interval - 0.1)
            except KeyboardInterrupt:
                logger.info("CPU monitor interrupted")
                break
            except Exception as e:
                logger.error(f"Error monitoring CPU: {e}")
                await asyncio.sleep(self.sample_interval)

    def stop_monitoring(self):
        """Stop CPU monitoring"""
        logger.info("Stopping CPU monitor")
        self.running = False
    
    def is_cpu_low(self, threshold: float = 20.0) -> bool:
        """Check if CPU usage is below threshold"""
        logger.debug(f"Checking CPU usage: {self.cpu_usage:.1f}% against threshold {threshold}%")
        print(f"CPU usage: {self.cpu_usage:.1f}%")
        return self.cpu_usage < threshold

class PriorityQueueManager:
    def __init__(self):
        self.priority_queues: Dict[int, asyncio.Queue] = {}
        for priority in range(1, 11):
            self.priority_queues[priority] = asyncio.Queue()
        
        self.running = False
        self.cpu_monitor = CPUMonitor(sample_interval=10.0)
        self._pending_jobs: set = set()
        
    async def add_task(self, priority: int, command: str, timestamp: float, redirect_output: bool = False):
        """Add task to appropriate priority queue"""
        if priority not in self.priority_queues:
            priority = 1
        
        job_key = (command, timestamp)
        if job_key in self._pending_jobs:
            logger.debug(f"Job already pending, skipping: {command} (scheduled at {timestamp})")
            return
        
        self._pending_jobs.add(job_key)
        await self.priority_queues[priority].put((timestamp, command, redirect_output))
        logger.info(f"Added task to priority {priority} queue: {command} ({self.priority_queues[priority].qsize()} tasks)")
        
    async def _execute_command(self, command: str, priority: int, redirect_output: bool = False) -> None:
        """Execute a command asynchronously"""
        try:
            logger.info(f"Executing command (priority {priority}): {command}")
            
            if redirect_output:
                process = await asyncio.create_subprocess_shell(
                    command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                
                stdout, stderr = await process.communicate()
                
                if process.returncode == 0:
                    logger.info(f"[REDIRECT] Command completed successfully: {command}")
                    if stdout:
                        output = stdout.decode().strip()
                        # Print directly to console without prefix
                        print(output)
                    if stderr:
                        error_output = stderr.decode().strip()
                        logger.warning(f"[REDIRECT STDERR] {command}: {error_output}")
                        print(f"[REDIRECT STDERR] {command}: {error_output}")
                else:
                    logger.error(f"[REDIRECT] Command failed with return code {process.returncode}: {command}")
                    if stderr:
                        error_output = stderr.decode().strip()
                        logger.error(f"[REDIRECT STDERR] {command}: {error_output}")
                        print(f"[REDIRECT ERROR] {command}: {error_output}")
                    if stdout:
                        output = stdout.decode().strip()
                        logger.info(f"[REDIRECT STDOUT] {command}: {output}")
                        print(f"[REDIRECT STDOUT] {command}: {output}")
            else:
                if sys.platform.startswith('win'):
                    console_cmd = ['start', 'cmd', '/k', command]
                    shell = True
                else:
                    console_cmd = ['bash', '-c', command]  # Simplified to avoid gnome-terminal
                    shell = True
                
                process = await asyncio.create_subprocess_shell(
                    ' '.join(console_cmd) if shell else command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    shell=shell
                )
                
                stdout, stderr = await process.communicate()
                
                if process.returncode == 0:
                    logger.info(f"Command completed successfully: {command}")
                    if stdout:
                        output = stdout.decode().strip()
                        logger.info(f"[STDOUT] {command}: {output}")
                        print(f"[TASK OUTPUT] {command}: {output}")
                    if stderr:
                        error_output = stderr.decode().strip()
                        logger.warning(f"[STDERR] {command}: {error_output}")
                        print(f"[TASK STDERR] {command}: {error_output}")
                else:
                    logger.error(f"Command failed with return code {process.returncode}: {command}")
                    if stderr:
                        error_output = stderr.decode().strip()
                        logger.error(f"[STDERR] {command}: {error_output}")
                        print(f"[TASK ERROR] {command}: {error_output}")
                    if stdout:
                        output = stdout.decode().strip()
                        logger.info(f"[STDOUT] {command}: {output}")
                        print(f"[TASK OUTPUT] {command}: {output}")
                    
        except Exception as e:
            logger.error(f"Error executing command '{command}': {e}", exc_info=True)

    async def _consume_priority_queue(self, priority: int):
        """Consume tasks from a priority queue"""
        print(f"Consumer {priority} started")
        logger.info(f"Starting consumer for priority {priority} in _consume_priority_queue")
        queue = self.priority_queues[priority]
        
        while self.running:
            try:
                await asyncio.sleep(0)
                timestamp, command, redirect_output = await asyncio.wait_for(queue.get(), timeout=5.0)
                
                logger.debug(f"Processing task from priority {priority} queue: {command}")
                while self.running and not self.cpu_monitor.is_cpu_low(20.0):
                    logger.info(f"CPU usage too high ({self.cpu_monitor.cpu_usage:.1f}%), waiting for priority {priority} task: {command}")
                    await asyncio.sleep(2.0)
                
                if self.running:
                    await self._execute_command(command, priority, redirect_output)
                    self._pending_jobs.discard((command, timestamp))
                    logger.info(f"Completed task from priority {priority} queue: {command}")
                else:
                    self._pending_jobs.discard((command, timestamp))
                
                queue.task_done()
                await asyncio.sleep(0.1)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in priority {priority} consumer: {e}", exc_info=True)
  
    async def start_consumers(self):
        """Start all priority queue consumers"""
        logger.info("Starting priority queue consumers...")
        print("Starting consumers...")
        self.running = True
        try:
            cpu_monitor_task = asyncio.create_task(self.cpu_monitor.start_monitoring())
            logger.debug(f"Created CPU monitor task: {cpu_monitor_task}, state: {cpu_monitor_task._state}")
            consumer_tasks = []
            loop = asyncio.get_running_loop()
            for priority in sorted(self.priority_queues.keys()):
                task = asyncio.create_task(self._consume_priority_queue(priority))
                consumer_tasks.append(task)
                logger.info(f"Started consumer for priority {priority}: {task}, state: {task._state}")
                logger.debug(f"Task count in loop: {len(loop._ready) + len(loop._scheduled)}")
            
            return [cpu_monitor_task] + consumer_tasks
        except Exception as e:
            logger.error(f"Error starting consumers: {e}", exc_info=True)
            raise
        
    async def stop_consumers(self):
        """Stop all consumers"""
        logger.info("Stopping priority queue consumers...")
        self.running = False
        self.cpu_monitor.stop_monitoring()
        
        for priority, queue in self.priority_queues.items():
            await queue.join()
            logger.info(f"Priority {priority} queue finished")
        
        pending_count = len(self._pending_jobs)
        self._pending_jobs.clear()
        if pending_count > 0:
            logger.info(f"Cleared {pending_count} pending jobs")

class AsyncCronScheduler:
    def __init__(self):
        self.jobs: List[CronJob] = []
        self.running = False
        self.queue_manager = PriorityQueueManager()
        self.crontab_mtime = 0.0
        self.crontab_filename = None

    def parse_cron_file(self, filename: str) -> None:
        """Parse crontab file with mandatory priority and redirect parameters"""
        self.crontab_filename = filename
        self.crontab_mtime = os.path.getmtime(filename) if os.path.exists(filename) else 0.0
        try:
            with open(filename, 'r') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.strip()
                    
                    if not line or line.startswith('#'):
                        continue
                    
                    logger.debug(f"Parsing line {line_num}: {line}")
                    
                    try:
                        parts = shlex.split(line)
                    except ValueError:
                        logger.warning(f"Failed to parse line with shlex, falling back to simple split: {line}")
                        parts = line.split()

                    if len(parts) < 7:
                        logger.warning(f"Invalid cron line {line_num}: {line} - Too few parts")
                        continue
                    
                    minute, hour, day, month, weekday = parts[:5]
                    command_and_params = parts[5:]
                    
                    if len(command_and_params) < 2:
                        logger.warning(f"Missing priority or redirect on line {line_num}: {line}")
                        continue
                    
                    redirect_output = command_and_params[-1].lower() in ['redirect', 'true', '1']
                    
                    try:
                        priority = int(command_and_params[-2])
                        if not 1 <= priority <= 10:
                            logger.warning(f"Invalid priority on line {line_num}: {priority}")
                            continue
                    except ValueError:
                        logger.warning(f"Invalid priority format on line {line_num}: {command_and_params[-2]}")
                        continue
                    
                    command = ' '.join(command_and_params[:-2])
                    
                    if not command:
                        logger.warning(f"Empty command on line {line_num}: {line}")
                        continue
                    
                    logger.debug(f"Parsed: minute={minute}, hour={hour}, day={day}, month={month}, weekday={weekday}, command='{command}', priority={priority}, redirect={redirect_output}")
                    
                    job = CronJob(minute, hour, day, month, weekday, command, priority, redirect_output)
                    self.jobs.append(job)
                    logger.info(f"Loaded job: {job}")
                
        except FileNotFoundError:
            logger.error(f"Cron file {filename} not found")
        except Exception as e:
            logger.error(f"Error parsing cron file: {e}")

    async def _monitor_crontab_file(self, interval: float = 10.0):
        """Periodically check crontab file for changes and reload if modified"""
        logger.info(f"Starting crontab file monitor for {self.crontab_filename}")
        while self.running:
            try:
                current_mtime = os.path.getmtime(self.crontab_filename)
                if current_mtime > self.crontab_mtime:
                    logger.info(f"Detected change in {self.crontab_filename} (mtime: {current_mtime})")
                    old_jobs = len(self.jobs)
                    self.jobs.clear()
                    self.parse_cron_file(self.crontab_filename)
                    logger.info(f"Reloaded {self.crontab_filename}: {len(self.jobs)} jobs (previously {old_jobs})")
                    self.crontab_mtime = current_mtime
                await asyncio.sleep(interval)
            except FileNotFoundError:
                logger.warning(f"Crontab file {self.crontab_filename} not found, waiting...")
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error monitoring crontab file: {e}", exc_info=True)
                await asyncio.sleep(interval)

    def _match_cron_field(self, field: str, current_value: int, max_value: int = None) -> bool:
        """Check if current value matches cron field pattern"""
        if field == '*':
            return True
        
        if '-' in field:
            start, end = map(int, field.split('-'))
            return start <= current_value <= end
        
        if '/' in field:
            if field.startswith('*/'):
                step = int(field[2:])
                return current_value % step == 0
            else:
                range_part, step = field.split('/')
                if self._match_cron_field(range_part, current_value, max_value):
                    if range_part == '*':
                        return current_value % int(step) == 0
                    else:
                        start = int(range_part.split('-')[0]) if '-' in range_part else int(range_part)
                        return (current_value - start) % int(step) == 0
                return False
        
        if ',' in field:
            values = [int(x) for x in field.split(',')]
            return current_value in values
        
        try:
            return int(field) == current_value
        except ValueError:
            return False

    def _should_run_job(self, job: CronJob, now: datetime) -> bool:
        """Check if job should run at current time"""
        cron_weekday = (now.weekday() + 1) % 7
        
        logger.debug(f"Checking job: {job}")
        logger.debug(f"Current time: {now} (minute={now.minute}, hour={now.hour}, day={now.day}, month={now.month}, weekday={cron_weekday})")
        
        minute_match = self._match_cron_field(job.minute, now.minute)
        hour_match = self._match_cron_field(job.hour, now.hour)
        day_match = self._match_cron_field(job.day, now.day)
        month_match = self._match_cron_field(job.month, now.month)
        weekday_match = self._match_cron_field(job.weekday, cron_weekday)
        
        logger.debug(f"Matches: minute={minute_match}, hour={hour_match}, day={day_match}, month={month_match}, weekday={weekday_match}")
        
        should_run = minute_match and hour_match and day_match and month_match and weekday_match
        logger.info(f"Job {'should' if should_run else 'should not'} run: {job}")
        return should_run

    async def _scheduler_loop(self) -> None:
        """Main scheduler loop - producer"""
        logger.info("Scheduler loop started")
        loop = asyncio.get_running_loop()
        logger.debug(f"Running in event loop: {loop}, tasks: {len(loop._ready) + len(loop._scheduled)}")
        
        try:
            while self.running:
                await asyncio.sleep(0)
                now = datetime.now()
                logger.debug(f"Scheduler tick at {now}")
                
                now = now.replace(second=0, microsecond=0)
                logger.info(f"Checking jobs at {now}")
                
                jobs_scheduled = 0
                for job in self.jobs:
                    if self._should_run_job(job, now):
                        await self.queue_manager.add_task(job.priority, job.command, now.timestamp(), job.redirect_output)
                        redirect_str = " (redirect)" if job.redirect_output else ""
                        logger.info(f"Scheduled job: {job.command} (priority: {job.priority}){redirect_str}")
                        jobs_scheduled += 1
                
                if jobs_scheduled > 0:
                    logger.info(f"Scheduled {jobs_scheduled} jobs at {now}")
                else:
                    logger.info(f"No jobs to schedule at {now}")
                
                next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                sleep_seconds = (next_minute - datetime.now()).total_seconds()
                logger.debug(f"Sleeping for {sleep_seconds:.2f} seconds until {next_minute}")
                if sleep_seconds > 0:
                    await asyncio.sleep(sleep_seconds)
        except Exception as e:
            logger.error(f"Error in scheduler loop: {e}", exc_info=True)
            raise

    async def _heartbeat(self):
        """Heartbeat to confirm event loop is running"""
        while self.running:
            loop = asyncio.get_running_loop()
            tasks = asyncio.all_tasks(loop)
            logger.info(f"Heartbeat at {datetime.now()}, tasks: {len(tasks)}")
            await asyncio.sleep(30)

    async def stop(self) -> None:
        """Stop the scheduler"""
        logger.info("Stopping scheduler...")
        self.running = False
        await self.queue_manager.stop_consumers()
        
async def demo_scheduler():
    """Demo function showing how to use the scheduler"""
    scheduler = AsyncCronScheduler()
    scheduler.parse_cron_file('crontab.txt')
    scheduler.running = True
    
    loop = asyncio.get_running_loop()
    consumer_tasks = await scheduler.queue_manager.start_consumers()
    scheduler_task = asyncio.create_task(scheduler._scheduler_loop())
    heartbeat_task = asyncio.create_task(scheduler._heartbeat())
    monitor_task = asyncio.create_task(scheduler._monitor_crontab_file())  # New task
    
    logger.info(f"Started scheduler: {scheduler_task}, state: {scheduler_task._state}")
    logger.info(f"Started heartbeat: {heartbeat_task}, state: {heartbeat_task._state}")
    logger.info(f"Started crontab monitor: {monitor_task}, state: {monitor_task._state}")
    
    all_tasks = consumer_tasks + [scheduler_task, heartbeat_task, monitor_task]
    
    try:
        await asyncio.sleep(0)
        while scheduler.running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt")
    finally:
        logger.info("Initiating shutdown...")
        scheduler.running = False
        await scheduler.stop()
        for task in all_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.debug(f"Task {task} cancelled")
            except Exception as e:
                logger.error(f"Task {task} raised: {e}", exc_info=True)

def create_sample_crontab():
    """Create a sample crontab.txt file"""
    sample_content = """# Sample crontab file with mandatory priority and redirect parameters
# Format: minute hour day month weekday command priority redirect
# Priority: 1-10 (1=highest, 10=lowest)
# Redirect: 'redirect', 'true', or '1' to redirect output to original console
# Tasks only execute when CPU usage < 10%

# Test tasks running every minute
* * * * * echo "Test task - normal console output in gnome-terminal" 1 false
* * * * * echo "Test task - redirected to original console" 2 true
"""
    
    with open('crontab.txt', 'w') as f:
        f.write(sample_content)
    print("Created sample crontab.txt file with mandatory priority and redirect parameters")

async def test_scheduler():
    scheduler = AsyncCronScheduler()
    scheduler.parse_cron_file('crontab.txt')
    scheduler.running = True
    
    # Start the queue manager consumers and CPU monitor
    consumer_tasks = await scheduler.queue_manager.start_consumers()
    
    # Create tasks for scheduler loop and heartbeat
    scheduler_task = asyncio.create_task(scheduler._scheduler_loop())
    heartbeat_task = asyncio.create_task(scheduler._heartbeat())
    
    # Keep the scheduler running until interrupted
    all_tasks = consumer_tasks + [scheduler_task, heartbeat_task]
    try:
        while scheduler.running:
            await asyncio.sleep(1)
            logger.debug("Main loop tick")  # Confirm main loop is running
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt in test_scheduler")
    finally:
        await scheduler.stop()
        for task in all_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.debug(f"Task {task} cancelled")

if __name__ == "__main__":
    import os
    if not os.path.exists('crontab.txt'):
        create_sample_crontab()
    
    try:
        asyncio.run(demo_scheduler())
        #asyncio.run(test_scheduler())

    except KeyboardInterrupt:
        print("\nScheduler stopped by user")
