import asyncio
import os
import aiohttp
import time
import subprocess
from redis_client import redis_client
from gateway_add_device import get_device_port
import serial_asyncio
from datetime import datetime

# Gateway configuration
GATEWAY_ID = 1
GATEWAY_TOKEN = "abcdefgh12345678"
SERVER_URL = "http://192.168.43.56:8000"
DOWNLOAD_DIR = "./downloads"
MAX_CONCURRENT_JOBS = 4

# Semaphore for concurrent job processing
job_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)

def print_status(job_id=None, device_id=None, message=""):
    """Helper function for consistent status messages"""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    job_info = f"[Job {job_id}]" if job_id else ""
    device_info = f"[Device {device_id}]" if device_id else ""
    print(f"{timestamp} {job_info}{device_info} {message}")

async def poll_for_download_notifications():
    await redis_client.init()
    print_status(message="🚦 Started polling for download notifications")
    while True:
        try:
            notification = await redis_client.get_download_notification(GATEWAY_ID)
            if notification:
                print_status(
                    job_id=notification.get('job_id'),
                    message=f"📥 Received download notification: {notification}"
                )
                asyncio.create_task(process_job(
                    notification['job_id'],
                    notification['source_file_id']
                ))
        except Exception as e:
            print_status(message=f"🔴 Download notification error: {str(e)}")
        await asyncio.sleep(0.1)

async def poll_for_job_notifications():
    await redis_client.init()
    print_status(message="🚦 Started polling for job notifications")
    while True:
        try:
            job_data = await redis_client.get_job(GATEWAY_ID)
            if job_data:
                print_status(
                    job_id=job_data.get('job_id'),
                    device_id=job_data.get('device_id'),
                    message="📨 Received job notification"
                )
                asyncio.create_task(handle_job_notification(job_data))
        except Exception as e:
            print_status(message=f"🔴 Job notification error: {str(e)}")
        await asyncio.sleep(0.1)

async def handle_job_notification(job_data: dict):
    async with job_semaphore:
        try:
            job_id = job_data['job_id']
            device_id = job_data['device_id']
            print_status(job_id, device_id, "🚀 Starting job processing")

            # Sequential execution for this job
            await flash_device(job_id, device_id)
            await collect_logs(job_id, device_id)

            print_status(job_id, device_id, "✅ Job processing completed")
            
        except KeyError as e:
            print_status(message=f"🔴 Invalid job format: {str(e)}")
        except Exception as e:
            print_status(job_id, device_id, f"🔴 Job processing failed: {str(e)}")
            await update_job_status(job_id, "failed")

async def download_file(job_id: int, file_id: int) -> str:
    print_status(job_id, message=f"⏬ Starting download of file {file_id}")
    try:
        download_url = f"{SERVER_URL}/api/v1/gateways/download/{file_id}"
        headers = {"X-Gateway-Token": GATEWAY_TOKEN}
        
        job_dir = os.path.join(DOWNLOAD_DIR, str(job_id))
        os.makedirs(job_dir, exist_ok=True)
        
        async with aiohttp.ClientSession() as session:
            async with session.get(download_url, headers=headers) as response:
                if response.status == 200:
                    disposition = response.headers.get("Content-Disposition", "")
                    filename = (disposition.split("filename=")[-1].strip('"') 
                                if "filename=" in disposition 
                                else f"job_{job_id}_source.bin")
                    
                    filepath = os.path.join(job_dir, filename)
                    content = await response.read()
                    
                    with open(filepath, "wb") as f:
                        f.write(content)
                    
                    print_status(job_id, message=f"✅ Download completed: {filepath}")
                    return filepath
                else:
                    text = await response.text()
                    raise Exception(f"Download failed: {response.status} {text}")
    except Exception as e:
        print_status(job_id, message=f"🔴 Download failed: {str(e)}")
        raise

async def compile_source_code(job_id: int):
    print_status(job_id, message="🔧 Starting compilation")
    try:
        source_path = f"./downloads/{job_id}"
        compile_cmd = ["make", f"SRC_DIR={source_path}"]
        
        proc = await asyncio.create_subprocess_exec(
            *compile_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            raise Exception(f"Compilation failed:\n{stderr.decode()}")
        
        print_status(job_id, message="✅ Compilation successful")
    except Exception as e:
        print_status(job_id, message=f"🔴 Compilation failed: {str(e)}")
        raise

async def process_job(job_id: int, file_id: int):
    async with job_semaphore:
        try:
            job_dir = os.path.join(DOWNLOAD_DIR, str(job_id))
            os.makedirs(job_dir, exist_ok=True)
            
            print_status(job_id, message="📁 Creating job directory")
            source_file_path = await download_file(job_id, file_id)
            await compile_source_code(job_id)
            await update_job_status(job_id, "pending")
            
        except Exception as e:
            print_status(job_id, message=f"🔴 Error processing job: {str(e)}")
            await update_job_status(job_id, "failed")

async def flash_device(job_id: int, device_id: int):
    try:
        print_status(job_id, device_id, "⚡ Starting flashing process")
        port = get_device_port(device_id)
        if not port:
            raise Exception(f"Device {device_id} not found")
        
        source_path = f"./downloads/{job_id}"
        flash_cmd = ["make", "flash", f"PORT={port}", f"SRC_DIR={source_path}"]
        
        proc = await asyncio.create_subprocess_exec(
            *flash_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        try:
            await asyncio.wait_for(proc.wait(), timeout=30)
        except asyncio.TimeoutError:
            proc.kill()
            raise Exception("Flashing timed out after 30 seconds")
        
        if proc.returncode != 0:
            stderr = await proc.stderr.read()
            raise Exception(f"Flashing failed: {stderr.decode()}")
            
        await update_job_status(job_id, "running")
        print_status(job_id, device_id, "✅ Flashing completed successfully")
        
    except Exception as e:
        await update_job_status(job_id, "failed")
        print_status(job_id, device_id, f"🔴 Flashing failed: {str(e)}")
        raise

async def collect_logs(job_id: int, device_id: int):
    try:
        print_status(job_id, device_id, "📝 Starting log collection")
        port = get_device_port(device_id)
        if not port:
            raise Exception(f"Device {device_id} not found")
        
        log_path = os.path.join(DOWNLOAD_DIR, str(job_id), "logs.txt")
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        print_status(job_id, device_id, "⏳ Waiting for device initialization (5s)")
        await asyncio.sleep(5)
        
        # Open serial connection with proper settings
        reader, writer = await serial_asyncio.open_serial_connection(
            url=port,
            baudrate=115200
        )
        
        # Reset serial buffers
        writer.transport.serial.reset_input_buffer()
        writer.transport.serial.reset_output_buffer()
        
        # Toggle DTR/RTS lines to ensure device resets properly after flashing
        writer.transport.serial.dtr = False  # Data Terminal Ready
        writer.transport.serial.rts = False  # Request To Send
        await asyncio.sleep(0.5)
        writer.transport.serial.dtr = True
        writer.transport.serial.rts = True
        await asyncio.sleep(1)
        
        # Send a newline to trigger output from some devices
        writer.write(b'\n')
        await writer.drain()
        
        log_active = False
        with open(log_path, "w") as f:
            start_time = time.time()
            print_status(job_id, device_id, "📊 Starting log capture (timeout: 1 minute)")
            
            while time.time() - start_time < 60:  # 5 minutes timeout
                try:
                    # Read with a short timeout
                    line_bytes = await asyncio.wait_for(reader.readline(), 1.0)
                    
                    if not line_bytes:  # Skip empty lines
                        continue
                        
                    decoded = line_bytes.decode('utf-8', 'ignore').strip()
                    if decoded:
                        # Write to both file and terminal
                        f.write(decoded + "\n")
                        f.flush()
                        print_status(job_id, device_id, f"📄 {decoded}")
                        log_active = True
                        
                except asyncio.TimeoutError:
                    # Only print waiting message if we've seen output before
                    if log_active:
                        print_status(job_id, device_id, "⏳ No data, waiting...")
                    continue
                except Exception as e:
                    print_status(job_id, device_id, f"🔴 Log error: {str(e)}")
                    break
                    
        if not log_active:
            print_status(job_id, device_id, "⚠️ Warning: No log data received during collection period")
            
    except Exception as e:
        print_status(job_id, device_id, f"🔴 Log collection failed: {str(e)}")
        await update_job_status(job_id, "failed")
        raise
    else:
        print_status(job_id, device_id, "✅ Log collection completed")
        await upload_logs(job_id, log_path)
        await update_job_status(job_id, "completed")
    finally:
        # Ensure serial connection is closed properly
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def update_job_status(job_id: int, new_status: str):
    try:
        print_status(job_id, message=f"🔄 Updating status to '{new_status}'")
        update_url = f"{SERVER_URL}/api/v1/jobs/{job_id}/status"
        payload = {"status": new_status}
        
        async with aiohttp.ClientSession() as session:
            async with session.put(update_url, json=payload) as response:
                if response.status != 200:
                    text = await response.text()
                    raise Exception(f"Status update failed: {response.status} {text}")
        print_status(job_id, message=f"🟢 Status updated to '{new_status}'")
    except Exception as e:
        print_status(job_id, message=f"🔴 Status update failed: {str(e)}")
        raise

async def upload_logs(job_id: int, log_path: str):
    try:
        print_status(job_id, message=f"📤 Uploading logs from {log_path}")
        upload_url = f"{SERVER_URL}/api/v1/jobs/{job_id}/logs"
        headers = {"X-Gateway-Token": GATEWAY_TOKEN}
        
        async with aiohttp.ClientSession() as session:
            form_data = aiohttp.FormData()
            form_data.add_field("log_file", open(log_path, "rb"))
            
            async with session.post(upload_url, headers=headers, data=form_data) as response:
                if response.status != 200:
                    text = await response.text()
                    raise Exception(f"Log upload failed: {text}")
        print_status(job_id, message="✅ Log upload successful")
    except Exception as e:
        print_status(job_id, message=f"🔴 Log upload failed: {str(e)}")
        raise

async def main():
    print_status(message="🏁 Starting gateway client")
    await asyncio.gather(
        poll_for_download_notifications(),
        poll_for_job_notifications()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print_status(message="🛑 Gateway client stopped by user")
