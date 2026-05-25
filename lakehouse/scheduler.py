"""
MEDALLION ARCHITECTURE SCHEDULER
Menjalankan Bronze, Silver, Gold secara otomatis setiap interval

Fitur:
- Jalankan Bronze → Silver → Gold secara sequential
- Interval: Default setiap 1 jam (configurable)
- Logging: Catat timestamp dan status setiap run
- Error handling: Continue even jika ada error di satu stage
"""

import schedule
import time
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

# Interval dalam menit
BRONZE_INTERVAL = 60      # Update Bronze setiap 1 jam
SILVER_INTERVAL = 65      # Update Silver 5 menit setelah Bronze
GOLD_INTERVAL = 70        # Update Gold 10 menit setelah Bronze

# Script paths
BASE_PATH = Path(__file__).parent
BRONZE_SCRIPT = BASE_PATH / "01_bronze.py"
SILVER_SCRIPT = BASE_PATH / "02_silver.py"
GOLD_SCRIPT = BASE_PATH / "03_gold.py"

# Log file
LOG_FILE = BASE_PATH / "scheduler.log"

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def log_message(message, level="INFO"):
    """Log ke console dan file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] [{level}] {message}"
    
    print(log_line)
    
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_line + "\n")


def run_script(script_path, script_name):
    """Jalankan script Spark dan catat hasilnya."""
    try:
        log_message(f"🚀 Starting {script_name}...")
        
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=3600  # 1 jam timeout max
        )
        
        if result.returncode == 0:
            log_message(f"✅ {script_name} completed successfully", level="SUCCESS")
            return True
        else:
            log_message(f"❌ {script_name} failed with return code {result.returncode}", level="ERROR")
            log_message(f"STDERR: {result.stderr}", level="ERROR")
            return False
            
    except subprocess.TimeoutExpired:
        log_message(f"⏱️ {script_name} timed out after 1 hour", level="ERROR")
        return False
    except Exception as e:
        log_message(f"❌ Error running {script_name}: {e}", level="ERROR")
        return False


def run_bronze():
    """Run Bronze layer script."""
    log_message("="*80)
    run_script(BRONZE_SCRIPT, "BRONZE LAYER")
    log_message("="*80)


def run_silver():
    """Run Silver layer script."""
    log_message("="*80)
    run_script(SILVER_SCRIPT, "SILVER LAYER")
    log_message("="*80)


def run_gold():
    """Run Gold layer script."""
    log_message("="*80)
    run_script(GOLD_SCRIPT, "GOLD LAYER")
    log_message("="*80)


def run_medallion_cycle():
    """Run complete medallion cycle: Bronze → Silver → Gold."""
    log_message("\n" + "🔄 STARTING MEDALLION CYCLE" + "\n")
    
    bronze_ok = run_bronze()
    silver_ok = run_silver()
    gold_ok = run_gold()
    
    if bronze_ok and silver_ok and gold_ok:
        log_message("\n✅ MEDALLION CYCLE COMPLETED SUCCESSFULLY\n")
    else:
        log_message("\n⚠️ MEDALLION CYCLE COMPLETED WITH WARNINGS\n")


def scheduler_info():
    """Tampilkan info scheduler."""
    log_message("\n" + "="*80)
    log_message("MEDALLION ARCHITECTURE SCHEDULER")
    log_message("="*80)
    log_message(f"Bronze interval: Every {BRONZE_INTERVAL} minutes")
    log_message(f"Silver interval: Every {SILVER_INTERVAL} minutes (after Bronze)")
    log_message(f"Gold interval:   Every {GOLD_INTERVAL} minutes (after Bronze)")
    log_message(f"Log file: {LOG_FILE}")
    log_message("="*80 + "\n")


# ============================================================================
# SCHEDULER SETUP
# ============================================================================

def setup_scheduler():
    """Setup jadwal untuk ketiga scripts."""
    
    # Schedule Bronze layer
    schedule.every(BRONZE_INTERVAL).minutes.do(run_bronze)
    
    # Schedule Silver layer (dengan offset)
    schedule.every(SILVER_INTERVAL).minutes.do(run_silver)
    
    # Schedule Gold layer (dengan offset)
    schedule.every(GOLD_INTERVAL).minutes.do(run_gold)
    
    # Jalankan medallion cycle lengkap setiap jam
    schedule.every(BRONZE_INTERVAL).minutes.do(run_medallion_cycle)


# ============================================================================
# MAIN SCHEDULER LOOP
# ============================================================================

def main():
    """Main scheduler loop."""
    scheduler_info()
    
    setup_scheduler()
    
    log_message("✅ Scheduler initialized. Waiting for scheduled tasks...")
    log_message("Press Ctrl+C to stop.\n")
    
    # Run first cycle immediately
    run_medallion_cycle()
    
    # Then schedule for future
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every 1 minute
        except KeyboardInterrupt:
            log_message("\n🛑 Scheduler stopped by user")
            break
        except Exception as e:
            log_message(f"❌ Scheduler error: {e}", level="ERROR")
            time.sleep(60)


if __name__ == "__main__":
    main()
