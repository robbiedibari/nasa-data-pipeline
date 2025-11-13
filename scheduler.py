import time
from datetime import datetime

import schedule
from asteroid_pipeline import update_latest


def job():
    """Wrapper for the update Job"""
    print("\n Starting Weekly schedule update")
    print(f" Time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ")
    update_latest()
    print("✅ Scheduled update complete\n")


schedule.every().sunday.at("00:00").do(job)

print("📅 Weekly Asteroid Update Scheduler Started")
print(f"   Schedule: Every Sunday at 00:00")
print(f"   Next run: {schedule.next_run()}")
print("\n   Press Ctrl+C to stop\n")


run_now = input("Run update now to catch up? (y/n):")
if run_now.lower() == "y":
    job()
    print(f"\n Next schedule run: {schedule.next_run()}")
else:
    print("Okay see you sunday!")
    exit()


while True:
    schedule.run_pending()
    time.sleep(3600)
