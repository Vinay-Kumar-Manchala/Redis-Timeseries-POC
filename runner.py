from apscheduler.schedulers.blocking import BlockingScheduler
from redis_timeseries_utility import PostgresLogger

scheduler = BlockingScheduler()

cleaner_obj = PostgresLogger()

scheduler.add_job(cleaner_obj.redis_db_cleaner, trigger='cron', minute='*')

# Start the scheduler
try:
    scheduler.start()
    print("****")
except Exception as e:
    print(str(e))
