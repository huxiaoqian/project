# /etc/crontab
# 14,29,44,59 * * * * root
cd /opt/project/weibo/cron/realtime
python profile_redis2leveldb.py >> ./logs/profile_redis2leveldb.log &
