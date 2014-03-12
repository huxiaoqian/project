# /etc/crontab
# 2/* * * * * root
cd /opt/project/weibo/cron/realtime
python profile_merge_keywords_leveldb.py >> ./logs/profile_merge_keywords_leveldb.log &
