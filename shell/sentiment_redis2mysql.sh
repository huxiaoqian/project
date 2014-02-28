# /etc/crontab
# 1,6,11,16,21,26,31,36,41,46,51,56 * * * * root
cd /opt/project/weibo/cron/moodlens
python _sentiment_redis2mysql.py >> ./logs/_sentiment_redis2mysql.log &
