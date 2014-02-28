# /etc/crontab
# */2 * * * * root
cd /opt/project/weibo/cron/identify
python cron_check.py >> ./logs/cron_check.log &
