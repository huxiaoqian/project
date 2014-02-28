# /etc/crontab
# */2 * * * * root
cd /opt/project/weibo/cron/moodlens
python _check_run_notcustomize_topic.py >> ./logs/check_run_nocustomize_topic.log &
