# /etc/crontab
# */2 * * * * root
cd /opt/project/weibo/cron/moodlens
python _check_add_sentiment_topic.py >> ./logs/check_add_sentiment_topic.log &
