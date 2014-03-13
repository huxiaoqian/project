# /etc/crontab
# 10 * * * * ubuntu12
var=`date "+%Y%m%d"`
cd /media/data/
mkdir -p $var
python /home/ubuntu12/linhao/xapian_weibo/zmq_workspace/xapian_zmq_work.py -r >> /opt/project/weibo/cron/realtime/logs/xapian_zmq_work.log &
python /home/ubuntu12/linhao/xapian_weibo/zmq_workspace/xapian_zmq_work.py -r >> /opt/project/weibo/cron/realtime/logs/xapian_zmq_work.log &
python /home/ubuntu12/linhao/xapian_weibo/zmq_workspace/xapian_zmq_work.py -r >> /opt/project/weibo/cron/realtime/logs/xapian_zmq_work.log &
python /home/ubuntu12/linhao/xapian_weibo/zmq_workspace/xapian_zmq_realtime_work.py >> /opt/project/weibo/cron/realtime/logs/xapian_zmq_realtime_work.log &
python /home/ubuntu12/linhao/xapian_weibo/zmq_workspace/xapian_zmq_realtime_work.py >> /opt/project/weibo/cron/realtime/logs/xapian_zmq_realtime_work.log &
python /home/ubuntu12/linhao/xapian_weibo/zmq_workspace/xapian_zmq_realtime_work.py >> /opt/project/weibo/cron/realtime/logs/xapian_zmq_realtime_work.log &
python /home/ubuntu12/linhao/xapian_weibo/zmq_workspace/xapian_zmq_realtime_work.py >> /opt/project/weibo/cron/realtime/logs/xapian_zmq_realtime_work.log &
python /home/ubuntu12/linhao/xapian_weibo/zmq_workspace/xapian_zmq_realtime_work.py >> /opt/project/weibo/cron/realtime/logs/xapian_zmq_realtime_work.log &
python /home/ubuntu12/linhao/xapian_weibo/zmq_workspace/xapian_zmq_realtime_work.py >> /opt/project/weibo/cron/realtime/logs/xapian_zmq_realtime_work.log &
