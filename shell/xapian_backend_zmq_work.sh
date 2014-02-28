var=`date "+%Y%m%d"`
cd /media/data/realtime/
mkdir -p $var
cd /home/ubuntu12/linhao/xapian_weibo/zmq_workspace/
python xapian_zmq_work.py -r >> xapian_zmq_work.log &
