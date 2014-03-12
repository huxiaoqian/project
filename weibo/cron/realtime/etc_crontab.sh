# /etc/crontab
1,6,11,16,21,26,31,36,41,46,51,56 * * * * root ./sentiment_redis2mysql.sh
14,29,44,59 * * * * root ./profile_redis2leveldb.sh
