project
=======

weibo project.

Website Deployment
----------
``$ cd /path/to/project ``

1. Nginx  
``$ sudo cp conf/nginx.conf /path/to/nginx/conf``  
``$ sudo service nginx restart``

2. FastCGI script  
``$ chmod +x weibo.fcgi``

3. Run  
``$ ./weibo.fcgi``  
``$ chmod 666 /tmp/weibo-fcgi.sock``

Use Supervisor to manage FastCGI processes in future.