sudo podman run --rm --privileged --detach --name pg_snpseek_1 -h pg_snpseek_1  --ip 10.88.0.21 -p 5432:5432 -v /path/to/cannseek-26trich/data:/var/lib/postgresql/data:rw  localhost/snpseek/freedb_3kfiltered:1.0
# wait a short time for database to start
sleep 30s  
sudo podman run --rm --privileged --detach --name tomcat-container -h tomcat-container  -p 8080:8080 -v /path/to/canseek_tomcat/webapps:/usr/local/tomcat/webapps:rw  -v /path/to/cannseek_flatfiles/transfer:/transfer:rw  docker.io/library/tomcat:latest
