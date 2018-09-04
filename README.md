# log2sqlite

This app can convert "on-the-fly" NGINX logs, that manage by logrotate.

App powered by chaky, 2018-09-03. (chaky22222222@gmail.com)

start with:
## ./bin/sentry


or
crystal build ./src/log2sqlite.cr 

and 
./log2sqlite sqlite_db="./sqlite_db.sqlite"  log_file="/var/log/nginx/../nginx/admins_requests.log"

