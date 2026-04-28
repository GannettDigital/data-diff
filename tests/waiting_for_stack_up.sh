#!/usr/bin/env bash
set -o nounset -o pipefail -o errexit

wait_for_mysql() {
    local retries=60

    echo "Check MySQL DB running..."
    until docker exec dd-mysql sh -c 'mysql --protocol=tcp -h 127.0.0.1 -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "SELECT 1" >/dev/null 2>&1'
    do
        retries=$((retries - 1))
        if [ "$retries" -le 0 ]
        then
            echo "MySQL DB did not become ready in time"
            docker ps --filter "name=dd-mysql"
            docker logs dd-mysql
            exit 1
        fi

        echo "Waiting for MySQL DB starting..."
        sleep 5
    done

    echo "MySQL DB is ready"
}

wait_for_mysql

if [ -n "${DATADIFF_VERTICA_URI:-}" ]
    then
        echo "Check Vertica DB running..."
        while true
        do
            if docker logs dd-vertica | tail -n 100 | grep -q -i "vertica is now running"
            then
               echo "Vertica DB is ready";
               break;
            else
               echo "Waiting for Vertica DB starting...";
               sleep 10;
            fi
        done
fi
