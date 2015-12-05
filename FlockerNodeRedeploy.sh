#!/usr/bin/env bash
git checkout mydev
git pull

# Move Python code from the Git clone to where they are used
rm -rf /opt/flocker/lib/python2.7/site-packages/flocker/
cp -r /flocker-source/flocker/flocker/ /opt/flocker/lib/python2.7/site-packages/

SYSTEMD_SOURCE_DIR=/etc/systemd/system/multi-user.target.wants
SOURCE_SERVICE_FILES=$(
   ls ${SYSTEMD_SOURCE_DIR}/*.service |
   grep flocker |
   xargs -n 1 -I {} sh -c 'basename {} .service'
);

# Stop systemd units before they are changed
for service in ${SOURCE_SERVICE_FILES};
do
    systemctl stop ${service}
done

# Reload systemd, so that it can find new or changed units:
systemctl daemon-reload

# Start systemd units
for service in ${SOURCE_SERVICE_FILES};
do
    if [ "$(systemctl is-enabled ${service})" == 'enabled' ]
    then
        systemctl start ${service}
    fi
done
