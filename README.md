# AVM v2
This repo contains webcrawslers written in scrapy. 

To run on a ec2 instance ssh to instance and:

1.Install requirements
sudo apt-get update
sudo apt-get install scrapy
sudo apt-get python3-pip
pip3 install scrapyd
pip3 install botocore
touch scrapyd.conf
vi scrapyd.conf

2.Add scrapyd.conf file content:
[scrapyd]
eggs_dir = eggs
logs_dir = logs
items_dir =
jobs_to_keep = 5
dbs_dir = dbs
max_proc = 0
max_proc_per_cpu = 4
finished_to_keep = 100
poll_interval = 5.0
bind_address = 0.0.0.0
http_port = 6800
debug = off
runner = scrapyd.runner
application = scrapyd.app.application
launcher = scrapyd.launcher.Launcher
webroot = scrapyd.website.Root

[services]
schedule.json = scrapyd.webservice.Schedule
cancel.json = scrapyd.webservice.Cancel
addversion.json = scrapyd.webservice.AddVersion
listprojects.json = scrapyd.webservice.ListProjects
listversions.json = scrapyd.webservice.ListVersions
listspiders.json = scrapyd.webservice.ListSpiders
delproject.json = scrapyd.webservice.DeleteProject
delversion.json = scrapyd.webservice.DeleteVersion
listjobs.json = scrapyd.webservice.ListJobs
daemonstatus.json = scrapyd.webservice.DaemonStatus

3. From a project root - locally (dir containing scrapy.cfg)
DEPLOY A SPIDER
scrapyd-deploy aws-target -p morizon_spider

4. Schedule spiders with crontab
@reboot cd /home/ubuntu && nohup >& /dev/null &
0 2 * * * curl http://localhost:6800/schedule.json -d project=morizon_spider -d spider=morizon_sale 
0 3 * * * curl http://localhost:6800/schedule.json -d project=morizon_spider -d spider=morizon_rent j