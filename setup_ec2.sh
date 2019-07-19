#!/bin/bash
snap install docker
&& docker pull yc7en/scrapyd-py3
&& docker run -d -p 6800:6800 yc7en/scrapyd-py3
&& echo "Started docker image successfully"
