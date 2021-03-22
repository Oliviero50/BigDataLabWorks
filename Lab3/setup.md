Run docker and remove container afterwards
sudo docker run -d --rm --name mymongo -p 27017-27019:27017-27019 mongo:latest

Run docker and persist datain local file system
sudo docker run -d --name testmongo -v /my/own/datadir:/data/db -p 27017-27019:27017-27019 mongo:latest