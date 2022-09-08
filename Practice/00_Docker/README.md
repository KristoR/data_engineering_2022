## CLI

Make sure you have installed Docker Desktop.  
CLI docs: https://docs.docker.com/engine/reference/commandline/cli/   
Open up a terminal and run the following commands  

To see installed images:  
`docker images`  

To see running containers:  
`docker ps` 

To pull an image from a repository:  
`docker pull [NAME]`

e.g.   
`docker pull alpine:3.16`

To create a container:  
`docker create -it --name hello alpine:3.16`
Notes:  
`-it` enables interactive tty on the service. Not usually needed, depends on the type of service  
`--name hello` names our container as "hello"  
this command only creates the container, it does not start it  

To start a container:  
`docker start hello`

See that the container is running:  
`docker ps` 

Attaching to a container  
`docker exec -it hello sh`

OR (it works the same because we created the container with interactive tty)  
`docker attach hello`

Tips for exiting Docker container  
`exit` 
Exits, shuts down container  
`ctrl+a, ctrl+d`  
Detaches - works if you started a different tty session  
`ctrl+p, ctrl+q`  
Graceful detach, keeps container running. Depends on tool shortcuts (e.g. VS Code might have conflict)  

Shortcut to starting a container:  
`docker run alpine:3.16`  
If image does not exist, it pulls it. Then, it creates a container and starts it.  

`docker run -it --name hello_again alpine:3.15`  

Stopping a container:  
`docker stop hello`

Removing a container:  
`docker rm hello`

Removing an image (need to remove all associated containers first):  
`docker rmi alpine:3.15`



## Docker Compose

CLI reference: https://docs.docker.com/compose/reference/ 

To start up all the services:  
`docker compose up -d`  
Note: you need to be in the right directory  
Use `-d` flag to start services in detached mode.  

To stop services:  
`docker compose stop`

To stop and remove containers:  
`docker compose down` 

Review the .yml file  
* Versioning
* Services


## Next up: Assignment
