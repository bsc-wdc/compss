# Simple test with docker machine swarm and runcompss-docker

1.- Create a swarm cluster with docker machine
```
$ ./create-swarm-docker-machine docktest 2
```

2.- Login to docker
```
$ docker login 
```

3.- Generate and upload image
```
$ compss_docker_gen_image --context-dir=$PWD/simple/ --image-base=compss/compss:2.7.pr --image-name=compss/simple-example:latest
```
4.- Run simple within docker
```
runcompss-docker --worker-containers=2 --swarm-manager=192.168.99.108:2376 --image-name=compss/simple-example:latest --context-dir=/home/jorgee/Testing/doker-test/simple/ -d --classpath=/home/jorgee/Testing/doker-test/simple/ /home/jorgee/Testing/doker-test/simple/simple.py 1
```
