#!/usr/bin/python
import socket
import subprocess
import shutil
import sys
import os

REDIS_TEMPLATE='''bind 0.0.0.0
port ##REDIS_PORT##
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 5000
'''

SANDBOX_PATH=os.path.sep.join(
  os.path.split(
    os.path.abspath(sys.argv[0])
  )[:-1]
)

def create_instance(host, current_hosts):
  port = 6379 if host not in current_hosts else current_hosts[host] + 1
  current_hosts[host] = port
  instance_path = os.path.join(SANDBOX_PATH, host, '%d'%port)
  try:
    shutil.rmtree(instance_path)
  except:
    pass
  os.makedirs(instance_path)
  with open( os.path.join(instance_path, 'redis.conf'), 'w') as f:
    f.write( REDIS_TEMPLATE.replace('##REDIS_PORT##', str(port)) )
  ssh_command='''ssh %s \"cd %s/%s/%d; redis-server redis.conf --daemonize yes\"
  '''%(host, SANDBOX_PATH, host, port)
  print (ssh_command)
  os.system(ssh_command)
  return '%s:%d'%(socket.gethostbyaddr(host)[2][0], port)

def main():
  hosts = sys.argv[1:]
  current_hosts = {}
  cluster_members = []
  for host in hosts:
    cluster_members.append(
      create_instance(host, current_hosts)
    )
  cluster_command = '''echo "yes" | redis-trib.rb create %s'''%(' '.join(cluster_members))
  print(cluster_command)
  os.system(cluster_command)

if __name__ == "__main__":
  main()
