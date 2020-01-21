import json
import os
import pickle
import sys
import tarfile
import tempfile
import shutil
from uuid import uuid4

import docker
from docker.types import Mount

client = docker.from_env()
# api_client = docker.APIClient()
api_client = docker.APIClient(base_url='unix://var/run/docker.sock')

if os.environ.get('COMPSS_DOCKER_IMAGE') is not None:
    image_name = os.environ['COMPSS_DOCKER_IMAGE']
else:
    image_name = 'compss/compss:2.6'  # Update when releasing new version
master_name = 'pycompss-master'
worker_name = 'pycompss-worker'
service_name = 'pycompss-service'
default_workdir = '/home/user'
default_cfg_name = 'cfg'
default_cfg = default_workdir + '/' + default_cfg_name


def _is_running(name: str):
    cs = client.containers.list(filters={'name': name})

    return len(cs) > 0


def _exists(name: str):
    cs = client.containers.list(filters={'name': name}, all=True)

    return len(cs) > 0


def _get_master():
    master = client.containers.list(filters={'name': master_name})[0]

    return master


def _get_workers():
    workers = client.containers.list(filters={'name': worker_name})

    return workers


def _get_worker_ips():
    ips = [c.attrs['NetworkSettings']['Networks']['bridge']['IPAddress']
           for c in client.containers.list(filters={'name': worker_name})]

    return ips


def _start_daemon(working_dir: str = "", restart: bool = True):
    masters = client.containers.list(filters={'name': master_name},
                                     all=True)
    assert len(masters) < 2  # never should we run 2 masters

    if restart or _exists(master_name):
        _stop_daemon(False)

    if not _is_running(master_name):
        if not working_dir:
            working_dir = os.getcwd()

        print("Starting %s container in dir %s" % (master_name, working_dir))
        print("If this is your first time running PyCOMPSs it may take a while "
              "because it needs to download the docker image. Please be "
              "patient.")

        mounts = _get_mounts(user_working_dir=working_dir)
        ports = {'8888/tcp': 8888,  # required for jupyter notebooks
                 '8080/tcp': 8080}  # required for monitor
        m = client.containers.run(image=image_name, name=master_name,
                                  mounts=mounts, detach=True, ports=ports)

        _generate_resources_cfg(ips=['localhost'])
        _generate_project_cfg(ips=['localhost'])

        # don't pass configs because they need to be  overwritten when adding
        # new nodes
        cfg_content = '{"working_dir":"' + working_dir + '","resources":"","project":""}'
        tmp_path, cfg_file = _store_temp_cfg(cfg_content)
        _copy_file(cfg_file, default_cfg)
        shutil.rmtree(tmp_path)


def _store_temp_cfg(cfg_content: str):
    tmp_path = tempfile.mkdtemp()
    cfg_file = os.path.join(tmp_path, default_cfg_name)
    with open(cfg_file, 'w') as f:
        f.write(cfg_content)
    return tmp_path, cfg_file


def _copy_file(src: str, dst: str):
    master = _get_master()

    os.chdir(os.path.dirname(src))
    src_name = os.path.basename(src)
    tar_name = src + '.tar'
    tar = tarfile.open(tar_name, mode='w')
    try:
        tar.add(src_name)
    finally:
        tar.close()

    data = open(tar_name, 'rb').read()
    output = master.put_archive(os.path.dirname(dst), data)

    if not output:
        print("ERROR COPYING CFG TO MASTER CONTAINER!!!")


def _get_mounts(user_working_dir: str):
    # mount target dir needs to be absolute
    target_dir = default_workdir

    user_dir = Mount(target=target_dir,
                     source=user_working_dir,
                     type='bind')

    compss_dir = os.environ['HOME'] + '/.COMPSs'
    os.makedirs(compss_dir, exist_ok=True)

    compss_log_dir = Mount(target='/root/.COMPSs',
                           source=compss_dir,
                           type='bind')

    mounts = [user_dir, compss_log_dir]

    return mounts


def _generate_project_cfg(curr_cfg: str = '', ips: list = (), cpus: int = 4,
                          install_dir: str = '/opt/COMPSs',
                          worker_dir: str = default_workdir):
    # ./generate_project.sh project.xml "172.17.0.3:4:/opt/COMPSs:/tmp"
    master = _get_master()
    proj_cmd = '/opt/COMPSs/Runtime/scripts/system/xmls/generate_project.sh'
    proj_arg = ' '.join(
        ["%s:%s:%s:%s" % (ip, cpus, install_dir, worker_dir) for ip in
         ips])
    cmd = "%s /project.xml '%s'" % (proj_cmd, proj_arg)
    exit_code, output = master.exec_run(cmd=cmd)
    if exit_code != 0:
        print("Exit code: %s" % exit_code)
        for line in [l for l in output.decode().split('\n')]:
            print(line)
        sys.exit(exit_code)
    return proj_arg


def _generate_resources_cfg(curr_cfg: str = '', ips: list = (), cpus: int = 4):
    # ./generate_resources.sh resources.xml "172.17.0.3:4"
    master = _get_master()

    res_cmd = '/opt/COMPSs/Runtime/scripts/system/xmls/generate_resources.sh'
    res_arg = ' '.join(["%s:%s" % (ip, cpus) for ip in ips])

    cmd = "%s /resources.xml '%s'" % (res_cmd, res_arg)
    exit_code, output = master.exec_run(cmd=cmd)
    if exit_code != 0:
        print("Exit code: %s" % exit_code)
        for line in [l for l in output.decode().split('\n')]:
            print(line)
        sys.exit(exit_code)
    return res_arg


def _get_cfg(master) -> dict:
    exit_code, output = master.exec_run(cmd='cat ' + default_cfg)
    json_str = output.decode()
    cfg = json.loads(json_str)

    return cfg


def _update_cfg(master, cfg: dict, ips, cpus):
    # Generate project.xml
    new_proj_cfg = _generate_project_cfg(cfg['project'], ips=ips, cpus=cpus)

    # Generate resources.xml
    new_res_cfg = _generate_resources_cfg(cfg['resources'], ips, cpus=cpus)

    cfg_content = '{"working_dir":"' + cfg['working_dir'] + '","resources":"' + new_res_cfg + '","project":"' + new_proj_cfg + '"}'
    tmp_path, cfg_file = _store_temp_cfg(cfg_content)
    _copy_file(cfg_file, default_cfg)
    shutil.rmtree(tmp_path)


def _add_custom_worker(custom_cfg: str):
    # custom_cfg = 'ip:cpus'
    ip, cpus = custom_cfg.split(':')

    master = _get_master()
    cfg = _get_cfg(master)

    # try to copy the master working dir to custom worker
    os.system("scp -r %s %s:/tmp" % (cfg['working_dir'], ip))

    ips = _get_worker_ips()
    ips.append(ip)
    _update_cfg(master, cfg, ips, cpus)

    print("Connected worker %s\n\tCPUs: %s" % (ip, cpus))


def _remove_custom_worker(custom_cfg: str):
    # custom_cfg = 'ip:cpus'
    ip, cpus = custom_cfg.split(':')

    master = _get_master()
    cfg = _get_cfg(master)

    # Find the worker with the given ip
    workers = _get_workers()
    for w in workers:
        w_ip = w.attrs['NetworkSettings']['Networks']['bridge']['IPAddress']
        if w_ip == ip:
            w.remove(force=True)

    ips = _get_worker_ips()
    _update_cfg(master, cfg, ips, cpus)

    print("Removed worker %s" % (ip))



def _add_workers(num_workers: int = 1, user_working_dir: str = "",
                 cpus: int = 4):
    master = _get_master()
    cfg = _get_cfg(master)
    mounts = _get_mounts(user_working_dir=cfg['working_dir'])

    for _ in range(num_workers):
        worker_id = worker_name + '-' + uuid4().hex[:8]
        client.containers.run(image=image_name, name=worker_id,
                              mounts=mounts, detach=True, auto_remove=True)
    ips = _get_worker_ips()

    _update_cfg(master, cfg, ips, cpus)
    print("Started %s worker/s\n\tWorking dir: %s\n\tCPUs: %s" %
          (num_workers, user_working_dir, cpus))


def _remove_workers(num_workers: int = 1,
                    cpus: int = 4):
    master = _get_master()
    cfg = _get_cfg(master)
    workers = _get_workers()
    to_remove = workers[:num_workers]
    for worker in to_remove:
        worker.remove(force=True)
    ips = _get_worker_ips()

    _update_cfg(master, cfg, ips, cpus)
    print("Removed " + str(num_workers) + " workers.")



def _stop_daemon(clean):
    _stop_by_name(master_name)
    _stop_by_name(worker_name)
    if clean:
        # Clean the cfg file
        try:
            os.remove(default_cfg_name)
        except OSError:
            print("WARNING: Could not remove the local " + default_cfg_name + " file.")
            print("         Please, check the folder where you started pycompss.")


def _stop_by_name(name: str):
    containers = client.containers.list(filters={'name': name}, all=True)
    for c in containers:
        c.remove(force=True)


def _start_monitoring():
    print("Starting Monitor")
    if not _is_running(master_name):
        _start_daemon()

    cmd = "/etc/init.d/compss-monitor start"

    master = _get_master()
    _, output = master.exec_run(cmd, workdir=default_workdir, stream=True)

    for line in output:
        print(line.strip().decode())

    print("Please, open: http://127.0.0.1:8080/compss-monitor")


def _stop_monitoring():
    print("Stopping Monitor")
    cmd = "/etc/init.d/compss-monitor stop"

    master = _get_master()
    _, output = master.exec_run(cmd, workdir=default_workdir, stream=True)

    for line in output:
        print(line.strip().decode())


def _exec_in_daemon(cmd: str):
    print("Executing cmd: %s" % cmd)
    if not _is_running(master_name):
        _start_daemon()

    master = _get_master()
    _, output = master.exec_run(cmd, workdir=default_workdir, stream=True)

    for line in output:
        print(line.strip().decode())


def _components(arg: str = 'list'):
    args = arg.split()

    if len(args) > 0:
        subcmd = args[0]

    if len(args) == 0 or subcmd == 'list':
        masters = client.containers.list(filters={'name': master_name})
        workers = client.containers.list(filters={'name': worker_name})
        for c in masters + workers:
            print(c.name)
    elif subcmd == 'add':
        resource = args[1]
        if resource == 'worker':
            if args[2].isdigit():
                number_of_res = int(args[2])
                _add_workers(number_of_res)
            else:
                _add_custom_worker(args[2])
        else:
            print("Unsupported resource to be added: " + str(resource))
            print("Supported resources: worker")
    elif subcmd == 'remove':
        resource = args[1]
        if resource == 'worker':
            if args[2].isdigit():
                number_of_res = int(args[2])
                _remove_workers(number_of_res)
            else:
                _remove_custom_worker(args[2])
        else:
            print("Unsupported resource to be reduced: " + str(resource))
            print("Supported resources: worker")
    else:
        print("Unexpected components command: " + subcmd)
        print("Supported commponents commands: list, add, reduce")
