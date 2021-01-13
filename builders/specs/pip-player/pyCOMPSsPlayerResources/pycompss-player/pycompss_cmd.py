import json
import os
import pickle
import sys
import tarfile
import tempfile
import shutil
import docker
from uuid import uuid4
from docker.types import Mount

# ################ #
# GLOBAL VARIABLES #
# ################ #

client = docker.from_env()
api_client = docker.APIClient(base_url='unix://var/run/docker.sock')
master_name = 'pycompss-master'
worker_name = 'pycompss-worker'
service_name = 'pycompss-service'
default_workdir = '/home/user/'
default_worker_workdir = '/home/user/.COMPSsWorker'
default_cfg_file = 'cfg'
default_cfg = default_workdir + '/' + default_cfg_file
default_image_file = 'image'
default_image = default_workdir + '/' + default_image_file

if os.environ.get('DEFAULT_DISLIB_DOCKER_IMAGE') is not None:
    # This environment variable will be defined by the dislib script.
    # It can be overriden by the COMPSS_DOCKER_IMAGE or the -i flag
    # when running init.
    image_name = os.environ['DEFAULT_DISLIB_DOCKER_IMAGE']
elif os.environ.get('COMPSS_DOCKER_IMAGE') is not None:
    # If specified in an environment variable, take it
    image_name = os.environ['COMPSS_DOCKER_IMAGE']
elif len(client.containers.list(filters={'name': master_name})) > 0:
    # Condition equivalent to: _is_running(master_name):
    # But since it is undefined yet, we do it explicitly.
    # If exists in the file (means that has been defined with init)
    master = client.containers.list(filters={'name': master_name})[0]
    # Command equivalent to: master = _get_master()
    # But since it is undefined yet, we do it explicitly.
    image_name = master.image.attrs['RepoTags'][0]
else:
    # Otherwise, fallback to default COMPSs image
    image_name = 'compss/compss:2.8'  # Update when releasing new version


# ############# #
# API FUNCTIONS #
# ############# #

def start_daemon(params: str = "", restart: bool = True):
    """
    Starts the main COMPSs image in Docker.
    It stops any existing one since it can not coexist with itself.
    :param params: Initialization parameters
    :param restart: Force stop the existing and start a new one.
    :returns: None
    """
    docker_image = image_name
    working_dir = ''
    image = ''
    i_tmp_path = ''
    # Parse input params
    if params:
        working_dir, image = _parse_init_params(params)
        if image:
            docker_image = image
    masters = client.containers.list(filters={'name': master_name},
                                     all=True)

    assert len(masters) < 2  # never should we run 2 masters

    if restart or _exists(master_name):
        stop_daemon(False)

    if not _is_running(master_name):
        if not working_dir:
            working_dir = os.getcwd()
        print("Starting %s container in dir %s" % (master_name, working_dir))
        print("If this is your first time running PyCOMPSs it may take a " +
              "while because it needs to download the docker image. " +
              "Please be patient.")
        mounts = _get_mounts(user_working_dir=working_dir)
        ports = {'8888/tcp': 8888,  # required for jupyter notebooks
                 '8080/tcp': 8080}  # required for monitor
        m = client.containers.run(image=docker_image, name=master_name,
                                  mounts=mounts, detach=True, ports=ports)
        _generate_resources_cfg(ips=['localhost'])
        _generate_project_cfg(ips=['localhost'])
        # don't pass configs because they need to be  overwritten when adding
        # new nodes
        cfg_content = '{"working_dir":"' + working_dir + \
                      '","resources":"","project":""}'
        tmp_path, cfg_file = _store_temp_cfg(cfg_content)
        _copy_file(cfg_file, default_cfg)
        shutil.rmtree(tmp_path)


def stop_daemon(clean: bool = True):
    """
    Stops all COMPSs images in Docker.
    :param clean: Force clean the generated files.
    :returns: None
    """
    if clean:
        # Clean the cfg file
        master = _get_master()
        _remove_cfg(master)

    _stop_by_name(master_name)
    _stop_by_name(worker_name)


def exec_in_daemon(cmd: str):
    """
    Execute the given command in the main COMPSs image in Docker.
    :param cmd: Command to execute.
    :returns: The execution stdout.
    """
    print("Executing cmd: %s" % cmd)
    if not _is_running(master_name):
        _start_daemon()

    master = _get_master()
    _, output = master.exec_run(cmd, workdir=default_workdir, stream=True)
    for line in output:
        print(line.strip().decode())


def start_monitoring():
    """
    Starts the COMPSs monitoring within the Docker instance.
    :returns: The monitoring initialization stdout.
    """
    print("Starting Monitor")
    if not _is_running(master_name):
        _start_daemon()

    cmd = "/etc/init.d/compss-monitor start"
    master = _get_master()
    env = {'COMPSS_MONITOR': str(default_workdir) + '/.COMPSs'}
    _, output = master.exec_run(cmd,
                                environment=env,
                                workdir=default_workdir,
                                stream=True)
    for line in output:
        print(line.strip().decode())
    print("Please, open: http://127.0.0.1:8080/compss-monitor")


def stop_monitoring():
    """
    Stops the COMPSs monitoring within the Docker instance.
    :returns: The monitoring stop stdout.
    """
    print("Stopping Monitor")
    cmd = "/etc/init.d/compss-monitor stop"
    master = _get_master()
    _, output = master.exec_run(cmd,
                                workdir=default_workdir,
                                stream=True)
    for line in output:
        print(line.strip().decode())


def components(arg: str = 'list'):
    """
    Performs actions over the COMPSS docker instances deployed.
    :param arg: Arguments string
    :returns: Action outuput.
    """
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
            print("Unsupported resource to be removed: " + str(resource))
            print("Supported resources: worker")
    else:
        print("Unexpected components command: " + subcmd)
        print("Supported commponents commands: list, add, remove")


# ################# #
# PRIVATE FUNCTIONS #
# ################# #

def _is_running(name: str):
    """
    Checks if a docker instance is running.
    :param name: Instance name.
    :returns: True if running. False otherwise.
    """
    cs = client.containers.list(filters={'name': name})
    return len(cs) > 0


def _exists(name: str):
    """
    Checks if a docker instance exists.
    :param name: Instance name.
    :returns: True if exists. False otherwise.
    """
    cs = client.containers.list(filters={'name': name}, all=True)
    return len(cs) > 0


def _get_master():
    """
    Retrieve the COMPSs master container object.
    :returns: Master container object.
    """
    master = client.containers.list(filters={'name': master_name})[0]
    return master


def _get_workers():
    """
    Retrieve the COMPSs worker containers objects.
    :returns: List of the Worker containers objects.
    """
    workers = client.containers.list(filters={'name': worker_name})
    return workers


def _get_worker_ips():
    """
    Retrieve the COMPSs worker containers IP address.
    :returns: List of the Worker containers IP address.
    """
    ips = [c.attrs['NetworkSettings']['Networks']['bridge']['IPAddress']
           for c in client.containers.list(filters={'name': worker_name})]
    return ips


def _parse_init_params(params: str):
    """
    Parse the initialization parameters.
    Currently supports 2:
        -i DOCKER_IMAGE_NAME
        -w WORK_PATH
    :returns: The working dir and image name.
    """
    supported_msg = "Supported -w for working directory and \
                     -i for COMPSs docker image."
    working_dir = ''
    image = ''
    fields = params.split()
    if len(fields) == 2:
        if fields[0] == '-w':
            working_dir = fields[1]
        elif fields[0] == '-i':
            image = fields[1]
        else:
            Exception("Unsupported flag: " + str(fields[0]) +
                      ". " + supported_msg)
    elif len(fields) == 4:
        if fields[0] == '-w':
            working_dir = fields[1]
        elif fields[0] == '-i':
            image = fields[1]
        else:
            Exception("Unsupported flag: " + str(fields[0]) +
                      ". " + supported_msg)
        if fields[2] == '-w':
            working_dir = fields[3]
        elif fields[2] == '-i':
            image = fields[3]
        else:
            Exception("Unsupported flag: " + str(fields[2]) +
                      ". " + supported_msg)
    else:
        raise Exception("Incorrect number of parameters")
    return working_dir, image


def _store_temp_cfg(cfg_content: str):
    """
    Stores the given content in the temporary cfg file.
    :param cfg_content: Cfg file contents.
    :returns: The tmp file path and the cfg file name.
    """
    tmp_path = tempfile.mkdtemp()
    cfg_file = os.path.join(tmp_path, default_cfg_file)
    with open(cfg_file, 'w') as f:
        f.write(cfg_content)
    return tmp_path, cfg_file


def _copy_file(src: str, dst: str):
    """
    Copy the given file to the given destination within the COMPSs docker
    master instance.
    :param src: Source file path.
    :param dst: Destination file path within the docker instance.
    :returns: None
    """
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
        print("ERROR COPYING " + str(src) +
              " TO " + src(dst) +
              " OF MASTER CONTAINER!!!")


def _get_mounts(user_working_dir: str):
    """
    Retrieve the list of folders to be mounted. It gets the Mount object
    from the given user working directory, and can include any other needed
    folder.
    :param user_working_dir: User working path.
    :returns: List of docker Mount objects.
    """
    # mount target dir needs to be absolute
    target_dir = default_workdir
    user_dir = Mount(target=target_dir,
                     source=user_working_dir,
                     type='bind')
    # compss_dir = os.environ['HOME'] + '/.COMPSs'
    # os.makedirs(compss_dir, exist_ok=True)
    #
    # compss_log_dir = Mount(target='/root/.COMPSs',
    #                        source=compss_dir,
    #                        type='bind')
    mounts = [user_dir]  # , compss_log_dir]
    return mounts


def _generate_project_cfg(ips: list = (), cpus: int = 4,
                          install_dir: str = '/opt/COMPSs',
                          worker_dir: str = default_worker_workdir):
    """
    Generates the project.xml according to the given parameters.
    :param ips: List of ip of the worker nodes.
    :param cpus: Number of cores per worker node.
    :param install_dir: COMPSs installation directory.
    :param worker_dir: Worker working directory.
    :returns: The cfg file contents for the project.
    """
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


def _generate_resources_cfg(ips: list = (), cpus: int = 4):
    """
    Generates the resources.xml according to the given parameters.
    :param ips: List of ip of the worker nodes.
    :param cpus: Number of cores per worker node.
    :returns: The cfg file contents for the resources.
    """
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
    """
    Retrieve the cfg file contents as dictionary.
    :param master: Master docker instance object.
    :returns: CFG file contents as dictionary.
    """
    exit_code, output = master.exec_run(cmd='cat ' + default_cfg)
    json_str = output.decode()
    cfg = json.loads(json_str)
    return cfg


def _remove_cfg(master) -> dict:
    """
    Remove the cfg file.
    :param master: Master docker instance object.
    :returns: None
    """
    exit_code, output = master.exec_run(cmd='rm ' + default_cfg)
    if exit_code != 0:
        for line in output:
            print(line.strip().decode())


def _update_cfg(master, cfg: dict, ips, cpus):
    """
    Update the cfg file with the given parameters.
    :param master: Master docker instance object.
    :param ips: List of IP addresses of the worker nodes.
    :param cpus: Number of cores per node.
    :returns: None
    """
    # Generate project.xml
    new_proj_cfg = _generate_project_cfg(ips=ips, cpus=cpus)
    # Generate resources.xml
    new_res_cfg = _generate_resources_cfg(ips=ips, cpus=cpus)
    # Update the cfg_content
    cfg_content = '{"working_dir":"' + cfg['working_dir'] + \
                  '","resources":"' + new_res_cfg + \
                  '","project":"' + new_proj_cfg + '"}'
    tmp_path, cfg_file = _store_temp_cfg(cfg_content)
    _copy_file(cfg_file, default_cfg)
    shutil.rmtree(tmp_path)


def _add_custom_worker(custom_cfg: str):
    """
    Add custom worker to the cfg file.
      * custom_cfg = 'ip:cpus'
    :param custom_cfg: Existing custom cfg file.
    :returns: None
    """
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
    """
    Remove custom worker from the cfg file.
      * custom_cfg = 'ip:cpus'
    :param custom_cfg: Existing custom cfg file.
    :returns: None
    """
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


def _add_workers(num_workers: int = 1,
                 user_working_dir: str = "",
                 cpus: int = 4):
    """
    Add COMPSs workers to the cfg file.
    :param num_workers: Number of workers to e added.
    :param user_working_dir: User working directory.
    :returns: None
    """
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
    """
    Removes COMPSs workers from the cfg file.
    :param num_workers: Number of workers to e added.
    :param cpus: Number of cores of the workers.
    :returns: None
    """
    master = _get_master()
    cfg = _get_cfg(master)
    workers = _get_workers()
    to_remove = workers[:num_workers]
    for worker in to_remove:
        worker.remove(force=True)
    ips = _get_worker_ips()
    _update_cfg(master, cfg, ips, cpus)
    print("Removed " + str(num_workers) + " workers.")


def _stop_by_name(name: str):
    """
    Stop a docker instance by name.
    :param name: Name of the instance to be removed.
    :returns: None
    """
    containers = client.containers.list(filters={'name': name}, all=True)
    for c in containers:
        c.remove(force=True)
