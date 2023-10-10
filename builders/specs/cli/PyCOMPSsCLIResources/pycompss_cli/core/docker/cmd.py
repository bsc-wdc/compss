#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import io
import json
import os
import sys
import tarfile
import tempfile
import shutil
from uuid import uuid4
import subprocess

from pycompss_cli.core.cmd_helpers import command_runner

# ################ #
# GLOBAL VARIABLES #
# ################ #

master_name = "pycompss-master"
worker_name = "pycompss-worker"
service_name = "pycompss-service"
default_workdir = "/home/user/"
default_worker_workdir = default_workdir + ".COMPSsWorker"
default_cfg_file = "cfg"
default_cfg = default_workdir + default_cfg_file
default_image_file = "image"
default_image = default_workdir + default_image_file


IMAGE_NAME = "compss/compss:3.2"  # Update when releasing new version
DOCKER_AVAILABALE = True

try:
    import docker
    from docker.types import Mount
    from docker.errors import DockerException
    from docker.models.containers import Container
except ImportError:
    DOCKER_AVAILABALE = False

# ############# #
# API FUNCTIONS #
# ############# #

class ErrorContainerNotRunning(Exception):
    pass

class DockerCmd(object):
    def __init__(self, env_id) -> None:
        self.env_id = env_id

        if not DOCKER_AVAILABALE:
            print('ERROR: Pip package `docker` is required for creating docker environments.')
            exit(1)
        
        super().__init__()

        try:
            docker.APIClient(base_url="unix://var/run/docker.sock")
            self.client = docker.from_env()
        except DockerException:
            print("ERROR: Docker service is not running", file=sys.stderr)
            print("       Please, start docker service and try again", file=sys.stderr)
            exit(1)

        self.master_name = master_name + "-" + self.env_id
        self.worker_name = worker_name + "-" + self.env_id
        self.__setup_image_name()

    def __setup_image_name(self):
        global IMAGE_NAME
        if os.environ.get("DEFAULT_DISLIB_DOCKER_IMAGE") is not None:
            # This environment variable will be defined by the dislib script.
            # It can be overriden by the COMPSS_DOCKER_IMAGE or the -i flag
            # when running init.
            IMAGE_NAME = os.environ["DEFAULT_DISLIB_DOCKER_IMAGE"]
        elif os.environ.get("COMPSS_DOCKER_IMAGE") is not None:
            # If specified in an environment variable, take it
            IMAGE_NAME = os.environ["COMPSS_DOCKER_IMAGE"]
        elif len(self.client.containers.list(filters={"name": self.master_name})) > 0:
            # Condition equivalent to: is_running(master_name):
            # But since it is undefined yet, we do it explicitly.
            # If exists in the file (means that has been defined with init)
            master = self.client.containers.list(filters={"name": self.master_name})[0]
            # Command equivalent to: master = _get_master()
            # But since it is undefined yet, we do it explicitly.

            IMAGE_NAME = master.image.attrs["Id"][7:7+12]

    def docker_deploy_compss(self, working_dir: str,
                            log_dir: str,
                            image: str = "",
                            restart: bool = True,
                            privileged: bool = False,
                            update_image: bool = False) -> None:
        """ Starts the main COMPSs image in Docker.
        It stops any existing one since it can not coexist with itself.

        :param working_dir: Given working directory
        :param image: Given docker image
        :param restart: Force stop the existing and start a new one.
        :returns: None
        """
        

        if image:
            docker_image = image
        else:
            docker_image = IMAGE_NAME

        masters = self.client.containers.list(filters={"name": self.master_name},
                                        all=True)

        assert len(masters) < 2  # never should we run 2 masters

        if restart or self._exists(self.master_name):
            self.docker_kill_compss(False)

        if not self.is_running(self.master_name):
            print("Starting %s container in dir %s" % (self.master_name, working_dir))
            print("If this is your first time running PyCOMPSs it may take a " +
                "while because it needs to download the docker image. " +
                "Please be patient.")
            if update_image:
                subprocess.run(f'docker pull {docker_image}', shell=True)
            mounts = self._get_mounts(user_working_dir=working_dir, log_dir=log_dir)
            ports = {"8888/tcp": 8888,  # required for jupyter notebooks
                    "8080/tcp": 8080}  # required for monitor
            container: Container = self.client.containers.run(image=docker_image, name=self.master_name,
                                    mounts=mounts, detach=True, ports=ports, privileged=privileged)
            self._generate_resources_cfg(ips=["localhost"])
            self._generate_project_cfg(ips=["localhost"])

            container.exec_run(f'mkdir -p {os.path.dirname(working_dir)}')
            container.exec_run(f'ln -s {default_workdir} {working_dir}')

            # don't pass configs because they need to be  overwritten when adding
            # new nodes
            cfg_content = '{"working_dir":"' + working_dir + \
                        '","resources":"","project":""}'
            tmp_path, cfg_file = self._store_temp_cfg(cfg_content)
            self._copy_file(cfg_file, default_cfg)
            shutil.rmtree(tmp_path)

    
    def docker_start_compss(self):
        self.client.containers.get(self.master_name).start()


    def docker_update_image(self) -> None:
        """ Updates the default docker image.

        :returns: None
        """
        docker_image = "compss/compss:latest"
        print("Updating docker image: %s" % docker_image)
        if "COMPSS_DOCKER_IMAGE" in os.environ:
            docker_image = os.environ["COMPSS_DOCKER_IMAGE"]
            print("Found COMPSS_DOCKER_IMAGE environment variable: %s. Updating." %
                docker_image)
        else:
            print("COMPSS_DOCKER_IMAGE is unset or empty. Updating default docker image: %s" %  # noqa: E501
                docker_image)
        command_runner(["docker", "pull", docker_image])


    def docker_kill_compss(self, clean: bool = True) -> None:
        """ Stops all COMPSs images in Docker.

        :param clean: Force clean the generated files.
        :returns: None
        """
        if clean:
            # Clean the cfg file
            try:
                master = self._get_master()
                self._remove_cfg(master)
            except ErrorContainerNotRunning:
                print("WARNING: No master container running.")

        self._stop_by_name(self.master_name)
        self._stop_by_name(self.worker_name)


    def docker_exec_in_daemon(self, cmd: str, return_output=False, return_stream=False) -> None:
        """ Execute the given command in the main COMPSs image in Docker.

        :param cmd: Command to execute.
        :returns: The execution stdout.
        """

        if not self.is_running(self.master_name):
            self.docker_start_compss()

        master = self._get_master()
        _, output = master.exec_run(cmd, workdir=default_workdir, stream=True)
        
        if return_output:
            return list(output)[-1].decode().strip()
        if return_stream:
            return output
        try:
            for line in output:
                print(line.strip().decode())
        except KeyboardInterrupt:
            master.exec_run('compss_clean_procs')


    def docker_start_monitoring(self) -> None:
        """ Starts the COMPSs monitoring within the Docker instance.

        :returns: The monitoring initialization stdout.
        """
        print("Starting Monitor")
        if not self.is_running(self.master_name):
            self.start_daemon()

        cmd = "/etc/init.d/compss-monitor start"
        master = self._get_master()
        env = {"COMPSS_MONITOR": str(default_workdir) + "/.COMPSs"}
        _, output = master.exec_run(cmd,
                                    environment=env,
                                    workdir=default_workdir,
                                    stream=True)
        for line in output:
            print(line.strip().decode())
        print("Please, open: http://127.0.0.1:8080/compss-monitor")


    def docker_stop_monitoring(self) -> None:
        """ Stops the COMPSs monitoring within the Docker instance.

        :returns: The monitoring stop stdout.
        """
        print("Stopping Monitor")
        cmd = "/etc/init.d/compss-monitor stop"
        master = self._get_master()
        _, output = master.exec_run(cmd,
                                    workdir=default_workdir,
                                    stream=True)
        for line in output:
            print(line.strip().decode())


    def docker_components(self, option: str = "list",
                        resource: str = "worker",
                        value: str = "1") -> None:
        """ Performs actions over the COMPSS docker instances deployed.

        :param option: Option to perform (supported: list, add and remove)
        :param element: Element to add or remove (not needed for list)
        :param value: Amount of elements to add or remove (not needed for list)
        :returns: None
        """
        if option == "list":
            masters = self.client.containers.list(filters={"name": self.master_name})
            workers = self.client.containers.list(filters={"name": self.worker_name})
            for c in masters + workers:
                print(c.name)
        elif option == "add":
            if resource == "worker":
                if value.isdigit():
                    self._add_workers(int(value))
                else:
                    self._add_custom_worker(value)
            else:
                raise Exception("Unsupported resource to be added: " + resource)
        elif option == "remove":
            if resource == "worker":
                if value.isdigit():
                    self._remove_workers(int(value))
                else:
                    self._remove_custom_worker(value)
            else:
                raise Exception("Unsupported resource to be removed: " + resource)
        else:
            raise Exception("Unexpected components option: " + option)


    def is_running(self, name: str = master_name) -> bool:
        """ Checks if a docker instance is running.

        :param name: Instance name.
        :returns: True if running. False otherwise.
        """
        cs = self.client.containers.list(filters={"name": name})
        return len(cs) > 0

    def exists(self, name: str = None) -> bool:
        """ Checks if a docker instance exists.

        :param name: Instance name.
        :returns: True if exists. False otherwise.
        """
        if name is None:
            name = master_name
        cs = self.client.containers.list(all=True, filters={"name": name})
        return len(cs) > 0


    # ################# #
    # PRIVATE FUNCTIONS #
    # ################# #



    def _exists(self, name: str) -> bool:
        """ Checks if a docker instance exists.

        :param name: Instance name.
        :returns: True if exists. False otherwise.
        """
        cs = self.client.containers.list(filters={"name": name}, all=True)
        return len(cs) > 0


    def _get_master(self):
        """ Retrieve the COMPSs master container object.

        :returns: Master container object.
        """
        try:
            master = self.client.containers.list(filters={"name": master_name})[0]
        except IndexError:
            raise ErrorContainerNotRunning(master_name)
        return master


    def _get_workers(self) -> list:
        """ Retrieve the COMPSs worker containers objects.

        :returns: List of the Worker containers objects.
        """
        try:
            workers = self.client.containers.list(filters={"name": worker_name})
        except IndexError:
            raise ErrorContainerNotRunning(worker_name)
        return workers


    def _get_worker_ips(self) -> list:
        """ Retrieve the COMPSs worker containers IP address.

        :returns: List of the Worker containers IP address.
        """
        ips = [c.attrs["NetworkSettings"]["Networks"]["bridge"]["IPAddress"]
            for c in self.client.containers.list(filters={"name": worker_name})]
        return ips


    def _store_temp_cfg(self, cfg_content: str) -> tuple:
        """ Stores the given content in the temporary cfg file.

        :param cfg_content: Cfg file contents.
        :returns: The tmp file path and the cfg file name.
        """
        tmp_path = tempfile.mkdtemp()
        cfg_file = os.path.join(tmp_path, default_cfg_file)
        with open(cfg_file, "w") as f:
            f.write(cfg_content)
        return tmp_path, cfg_file


    def _copy_file(self, src: str, dst: str) -> None:
        """ Copy the given file to the given destination within the COMPSs docker
        master instance.

        :param src: Source file path.
        :param dst: Destination file path within the docker instance.
        :returns: None
        """
        master = self._get_master()
        os.chdir(os.path.dirname(src))
        src_name = os.path.basename(src)
        tar_name = src + ".tar"
        tar = tarfile.open(tar_name, mode="w")
        try:
            tar.add(src_name)
        finally:
            tar.close()
        data = open(tar_name, "rb").read()
        output = master.put_archive(os.path.dirname(dst), data)
        if not output:
            print("ERROR COPYING " + str(src) +
                " TO " + src(dst) +
                " OF MASTER CONTAINER!!!")


    def _get_mounts(self, user_working_dir: str, log_dir: str) -> list:
        """ Retrieve the list of folders to be mounted. It gets the Mount object
        from the given user working directory, and can include any other needed
        folder.

        :param user_working_dir: User working path.
        :returns: List of docker Mount objects.
        """
        # mount target dir needs to be absolute
        target_dir = default_workdir
        user_dir = Mount(target=target_dir,
                        source=user_working_dir,
                        type="bind")
        # WARNING: mounting .COMPSs makes it fail
        if '.COMPSs' not in log_dir:
            log_dir = log_dir + "/.COMPSs"
        os.makedirs(log_dir, exist_ok=True)

        compss_log_dir = Mount(target="/root/.COMPSs",
                               source=log_dir,
                               type="bind")
        mounts = [user_dir, compss_log_dir]
        return mounts


    def _generate_project_cfg(self, ips: list = (), cpus: int = 4,
                            install_dir: str = "/opt/COMPSs",
                            worker_dir: str = default_worker_workdir) -> str:
        """ Generates the project.xml according to the given parameters.

        :param ips: List of ip of the worker nodes.
        :param cpus: Number of cores per worker node.
        :param install_dir: COMPSs installation directory.
        :param worker_dir: Worker working directory.
        :returns: The cfg file contents for the project.
        """
        # ./generate_project.sh project.xml "172.17.0.3:4:/opt/COMPSs:/tmp"
        master = self._get_master()
        proj_cmd = "/opt/COMPSs/Runtime/scripts/system/xmls/generate_project.sh"
        master_ip = "127.0.0.1"
        workers_ip = ips
        proj_master = ":".join((master_ip, "0", install_dir, worker_dir))
        proj_workers = " ".join(
            ["%s:%s:%s:%s" % (ip, cpus, install_dir, worker_dir) for ip in
            workers_ip])
        cmd = "%s /project.xml '%s' '%s'" % (proj_cmd, proj_master, proj_workers)
        exit_code, output = master.exec_run(cmd=cmd)
        if exit_code != 0:
            print("Exit code: %s" % exit_code)
            for line in [l for l in output.decode().split("\n")]:
                print(line)
            sys.exit(exit_code)
        return proj_workers


    def _generate_resources_cfg(self, ips: list = (), cpus: int = 4) -> str:
        """ Generates the resources.xml according to the given parameters.

        :param ips: List of ip of the worker nodes.
        :param cpus: Number of cores per worker node.
        :returns: The cfg file contents for the resources.
        """
        # ./generate_resources.sh resources.xml "172.17.0.3:4"
        master = self._get_master()
        res_cmd = "/opt/COMPSs/Runtime/scripts/system/xmls/generate_resources.sh"
        res_arg = " ".join(["%s:%s" % (ip, cpus) for ip in ips])
        cmd = "%s /resources.xml '%s'" % (res_cmd, res_arg)
        exit_code, output = master.exec_run(cmd=cmd)
        if exit_code != 0:
            print("Exit code: %s" % exit_code)
            for line in [l for l in output.decode().split("\n")]:
                print(line)
            sys.exit(exit_code)
        return res_arg


    def _get_cfg(self, master) -> dict:
        """ Retrieve the cfg file contents as dictionary.

        :param master: Master docker instance object.
        :returns: CFG file contents as dictionary.
        """
        exit_code, output = master.exec_run(cmd="cat " + default_cfg)
        json_str = output.decode()
        cfg = json.loads(json_str)
        return cfg


    def _remove_cfg(self, master) -> dict:
        """ Remove the cfg file.

        :param master: Master docker instance object.
        :returns: None
        """
        exit_code, output = master.exec_run(cmd="rm -f" + default_cfg)
        # if exit_code != 0:
        #     for line in output:
        #         print(line.strip().decode())


    def _update_cfg(self, master, cfg: dict, ips, cpus) -> None:
        """ Update the cfg file with the given parameters.

        :param master: Master docker instance object.
        :param ips: List of IP addresses of the worker nodes.
        :param cpus: Number of cores per node.
        :returns: None
        """
        # Generate project.xml
        new_proj_cfg = self._generate_project_cfg(ips=ips, cpus=cpus)
        # Generate resources.xml
        new_res_cfg = self._generate_resources_cfg(ips=ips, cpus=cpus)
        # Update the cfg_content
        cfg_content = '{"working_dir":"' + cfg['working_dir'] + \
                    '","resources":"' + new_res_cfg + \
                    '","project":"' + new_proj_cfg + '"}'
        tmp_path, cfg_file = self._store_temp_cfg(cfg_content)
        self._copy_file(cfg_file, default_cfg)
        shutil.rmtree(tmp_path)


    def _add_custom_worker(self, custom_cfg: str) -> None:
        """ Add custom worker to the cfg file.
        * custom_cfg = "ip:cpus"

        :param custom_cfg: Existing custom cfg file.
        :returns: None
        """
        ip, cpus = custom_cfg.split(":")
        master = self._get_master()
        cfg = self._get_cfg(master)
        # try to copy the master working dir to custom worker
        os.system("scp -r %s %s:/tmp" % (cfg["working_dir"], ip))
        ips = self._get_worker_ips()
        ips.append(ip)
        self._update_cfg(master, cfg, ips, cpus)
        print("Connected worker %s\n\tCPUs: %s" % (ip, cpus))


    def _remove_custom_worker(self, custom_cfg: str) -> None:
        """ Remove custom worker from the cfg file.
        * custom_cfg = "ip:cpus"

        :param custom_cfg: Existing custom cfg file.
        :returns: None
        """
        ip, cpus = custom_cfg.split(":")
        master = self._get_master()
        cfg = self._get_cfg(master)
        # Find the worker with the given ip
        workers = self._get_workers()
        for w in workers:
            w_ip = w.attrs["NetworkSettings"]["Networks"]["bridge"]["IPAddress"]
            if w_ip == ip:
                w.remove(force=True)
        ips = self._get_worker_ips()
        self._update_cfg(master, cfg, ips, cpus)
        print("Removed worker %s" % (ip))


    def _add_workers(self, num_workers: int = 1,
                    user_working_dir: str = "",
                    cpus: int = 4) -> None:
        """ Add COMPSs workers to the cfg file.

        :param num_workers: Number of workers to e added.
        :param user_working_dir: User working directory.
        :returns: None
        """
        master = self._get_master()
        cfg = self._get_cfg(master)
        mounts = self._get_mounts(user_working_dir=cfg["working_dir"])
        for _ in range(num_workers):
            worker_id = worker_name + "-" + uuid4().hex[:8]
            self.client.containers.run(image=IMAGE_NAME, name=worker_id,
                                mounts=mounts, detach=True, auto_remove=True)
        ips = self._get_worker_ips()
        self._update_cfg(master, cfg, ips, cpus)
        print("Started %s worker/s\n\tWorking dir: %s\n\tCPUs: %s" %
            (num_workers, user_working_dir, cpus))


    def _remove_workers(self, num_workers: int = 1,
                        cpus: int = 4) -> None:
        """ Removes COMPSs workers from the cfg file.

        :param num_workers: Number of workers to e added.
        :param cpus: Number of cores of the workers.
        :returns: None
        """
        master = self._get_master()
        cfg = self._get_cfg(master)
        workers = self._get_workers()
        to_remove = workers[:num_workers]
        for worker in to_remove:
            worker.remove(force=True)
        ips = self._get_worker_ips()
        self._update_cfg(master, cfg, ips, cpus)
        print("Removed " + str(num_workers) + " workers.")


    def _stop_by_name(self, name: str) -> None:
        """ Stop a docker instance by name.

        :param name: Name of the instance to be removed.
        :returns: None
        """
        containers = self.client.containers.list(filters={"name": name}, all=True)
        for c in containers:
            c.remove(force=True)

    def docker_copy_to_host(self, container_name, src, dst):
        """ Copy files from a container directory to a host directory.

        :param container_name: Name of the container.
        :param src: Source path.
        :param dst: Destination path.
        :returns: None
        """
        container = self.client.containers.get(container_name)
        data, stat = container.get_archive(src)
        with open(dst, 'wb') as f:
            for chunk in data:
                f.write(chunk)
