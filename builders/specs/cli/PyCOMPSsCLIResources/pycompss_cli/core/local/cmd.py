#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
import os
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List

import pycompss_cli.core.utils as utils
from rich.console import Console
from rich.panel import Panel
from rich.tree import Tree
from rocrate.model.contextentity import ContextEntity
from rocrate.model.entity import Entity
from rocrate.rocrate import ROCrate

# ################ #
# GLOBAL VARIABLES #
# ################ #

pointers = ["├── ", "└── "]
follow_prefix = "│   "
empty_prefix = "    "

METRICS_UNITS = {
    "maxTime": "ms",
    "minTime": "ms",
    "avgTime": "ms",
    "executionTime": "ms",
    "cpuAvg": "%",
    "cpuMax": "%",
    "memAvg": "%",
    "memMax": "%",
    "memMin": "%",
    "byteSent": "bytes",
    "byteRecv": "bytes",
}

ORDER = {"executions": 0, "avgTime": 1, "maxTime": 2, "minTime": 3}


# ############# #
# API FUNCTIONS #
# ############# #


def print_resource_usage_ordered(resource_usage_list: list):
    sorted_list = sorted(
        resource_usage_list,
        key=lambda line: ORDER.get(
            next((k for k in ORDER if k in line), ""), float("inf")
        ),
    )

    for line in sorted_list:
        print(line, end="")


def resources_tree(
    jsonData,
    name="",
    file=None,
    prefix=empty_prefix,
    last=False,
    isfirst=True,
    to_print=[],
):
    if isinstance(jsonData, dict):
        if not isfirst:
            print(prefix, pointers[1] if last else pointers[0], name, sep="", file=file)
        prefix += empty_prefix if last else follow_prefix
        length = len(jsonData)
        for i, key in enumerate(jsonData.keys()):
            last = i == (length - 1)
            resources_tree(
                jsonData[key], key, file, prefix, last, isfirst=False, to_print=to_print
            )
        print_resource_usage_ordered(to_print)
        to_print.clear()
    else:
        unit = METRICS_UNITS[name] if name in METRICS_UNITS.keys() else ""
        try:
            int_value = int(jsonData)
            name = name + f" = {int_value:,} {unit}"
        except ValueError:
            name = name + f" = {jsonData} {unit}"
        to_print.append(f"{prefix}{pointers[1] if last else pointers[0]}{name}\n")

def fmt(num):
    if num in (None, "", "None"):
        return ""
    try:
        return f"{int(num):,}"  # Thousands sepparated with comma
    except ValueError:
        return num  # Return whatever was there


def local_deploy_compss(working_dir: str = "") -> None:
    """Starts the main COMPSs image in Docker.
    It stops any existing one since it can not coexist with itself.

    :param working_dir: Given working directory
    :param image: Given docker image
    :param restart: Force stop the existing and start a new one.
    :returns: None
    """

    # cfg_content = '{"working_dir":"' + working_dir + \
    #                 '","resources":"","project":""}'
    # tmp_path, cfg_file = _store_temp_cfg(cfg_content)
    # _copy_file(cfg_file, default_cfg)
    # shutil.rmtree(tmp_path)
    pass


def local_run_app(cmd: List[str]) -> None:
    """Execute the given command in the main COMPSs image in Docker.

    :param cmd: Command to execute.
    :returns: The execution stdout.
    """

    if utils.check_exit_code("which enqueue_compss") == 1:
        cmd = ["module load COMPSs"] + cmd
    cmd = ";".join(cmd)

    subprocess.run(cmd, shell=True)


def local_jupyter(work_dir, lab_or_notebook, jupyter_args):
    cmd = f"jupyter {lab_or_notebook} --notebook-dir=" + work_dir
    process = subprocess.Popen(
        cmd + " " + jupyter_args,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        while True:
            line = process.stdout.readline()
            if line == "" and process.poll() is not None:
                break
            if line:
                print(line.strip().decode("utf-8"))
            if process.poll() is not None:
                break
    except KeyboardInterrupt:
        print("Closing jupyter...")
        process.kill()


def local_exec_app(command, return_process=False):
    p = subprocess.run(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if return_process:
        return p
    return p.stdout.decode().strip()


def local_submit_job(app_args, env_vars):
    cmd = f"enqueue_compss {app_args}"
    if utils.check_exit_code("which enqueue_compss") == 1:
        cmd = "module load COMPSs;" + cmd

    if env_vars:
        cmd = " ; ".join([*[f"export {var}" for var in env_vars], cmd])

    p = local_exec_app(cmd, return_process=True)
    job_id = p.stdout.decode().strip().split("\n")[-1].split(" ")[-1]
    if p.returncode != 0:
        print("ERROR:", p.stderr.decode())
    else:
        print("Job submitted:", job_id)
        return job_id


def local_job_list(local_job_scripts_dir):
    cmd = f"python3 {local_job_scripts_dir}/find.py"
    if utils.check_exit_code("which enqueue_compss") == 1:
        cmd = "module load COMPSs;" + cmd
    return local_exec_app(cmd)


def local_cancel_job(local_job_scripts_dir, jobid):
    cmd = f"python3 {local_job_scripts_dir}/cancel.py {jobid}"
    if utils.check_exit_code("which enqueue_compss") == 1:
        cmd = "module load COMPSs;" + cmd
    return local_exec_app(cmd)


def local_job_status(local_job_scripts_dir, jobid):
    cmd = f"python3 {local_job_scripts_dir}/status.py {jobid}"
    if utils.check_exit_code("which enqueue_compss") == 1:
        cmd = "module load COMPSs;" + cmd
    status = local_exec_app(cmd)

    if status == "SUCCESS\nSTATUS:":
        return "ERROR"
    return status


def local_app_deploy(local_source: str, app_dir: str, dest_dir: str = None):
    dst = os.path.abspath(dest_dir) if dest_dir else app_dir
    os.makedirs(dst, exist_ok=True)
    for f in os.listdir(local_source):
        if os.path.isfile(os.path.join(local_source, f)):
            shutil.copy(os.path.join(local_source, f), os.path.join(dst, f))
        else:
            shutil.copytree(os.path.join(local_source, f), os.path.join(dst, f))
    if dest_dir is not None:
        with open(app_dir + "/.compss", "w") as f:
            f.write(dst)
    print("App deployed from " + local_source + " to " + dst)


def local_inspect_execution(ro_crate_list: list, verbose: bool):
    console = Console()

    for ro_crate_zip_or_dir in ro_crate_list:
        console.rule("RO-Crate Inspection")
        try:
            crate = ROCrate(ro_crate_zip_or_dir)
        except Exception as e:
            console.print(
                f"[red]Error loading the RO-Crate from {ro_crate_zip_or_dir}: {e}"
            )
            continue

        root_tree = Tree(f"[bold cyan]CRATE {ro_crate_zip_or_dir}")

        nr_of_tasks_completed = 0
        nr_of_tasks_failed = 0
        total_tasks = 0
        profiles = []
        e_main_create_action = None
        e_main_entity = None

        for e in crate.get_entities():
            # --- GENERAL INFO ---
            if e.id == "./":
                if crate_name := e.get("name"):
                    root_tree.add(f"Name —— [green]{crate_name}")

                if crate_desc := e.get("description", ""):
                    root_tree.add(f"Description —— [green]{crate_desc}")

                if "creator" in e:
                    authors_tree = root_tree.add("Authors")
                    c_list = []
                    c_e = e.get("creator")
                    if isinstance(c_e, str):
                        authors_tree.add(f"[green]{c_e}")
                    elif isinstance(c_e, list):
                        c_list = c_e
                    elif isinstance(c_e, Entity):
                        c_list.append(c_e)

                    for c in c_list:
                        if isinstance(c, str):
                            authors_tree.add(f"[green]{c}")
                        else:
                            author_str = c.get("name", c.get("@id", ""))
                            if not "name" in c:
                                given_n = c.get("givenName")
                                family_n = c.get("familyName")
                                if given_n or family_n:
                                    author_str = (
                                        family_n
                                        if not given_n
                                        else (
                                            f"{family_n}, {given_n}"
                                            if family_n
                                            else given_n
                                        )
                                    )
                            affiliation = c.get("affiliation", {})
                            if isinstance(affiliation, ContextEntity):
                                affiliation_str = affiliation.get(
                                    "name", affiliation.get("@id", "")
                                )
                            else:
                                affiliation_str = str(affiliation or "")

                            contact_point = c.get("contactPoint", "")
                            if isinstance(contact_point, ContextEntity):
                                email = contact_point.get("email", "")
                            elif isinstance(contact_point, str):
                                email = contact_point
                            else:
                                email = ""

                            authors_tree.add(
                                f"[green]{author_str}[/green] {f'[dark_orange]({affiliation_str})[/]' if affiliation_str else ''} {f'[cyan]({email})[/]' if email else ''}"
                            )
                elif "author" in e:
                    authors_tree = root_tree.add("Authors")
                    c_list = []
                    c_e = e.get("author")
                    if isinstance(c_e, str):
                        authors_tree.add(f"[green]{c_e}")
                    elif isinstance(c_e, list):
                        c_list = c_e
                    elif isinstance(c_e, Entity):
                        c_list.append(c_e)

                    for c in c_list:
                        if isinstance(c, str):
                            authors_tree.add(f"[green]{c}")
                        else:
                            author_str = c.get("name", c.get("@id", ""))
                            if not "name" in c:
                                given_n = c.get("givenName")
                                family_n = c.get("familyName")
                                if given_n or family_n:
                                    author_str = (
                                        family_n
                                        if not given_n
                                        else (
                                            f"{family_n}, {given_n}"
                                            if family_n
                                            else given_n
                                        )
                                    )
                            affiliation = c.get("affiliation", {})
                            if isinstance(affiliation, ContextEntity):
                                affiliation_str = affiliation.get(
                                    "name", affiliation.get("@id", "")
                                )
                            else:
                                affiliation_str = str(affiliation or "")

                            contact_point = c.get("contactPoint", "")
                            if isinstance(contact_point, ContextEntity):
                                email = contact_point.get("email", "")
                            elif isinstance(contact_point, str):
                                email = contact_point
                            else:
                                email = ""

                            authors_tree.add(
                                f"[green]{author_str}[/green] {f'[dark_orange]({affiliation_str})[/]' if affiliation_str else ''} {f'[cyan]({email})[/]' if email else ''}"
                            )

                if license_e := e.get("license"):
                    if isinstance(license_e, ContextEntity):
                        lic_name = license_e.get("name", "")
                        lic_url = license_e.get("url", "")
                        root_tree.add(
                            f"License —— [green]{lic_name} {f'({lic_url})' if lic_url else ''}"
                        )

                    else:
                        root_tree.add(f"License —— [green]{license_e}")

                try:
                    publish_time = datetime.fromisoformat(e.get("datePublished"))
                except (TypeError, ValueError):
                    publish_time = None
                    publish_time_str = e.get("datePublished")
                if publish_time:
                    root_tree.add(
                        f"Date Published —— [green]{publish_time.strftime('%A, %d of %B of %Y - %H:%M %Z')}"
                    )
                else:
                    root_tree.add(f"Date Published —— [green]{publish_time_str}")

                if e_main_entity := e.get("mainEntity"):
                    if isinstance(e_main_entity, Entity):
                        me_tree = root_tree.add(
                            f"Main entity —— [green]{e_main_entity.get('@id', '')}"
                        )
                        if prog_lang_e := e_main_entity.get("programmingLanguage"):
                            pl_name = prog_lang_e.get("name", "")
                            pl_version = prog_lang_e.get("version")
                            me_tree.add(
                                f"Programming language —— [green]{pl_name} {f'({pl_version})' if pl_version else ''}"
                            )

                conforms_to = e.get("conformsTo")
                if isinstance(conforms_to, ContextEntity):
                    profiles.append(
                        f"{conforms_to.get('name', '')} ({conforms_to.get('version', '')})"
                    )
                elif isinstance(conforms_to, list):
                    for prof in conforms_to:
                        if isinstance(prof, ContextEntity):
                            profiles.append(
                                f"{prof.get('name', '')} ({prof.get('version', '')})"
                            )
                        elif isinstance(prof, str):
                            profiles.append(prof)

            elif "CreateAction" in e.type and e.get("instrument") == e_main_entity:
                e_main_create_action = e

            elif "ControlAction" in e.type:
                # actionStatus potential values: ActiveActionStatus, CompletedActionStatus, FailedActionStatus, PotentialActionStatus
                action_status = e.get("actionStatus", "")
                if "Completed" in action_status:
                    nr_of_tasks_completed += 1
                elif "Failed" in action_status:
                    nr_of_tasks_failed += 1
                total_tasks += 1

        if e_main_entity and (
            software_requirements := e_main_entity.get("softwareRequirements")
        ):
            deps_tree = root_tree.add("Software Requirements")
            if isinstance(software_requirements, list):
                for s in software_requirements:
                    s_name = s.get("name", "")
                    ver = s.get("softwareVersion")  # canonical expected
                    deps_tree.add(f"[#B5651D]{s_name}{f' ({ver})' if ver else ''}")
            else:
                s_name = software_requirements.get("name", "")
                ver = software_requirements.get("softwareVersion", "")
                deps_tree.add(f"[#B5651D]{s_name}{f' ({ver})' if ver else ''}")

        if verbose and profiles:
            prof_tree = root_tree.add("RO-Crate compliance")
            for prof in profiles:
                prof_tree.add(f"[green]{prof}[/green]")

        # --- EXECUTION DETAILS ---
        if e_main_create_action:
            action_tree = root_tree.add("Execution details")

            exec_info_str = e_main_create_action.get("@id")
            if (
                ca_name := e_main_create_action.get("name")
            ) and not exec_info_str.startswith("#COMPSs"):
                action_tree.add(f"Name —— [green]{ca_name}")

            if main_ca_status := e_main_create_action.get("actionStatus", ""):
                # actionStatus potential values: ActiveActionStatus, CompletedActionStatus, FailedActionStatus, PotentialActionStatus
                if "Completed" in main_ca_status:
                    action_tree.add(f"Status —— {'[yellow]COMPLETED[/yellow]'}")
                elif "Failed" in main_ca_status:
                    action_tree.add(f"Status —— {'[red]FAILED[/red]'}")

            if e_main_entity.get("step"):
                task_tree = action_tree.add(
                    f"Executed Tasks: {total_tasks} —— [green]COMPLETED: {nr_of_tasks_completed}[/green] [red]FAILED: {nr_of_tasks_failed}[/red]"
                )

            start_time = end_time = None
            if e_main_create_action.get("startTime"):
                try:
                    start_time = datetime.fromisoformat(
                        e_main_create_action["startTime"]
                    )
                except (TypeError, ValueError):
                    start_time = None
            if e_main_create_action.get("endTime"):
                try:
                    end_time = datetime.fromisoformat(e_main_create_action["endTime"])
                except (TypeError, ValueError):
                    end_time = None

            time_details = None
            if start_time and end_time:
                total_time = end_time - start_time
                time_details = action_tree.add(
                    f"Execution Time —— [magenta]{total_time} s[/magenta]"
                )
            # elif (start_time or end_time) and verbose:
            #     time_details = action_tree.add(f"Execution Time")
            if verbose and (start_time or end_time):
                if not time_details:
                    time_details = action_tree.add(f"Execution Time")
                if start_time:
                    time_details.add(
                        f"Start Time —— [green]{start_time.strftime('%A, %d of %B of %Y - %H:%M:%S %Z')}[/green]"
                    )
                if end_time:
                    time_details.add(
                        f"End Time   —— [green]{end_time.strftime('%A, %d of %B of %Y - %H:%M:%S %Z')}[/green]"
                    )

            if exec_info_str.startswith("#COMPSs"):
                # We can extract more details. Hostname included from COMPSs 3.2 version
                # Old Create action id format #COMPSs_Workflow_Run_Crate_marenostrum4_SLURM_JOB_ID_27072117
                # New format: #COMPSs_WRROC_Workflow_Run_Crate_MacBook-Pro-Raul-2025.local_4f748a91-50d8-4716-b107-f737045c548e
                exec_info = exec_info_str.split("_")
                host_index = exec_info.index("Crate") + 1
                host_name_text = (
                    exec_info[host_index] if exec_info[host_index] != "for" else ""
                )  # Avoid problems with < 3.2 versions
                num_nodes_e = crate.get("#slurm_job_num_nodes")
                num_nodes_text = (
                    f" ({num_nodes_e.get('value', '')} nodes)" if num_nodes_e else ""
                )
                job_id = None
                if job_id_e := crate.get("#slurm_job_id"):
                    job_id = job_id_e.get("value", None)
                elif len(exec_info) >= 9:
                    job_id = exec_info[-1]
                job_id_text = f" —— Job ID —— [blue]{job_id}" if job_id else ""
                action_tree.add(
                    f"Host —— [blue]{host_name_text}{num_nodes_text}[/blue]{job_id_text}"
                )

            # Resource Usage section
            if "resourceUsage" in e_main_create_action:
                # Create a dictionary with Resource Usage info, then print it
                ru_dict = {}
                for e in e_main_create_action["resourceUsage"]:
                    is_master = False
                    keys = e.get("@id", "").split(".")
                    value = e.get("value", "")
                    host = keys[0].lstrip("#").removesuffix("-ib0")
                    if host.endswith("-MASTER"):
                        host = host.removesuffix("-MASTER")
                        is_master = True
                    if host == "overall":
                        host = host.upper()
                    if host not in ru_dict:
                        ru_dict[host] = {}
                    if is_master:
                        ru_dict[host]["is_master"] = True
                    if len(keys) == 2:
                        # Metric id: #[host|host-MASTER].metric_name
                        metric = keys[1]
                        ru_dict[host][metric] = value
                    elif len(keys) >= 3:
                        # Metric id's are: #[overall|host].filename.method_name.metric_name
                        method = ".".join(keys[1:-1])
                        if method not in ru_dict[host]:
                            ru_dict[host][method] = {}
                        metric = keys[-1]
                        ru_dict[host][method][metric] = value

                # Calculate totals for non-verbose
                cpu_values = []
                mem_values = []

                del_key = None
                for key, host_data in ru_dict.items():
                    if "is_master" in host_data:
                        # We ingnore the data from the master to calculate the avg and merge entries
                        continue
                    cpu = host_data.get("cpuAvg")
                    mem = host_data.get("memAvg")
                    if cpu is not None:
                        cpu_values.append(float(cpu))
                    if mem is not None:
                        mem_values.append(float(mem))
                avg_cpu = (
                    round(sum(cpu_values) / len(cpu_values), 2) if cpu_values else None
                )
                avg_mem = (
                    round(sum(mem_values) / len(mem_values), 2) if mem_values else None
                )

                # Non-verbose
                if not verbose:
                    usage_tree = action_tree.add(
                        f"Resource Usage —— CPU [gold1]{avg_cpu} %[/] —— Mem [gold1]{avg_mem} %[/]"
                    )
                else:
                    # add to usage_tree
                    usage_tree = action_tree.add(f"Resource Usage ([cyan]method_name[/] (invocations): [gold1]Avg[/], [bright_red]Max[/], [light_green]Min[/] time in ms)")
                    for host, host_dict in sorted(ru_dict.items()):
                        master_text = (
                            " (master node)" if "is_master" in host_dict else ""
                        )
                        if host != "OVERALL":
                            host_tree = usage_tree.add(
                                f"[blue]{host} {master_text}"
                            )
                        else:
                            host_tree = usage_tree.add(
                                f"[gold1]{host} {master_text}"
                            )
                        for metric, metric_value in host_dict.items():
                            if isinstance(metric_value, dict):
                                # Info about a method
                                if "executionTime" in metric_value:
                                    continue  # Ignore executionTime metric
                                executions = metric_value.get('executions')
                                if host != "OVERALL" and executions == "None":
                                    # None comes as a string in the host_dict, not as a real None
                                    continue  # Do not print if no executions in a host, but print in the OVERALL
                                host_tree.add(
                                    f"[cyan]{metric}[/] ({metric_value.get('executions', '')}): [gold1]{fmt(metric_value.get('avgTime', ''))}[/] —— [bright_red]{fmt(metric_value.get('maxTime', ''))}[/] —— [light_green]{fmt(metric_value.get('minTime', ''))}"
                                )
                        # Deal with info about a machine direct metric
                        if "cpuAvg" in host_dict:
                            host_tree.add(
                                f"CPU: [gold1]{host_dict.get('cpuAvg', '')} % —— [bright_red]{host_dict.get('cpuMax', '')} %"
                            )
                        elif host == "OVERALL":
                            host_tree.add(f"CPU: [gold1]{avg_cpu} %")
                        if "memAvg" in host_dict:
                            host_tree.add(
                                f"Memory: [gold1]{host_dict.get('memAvg', '')} % —— [bright_red]{host_dict.get('memMax', '')} %[/] —— [light_green]{host_dict.get('memMin', '')} %"
                            )
                        elif host == "OVERALL":
                            host_tree.add(f"Memory: [gold1]{avg_mem} %")

            if (description := e_main_create_action.get("description")) and verbose:
                # Backwards compatible with txt files, only works when Crates are not zipped
                args_files = [Path(ro_crate_zip_or_dir) / f for f in ["compss_command_line_arguments.txt", "compss_submission_command_line.txt"]]  # Backward compatible with COMPSs < 3.3.3
                for file in args_files:
                    try:
                        with file.open("r", encoding="utf-8") as f:
                            description = f.readline().strip()
                    except Exception:
                        pass
                action_tree.add(
                    f"Submission —— [dim]{description}"
                )

            if e_main_create_action.get("agent"):
                agent = e_main_create_action.get("agent")
                agent_str = agent.get("name", str(agent))
                affiliation = agent.get("affiliation")
                if isinstance(affiliation, ContextEntity):
                    affiliation_str = affiliation.get("name", affiliation.get("@id"))
                else:
                    affiliation_str = str(affiliation or "")

                contact_point = agent.get("contactPoint", "")
                if isinstance(contact_point, ContextEntity):
                    email = contact_point.get("email", "")
                elif isinstance(contact_point, str):
                    email = contact_point
                else:
                    email = ""

                action_tree.add(
                    f"Agent —— [green]{agent_str}[/green] {f'[dark_orange]({affiliation_str})[/]' if affiliation_str else ''} {f'[cyan]({email})[/]' if email else ''}"
                )

            if e_main_create_action.get("environment"):
                if verbose:
                    env_tree = action_tree.add("Environment")
                    for env in e_main_create_action["environment"]:
                        env_tree.add(
                            f"[dark_goldenrod]{env['name']}[/dark_goldenrod] = [green]{env['value']}[/green]"
                        )
                else:
                    action_tree.add(
                        f"Environment —— [dark_goldenrod]{len(e_main_create_action['environment'])} variables [/dark_goldenrod]"
                    )

            data_assets = False
            if not data_assets:
                ins_e = e_main_create_action.get("object")
                outs_e = e_main_create_action.get("result")
                action_tree.add(
                    f"Data assets —— [dark_goldenrod]{len(ins_e) if ins_e else 0} Inputs —— [dark_goldenrod]{len(outs_e) if outs_e else 0} Outputs"
                )

            else:
                if e_main_create_action.get("object"):
                    inputs_tree = action_tree.add("Inputs:")
                    for wf_in in e_main_create_action["object"]:
                        if isinstance(wf_in, Entity):
                            if "contentSize" in wf_in:
                                inputs_tree.add(
                                    f"[dark_goldenrod]{wf_in.get('@id')}[/dark_goldenrod] [dim]({int(wf_in['contentSize']):,} bytes)[/dim]"
                                )
                            else:
                                inputs_tree.add(
                                    f"[dark_goldenrod]{wf_in.get('@id')}[/dark_goldenrod]"
                                )
                        if isinstance(wf_in, str):
                            # Backwards compatible with COMPSs 3.0
                            inputs_tree.add(f"[dark_goldenrod]{wf_in}[/dark_goldenrod]")

                if e_main_create_action.get("result"):
                    outputs_tree = action_tree.add("Outputs")
                    for wf_out in e_main_create_action["result"]:
                        if isinstance(wf_out, Entity):
                            if "contentSize" in wf_out:
                                outputs_tree.add(
                                    f"[dark_goldenrod]{wf_out.get('@id')}[/dark_goldenrod] [dim]({int(wf_out['contentSize']):,} bytes)[/dim]"
                                )
                            else:
                                outputs_tree.add(
                                    f"[dark_goldenrod]{wf_out.get('@id')}[/dark_goldenrod]"
                                )
                        elif isinstance(wf_out, str):
                            # Backwards compatible with COMPSs 3.0
                            outputs_tree.add(
                                f"[dark_goldenrod]{wf_out}[/dark_goldenrod]"
                            )

        console.print(root_tree)
        console.rule()


def local_inspect_tasks(
    ro_crate_list,
    failing_tasks_only: bool,
    tasks_to_inspect: list[int],
    methods_to_inspect: list[str]
):
    from datetime import datetime
    from rich.tree import Tree
    from rich.console import Console

    console = Console()

    for ro_crate_zip_or_dir in ro_crate_list:
        try:
            crate = ROCrate(ro_crate_zip_or_dir)
        except Exception as e:
            console.print(
                f"[bold red] Error loading the RO-Crate[/bold red] from [yellow]{ro_crate_zip_or_dir}[/yellow]: {e}"
            )
            continue

        tree = Tree(f"[bold cyan]CRATE {ro_crate_zip_or_dir}")

        log_tree = {}
        failing_tasks = set()
        task_create_actions = []

        # OrganizeAction -> object: ControlAction's of tasks, result: main CreateAction
        # ControlAction's of tasks -> object: CreateAction of the task
        for e in crate.get_entities():
            # Get only the ControlAction's from the OrganizeAction
            if "OrganizeAction" in e.type:
                for cont_act_task in e.get("object"):
                    task_create_actions.append(cont_act_task.get("object"))

            # —— LOGS ——
            if "File" in e.type and e.get("about") and "logs" in e.get("@id"):
                task_id = int(e.get("about").get("@id").split("_")[1])
                if (
                    (not tasks_to_inspect)
                    or (task_id in tasks_to_inspect)
                    or (failing_tasks_only and task_id in failing_tasks)
                ):
                    log_tree.setdefault(task_id, [])
                    log_tree[task_id].append(e.id)

        task_tree = {}
        task_counter = 0

        for e in task_create_actions:
            task_id = int(e.id.split("_")[1])
            task_counter += 1
            if "CompletedActionStatus" in e.get("actionStatus", ""):
                status = "[green]COMPLETED[/green]"
            else:
                status = "[red]FAILED[/red]"
                failing_tasks.add(task_id)

            method = e.get("instrument") or {}
            method_name = method.get("name", "")

            should_print = (
                (methods_to_inspect is None or any(re.search(m, method_name) for m in methods_to_inspect))
                and (not tasks_to_inspect or task_id in tasks_to_inspect)
                and (not failing_tasks_only or "FAILED" in status)
            )

            if should_print:
                task_label = f"[bold yellow]Task {task_id}[/bold yellow]"
                task_tree[task_id] = tree.add(task_label)

                # —— STATUS ——
                task_tree[task_id].add(f"Status: {status}")

                # —— METHOD ——
                task_tree[task_id].add(f"Method: [cyan]{method_name}[/cyan]")

                # —— EXECUTION TIME ——
                start_time = end_time = None
                if e.get("startTime"):
                    try:
                        start_time = datetime.fromisoformat(e.get("startTime"))
                    except (TypeError, ValueError):
                        start_time = None
                if e.get("endTime"):
                    try:
                        end_time = datetime.fromisoformat(e.get("endTime"))
                    except (TypeError, ValueError):
                        end_time = None

                if start_time and end_time:
                    execution_time = end_time - start_time
                    task_tree[task_id].add(
                        f"Execution Time: [magenta]{execution_time.total_seconds() * 1000:,.3f} ms[/magenta]"
                    )

                # —— HOST ——
                if e.get("name"):
                    name_before, _, name_host = e.get("name").rpartition(" ")
                    host = name_host if name_before.endswith("host") else None
                    task_tree[task_id].add(f"Host: [blue]{host}[/blue]")

                # —— INPUTS ——
                t_inputs = task_tree[task_id].add("[bold green]Inputs:[/bold green]")
                input_values = e.get("object", [])
                input_params = method.get("input", [])
                for index, param in enumerate(input_params):
                    param_section = t_inputs.add(f"Parameter {index + 1}")
                    param_section.add(f"Name: [cyan]{param.get('name', '')}[/cyan]")
                    param_section.add(
                        f"Type: [grey50]{param.get('additionalType') or param.get('@type', '')}[/grey50]"
                    )
                    val = next(
                        (v for v in param.get("workExample", []) if v in input_values),
                        {},
                    )
                    param_section.add(
                        f"Value: [dark_goldenrod]{val.get('value', val.get('@id'))}[/dark_goldenrod]"
                    )

                # —— OUTPUTS ——
                if "COMPLETED" in status:
                    output_values = e.get("result", [])
                    output_params = method.get("output", [])
                    for index, param in enumerate(output_params):
                        if index == 0:
                            t_outputs = task_tree[task_id].add(
                                "[bold green]Outputs:[/bold green]"
                            )
                        param_section = t_outputs.add(f"Parameter {index + 1}")
                        param_section.add(f"Name: [cyan]{param.get('name', '')}[/cyan]")
                        param_section.add(
                            f"Type: [grey50]{param.get('additionalType') or param.get('@type', '')}[/grey50]"
                        )
                        val = next(
                            (
                                v
                                for v in param.get("workExample", [])
                                if v in output_values
                            ),
                            {},
                        )
                        param_section.add(
                            f"Value: [dark_goldenrod]{val.get('value', val.get('@id'))}[/dark_goldenrod]"
                        )

        for task_id, logs in log_tree.items():
            log_section = task_tree[task_id].add("[bold green]Logs:[/bold green]")
            for log in logs:
                log_section.add(f"[dim]{log}[/dim]")

        tree.add(f"[bold cyan]Total Tasks —— {task_counter}")
        if failing_tasks_only:
            tree.add(f"[bold red]Failing Tasks —— {len(failing_tasks)}[/bold red]")

        if crate.mainEntity and not crate.mainEntity.get("step"):
            warning_panel = Panel(
                "[yellow]Note: Task-level execution details are missing in this RO-Crate. Enable `provenance_run: True` in the `ro-crate-info.yaml` on your next run.",
                border_style="yellow",
            )
            console.print(warning_panel)

        console.print(tree)
