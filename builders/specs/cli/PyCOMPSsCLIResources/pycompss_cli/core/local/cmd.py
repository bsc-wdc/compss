#!/usr/bin/env python3
#
#  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

from dataclasses import dataclass, field


@dataclass
class _CrateContext:
    crate_path: str
    crate: ROCrate
    root: Entity | None = None
    main_entity: Entity | None = None
    create_action: Entity | None = None
    profiles: list[str] = field(default_factory=list)
    task_stats: dict = field(
        default_factory=lambda: {
            "completed": 0,
            "failed": 0,
            "canceled": 0,
            "total": 0,
        }
    )


# ############# #
# API FUNCTIONS #
# ############# #


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


def _format_email(cp):
    if isinstance(cp, ContextEntity):
        return cp.get("email", "")
    if isinstance(cp, str):
        return cp
    return ""


def _format_affiliation(aff):
    if isinstance(aff, ContextEntity):
        return aff.get("name", aff.get("@id", ""))
    return str(aff or "")


def _build_name(author):
    given = author.get("givenName")
    family = author.get("familyName")
    if given and family:
        return f"{family}, {given}"
    return given or family or author.get("@id", "")


def _format_author(author):
    if isinstance(author, str):
        return f"[green]{author}"

    name = author.get("name") or _build_name(author)
    affiliation = _format_affiliation(author.get("affiliation"))
    email = _format_email(author.get("contactPoint"))

    return (
        f"[green]{name}[/green]"
        f"{f' [dark_orange]({affiliation})[/]' if affiliation else ''}"
        f"{f' [cyan]({email})[/]' if email else ''}"
    )


def _render_times(action_tree, ca, verbose):
    start_time = end_time = None
    if ca.get("startTime"):
        try:
            start_time = datetime.fromisoformat(
                ca["startTime"]
            )
        except (TypeError, ValueError):
            start_time = None
    if ca.get("endTime"):
        try:
            end_time = datetime.fromisoformat(ca["endTime"])
        except (TypeError, ValueError):
            end_time = None

    time_details = None
    if start_time and end_time:
        total_time = end_time - start_time
        time_details = action_tree.add(
            f"Execution Time —— [magenta]{total_time} s[/magenta]"
        )
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


def _render_host_info(tree, crate, ca):
    host_name_text = ""
    num_nodes_text = ""
    job_id_text = ""
    job_id = None

    if location := ca.get("location"):
        if isinstance(location, str):
            host_name_text = location
        elif isinstance(location, Entity):
            host_name_text = location.get("name") or location.get("alternateName") or location.get("@id")
    
    exec_info_str = ca.get("@id")
    if not location and exec_info_str.startswith("#COMPSs"):
        # We can extract more details. Hostname included from COMPSs 3.2 version
        # Old CreateAction id format #COMPSs_Workflow_Run_Crate_marenostrum4_SLURM_JOB_ID_27072117 
        # New format: #COMPSs_WRROC_Workflow_Run_Crate_MacBook-Pro-Raul-2025.local_4f748a91-50d8-4716-b107-f737045c548e
        # and some host names can be bsc_nvidia (with underscores), so, splitting by underscores may not work
        # It may be easier to get the host and job id from the 'name' rather than from the '@id'
        match = re.search(r"Crate_(.+?)(?:_SLURM_JOB_ID|_[0-9a-fA-F]{8}-[0-9a-fA-F-]{27}|$)", exec_info_str)
        host_name_text = match.group(1) if match else ""
        
    num_nodes_e = crate.get("#slurm_job_num_nodes")
    num_nodes_text = (
        f" ({num_nodes_e.get('value', '')} nodes)" if num_nodes_e else ""
    )

    if job_id_e := crate.get("#slurm_job_id"):
        job_id = job_id_e.get("value", "")
    else:
        if match := re.search(r"_SLURM_JOB_ID_(\d+)$", exec_info_str):
            job_id = match.group(1)
    if job_id:
        job_id_text = f" —— Job ID —— [blue]{job_id}" if job_id else ""

    if host_name_text or num_nodes_text or job_id_text:
        tree.add(
            f"Host —— [blue]{host_name_text}{num_nodes_text}[/blue]{job_id_text}"
        )


def _render_resource_usage(action_tree, ca, verbose):
    if "resourceUsage" in ca:
        # Create a dictionary with Resource Usage info, then print it
        ru_dict = {}
        for e in ca["resourceUsage"]:
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
        master_avg_cpu = None
        master_avg_mem = None
        for key, host_data in ru_dict.items():
            if "is_master" in host_data:
                master_avg_cpu = host_data.get("cpuAvg")
                master_avg_mem = host_data.get("memAvg")
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
            if avg_cpu and avg_mem:
                usage_tree = action_tree.add(
                    f"Resource Usage —— CPU [gold1]{avg_cpu} %[/] —— Mem [gold1]{avg_mem} %[/]"
                )
            else:
                if master_avg_cpu or master_avg_mem:
                    usage_tree = action_tree.add(
                        f"Resource Usage —— CPU [gold1]{master_avg_cpu} %[/] —— Mem [gold1]{master_avg_mem} %[/]"
                    )
        else:
            # add to usage_tree
            usage_tree = action_tree.add(
                f"Resource Usage ([cyan]method_name[/] (invocations): [gold1]Avg[/] —— [bright_red]Max[/] —— [light_green]Min[/] time in ms)"
            )
            for host, host_dict in sorted(ru_dict.items()):
                host_executed_tasks = 0
                master_text = (
                    " (master node)" if "is_master" in host_dict else ""
                )
                if host != "OVERALL":
                    host_tree = usage_tree.add(f"[blue]{host}{master_text}")
                else:
                    host_tree = usage_tree.add(f"[gold1]Overall Statistics")
                for metric, metric_value in host_dict.items():
                    if isinstance(metric_value, dict):
                        # Info about a method
                        if "executionTime" in metric_value:
                            continue  # Ignore executionTime metric
                        executions = metric_value.get("executions")
                        if executions == "None":
                            # None comes as a string in the host_dict, not as a real None
                            executions = 0
                        else:
                            executions = int(executions)
                        if host != "OVERALL" and executions == 0:
                            continue  # Do not print if no executions in a host, but print in the OVERALL
                        host_executed_tasks += executions
                        if executions > 0:
                            host_tree.add(
                                f"[cyan]{metric}[/] ({metric_value.get('executions', '')}): [gold1]{fmt(metric_value.get('avgTime', ''))}[/] —— [bright_red]{fmt(metric_value.get('maxTime', ''))}[/] —— [light_green]{fmt(metric_value.get('minTime', ''))}"
                            )
                        else:
                            host_tree.add(
                                f"[cyan]{metric}[/] ({metric_value.get('executions', '')})"
                            )
                # Deal with info about a machine direct metric
                if "cpuAvg" in host_dict:
                    host_tree.add(
                        f"CPU: [gold1]{host_dict.get('cpuAvg', '')} %[/] —— [bright_red]{host_dict.get('cpuMax', '')} %"
                    )
                elif host == "OVERALL":
                    if avg_cpu:
                        host_tree.add(f"CPU: [gold1]{avg_cpu} %")
                    elif master_avg_cpu:
                        host_tree.add(f"CPU: [gold1]{master_avg_cpu} %")
                if "memAvg" in host_dict:
                    host_tree.add(
                        f"Memory: [gold1]{host_dict.get('memAvg', '')} %[/] —— [bright_red]{host_dict.get('memMax', '')} %[/] —— [light_green]{host_dict.get('memMin', '')} %"
                    )
                elif host == "OVERALL":
                    if avg_mem:
                        host_tree.add(f"Memory: [gold1]{avg_mem} %")
                    elif master_avg_mem:
                        host_tree.add(f"Memory: [gold1]{master_avg_mem} %")
                if host != "OVERALL":
                    host_tree.label = f"[blue]{host}{master_text}[/] ({host_executed_tasks} tasks executed)"


def _render_agent(action_tree, ca):
    if ca.get("agent"):
        agent = ca.get("agent")
        action_tree.add(f"Agent —— {_format_author(agent)}")


def _render_submission(action_tree, ctx):
    crate_path = ctx.crate_path
    ca = ctx.create_action
    description = ca.get("description")
    # Backwards compatible with txt files, only works when Crates are not zipped
    args_files = [
        Path(crate_path) / f
        for f in [
            "compss_command_line_arguments.txt",
            "compss_submission_command_line.txt",
        ]
    ]  # Backward compatible with COMPSs < 3.3.3
    for file in args_files:
        try:
            with file.open("r", encoding="utf-8") as f:
                description = f.readline().strip()
        except Exception:
            pass
    if description:
        action_tree.add(f"Submission —— [dim]{description}")


def _render_environment(action_tree, ca, verbose):
    if ca.get("environment"):
        if verbose:
            env_tree = action_tree.add("Environment")
            for env in ca["environment"]:
                env_tree.add(
                    f"[dark_goldenrod]{env['name']}[/dark_goldenrod] = [green]{env['value']}[/green]"
                )
        else:
            action_tree.add(
                f"Environment —— [dark_goldenrod]{len(ca['environment'])} variables [/dark_goldenrod]"
            )


def _render_io(tree, title, items):
    if not items:
        return

    if not isinstance(items, list):
        items = [items]

    io_tree = tree.add(title)
    # The items in 'object' and 'result' should be values (PropertyValue, File, Dataset, Collection, ...)
    for item in items:
        item_str = ""
        if isinstance(item, str):
            # Backwards compatible with COMPSs 3.0
            io_tree.add(f"[dark_goldenrod]{item}[/]")
        elif isinstance(item, Entity):
            e_type = item.get("@type")
            if any(t in e_type for t in ["File", "Dataset", "Collection"]):
                # NAME
                if e_type == "Collection":
                    item_me = item.get("mainEntity", {})
                    item_name = item_me.get("alternateName") or item_me.get("@id")
                else:
                    item_name = item.get("alternateName") or item.get('@id')
                item_str = f"[dark_goldenrod]{item_name}[/]"

                # ADD num items and / or contentSize
                if any(t in e_type for t in ["Dataset", "Collection"]) and item_name != "./":
                    item_str += f" [dim]({len(item.get('hasPart'))} items)[/]"
                if "contentSize" in item:
                    # Mainly true for Files, but Datasets could have it defined
                    item_str += f" [dim]({int(item['contentSize']):,} bytes)[/]"
            elif e_type == "PropertyValue":
                name = item.get("name") or item.get("@id")
                value = str(item.get("value"))[:80]
                item_str = f"[dark_goldenrod]{name}[/] = [green]{value}[/]"
            else:
                # This needs to change if we want to print more info on other entities
                    item_str = f"[dark_goldenrod]{item.get('@id')}[/]"
            if item_str:
                io_tree.add(item_str)


def _render_data_assets(action_tree, ca, data_assets):
    if not data_assets:
        ins_e = ca.get("object")
        if ins_e and not isinstance(ins_e, list):
            ins_e = [ins_e]
        outs_e = ca.get("result")
        if outs_e and not isinstance(outs_e, list):
            outs_e = [outs_e]
        action_tree.add(
            f"Data assets —— [dark_goldenrod]{len(ins_e) if ins_e else 0} Inputs —— [dark_goldenrod]{len(outs_e) if outs_e else 0} Outputs"
        )
    else:
        if wf_in := ca.get("object"):
            _render_io(action_tree, "Inputs", wf_in)
        if wf_out := ca.get("result"):
            _render_io(action_tree, "Outputs", wf_out)


def _render_execution(tree, ctx: _CrateContext, verbose: bool, data_assets: bool):
    ca = ctx.create_action
    m_e = ctx.main_entity
    if not ca:
        return

    action_tree = tree.add("Execution details")

    # Name
    exec_info_str = ca.get("@id")
    if (
        ca_name := ca.get("name")
    ) and not exec_info_str.startswith("#COMPSs"):
        # Compatibility with non-COMPSs crates
        action_tree.add(f"Name —— [green]{ca_name}")

    # actionStatus
    if main_ca_status := ca.get("actionStatus", ""):
    # actionStatus potential values: ActiveActionStatus, CompletedActionStatus, FailedActionStatus, PotentialActionStatus
        if "Completed" in main_ca_status:
            action_tree.add(f"Status —— {'[yellow]COMPLETED[/yellow]'}")
        elif "Failed" in main_ca_status:
            action_tree.add(f"Status —— {'[red]FAILED[/red]'}")

    # Task summary
    if ctx.task_stats['total'] != 0:
        # Can't trust m_e.get("step"), since Process Run can have executed tasks but no steps defined 
        task_tree = action_tree.add(
            f"Executed Tasks: {ctx.task_stats['total']} —— [green]COMPLETED: {ctx.task_stats['completed']}[/green] —— [red]FAILED: {ctx.task_stats['failed']}[/red] —— [yellow]CANCELED: {ctx.task_stats['canceled']}[/yellow]"
        )

    _render_times(action_tree, ca, verbose)
    _render_host_info(action_tree, ctx.crate, ca)
    _render_resource_usage(action_tree, ca, verbose)
    _render_agent(action_tree, ca)
    if verbose:
        _render_submission(action_tree, ctx)
    _render_environment(action_tree, ca, verbose)
    _render_data_assets(action_tree, ca, data_assets)


def _add_single_author(tree, entity, field):
    if field not in entity:
        return
    if not (authors := entity.get(field)):
        # Some WFHub crates come with the authors field, but with an empty list
        return
    if not isinstance(authors, list):
        authors = [authors]
    if len(authors) == 1:
        tree.add(f"Authors - {_format_author(authors[0])}")
    else:
        tree.add(f"Authors - {_format_author(authors[0])}[gold1] and {len(authors) - 1 } more[/]")


def _add_authors(tree, entity, field):
    if field not in entity:
        return

    authors_tree = tree.add("Authors")
    authors = entity.get(field)
    if not isinstance(authors, list):
        authors = [authors]

    for author in authors:
        authors_tree.add(_format_author(author))


def _render_license(tree, entity):
    lic = entity.get("license")
    if not lic:
        return

    if isinstance(lic, ContextEntity):
        name = lic.get("name", "")
        url = lic.get("url", "")
        tree.add(f"License —— [green]{name} {f'({url})' if url else ''}")
    else:
        tree.add(f"License —— [green]{lic}")


def _render_publish_date(tree, entity):
    raw = entity.get("datePublished")
    try:
        dt = datetime.fromisoformat(raw)
        tree.add(
            f"Date Published —— [green]{dt.strftime('%A, %d of %B of %Y - %H:%M %Z')}"
        )
    except Exception:
        if raw:
            tree.add(f"Date Published —— [green]{raw}")


def _render_main_entity(tree, main_entity):
    if not main_entity:
        return

    me_tree = tree.add(f"Main entity —— [green]{main_entity.get('@id', '')}")

    if pl := main_entity.get("programmingLanguage"):
        name = pl.get("name", "")
        ver = pl.get("version")
        me_tree.add(
            f"Programming language —— [green]{name} {f'({ver})' if ver else ''}"
        )


def _render_software_reqs(tree, main_entity):
    if not main_entity:
        return
    
    software_requirements = main_entity.get("softwareRequirements")
    if not software_requirements:
        return
    
    deps_tree = tree.add("Software Requirements")
    if not isinstance(software_requirements, list):
        software_requirements = [software_requirements]

    for s in software_requirements:
        s_name = s.get("name", "")
        ver = s.get("softwareVersion")  # canonical expected
        deps_tree.add(f"[#B5651D]{s_name}{f' ({ver})' if ver else ''}")

def _truncate_description(s, max_lines=5, max_chars=500):
    # cut = min(len(s), max_chars)
    lines = 0
    for i, c in enumerate(s[:max_chars]):
        if c == "\n":
            lines += 1
            if lines >= max_lines:
                return s[:i+1] + "\n[cyan][... truncated text ...][/]"
    if len(s) >= max_chars + 10:
        # Try to finish current word
        return s[:max_chars + 10] + "\n[cyan][... truncated text ...][/]"
    else:
        return s


def _render_general_info(tree, ctx: _CrateContext, verbose: bool):
    e = ctx.root
    if not e:
        return

    if name := e.get("name"):
        tree.add(f"Name —— [green]{name}")

    if desc := e.get("description"):
        tree.add(f"Description —— [green]{desc if verbose else _truncate_description(desc)}")

    if "creator" in e:
        _add_authors(tree, e, "creator") if verbose else _add_single_author(tree, e, "creator")
    elif "author" in e:
        _add_authors(tree, e, "author") if verbose else _add_single_author(tree, e, "author")

    _render_license(tree, e)
    _render_publish_date(tree, e)
    _render_main_entity(tree, ctx.main_entity)

    if verbose:
        _render_software_reqs(tree, ctx.main_entity)

    if verbose and ctx.profiles:
        prof_tree = tree.add("RO-Crate compliance")
        for prof in ctx.profiles:
            prof_tree.add(f"[green]{prof}")


def _collect_profiles(ctx, entity):
    conforms_to = entity.get("conformsTo")
    if not conforms_to:
        return

    if not isinstance(conforms_to, list):
        conforms_to = [conforms_to]

    for prof in conforms_to:
        if isinstance(prof, ContextEntity):
            ctx.profiles.append(f"{prof.get('name', '')} ({prof.get('version', '')})")
        elif isinstance(prof, str):
            ctx.profiles.append(prof)


def _update_task_stats(stats, action):
    # actionStatus potential values: ActiveActionStatus, CompletedActionStatus, FailedActionStatus, PotentialActionStatus
    status = action.get("actionStatus", "")
    if "Completed" in status:
        stats["completed"] += 1
    elif "Failed" in status:
        stats["failed"] += 1
    elif "Potential" in status:
        stats["canceled"] += 1
    stats["total"] += 1


def _inspect_crate(path, crate: ROCrate) -> _CrateContext:
    # Provenance Run Crate: OrganizeAction and ControlActions are not mandatory. Task CreateActions must be searched sequentially
    ctx = _CrateContext(crate_path=path, crate=crate)

    ctx.root = crate.root_dataset
    ctx.main_entity = ctx.root.get("mainEntity")
    _collect_profiles(ctx, crate.root_dataset)

    for e in crate.get_entities():
        if "CreateAction" in e.type:
            if e.get("instrument") == ctx.main_entity:
                ctx.create_action = e
            else:
                # COMPSs 3.0 crates did not have 'instrument'
                # Does this increase processing time a lot for ProvenanceRun???
                ca_name = e.get("name")
                if ca_name and ca_name.startswith("COMPSs") and all(e.get(k) for k in ["actionStatus", "endTime", "agent"]):
                    ctx.create_action = e
                else:
                    # Task CreateAction
                    _update_task_stats(ctx.task_stats, e)
            
    return ctx


def _load_crate(path, console):
    try:
        return ROCrate(path)
    except Exception as e:
        console.print(f"[bold red] Error loading RO-Crate[/bold red] from [yellow]{path}[/yellow]: {e}")
        return None


def local_inspect_execution(ro_crate_list: list, verbose: bool, data_assets: bool):
    console = Console()

    for ro_crate_zip_or_dir in ro_crate_list:
        console.rule("RO-Crate Inspection")

        crate = _load_crate(ro_crate_zip_or_dir, console)
        if not crate:
            continue

        root_tree = Tree(f"[bold cyan]CRATE {ro_crate_zip_or_dir}")

        # import time
        # part_time = time.time()

        ctx = _inspect_crate(ro_crate_zip_or_dir, crate)

        _render_general_info(root_tree, ctx, verbose)
        _render_execution(root_tree, ctx, verbose, data_assets)

        console.print(root_tree)
        console.rule()

        # print(f"PROVENANCE | Process and print whole crate TIME: {time.time() - part_time} s")  


def _render_parameters(
    parent_node,
    title,
    values,
    valid_formal_params,
    is_compss_wf,
):
    section = parent_node.add(f"[bold green]{title}:[/bold green]")

    # Ensure both values (PropertyValues, Datasets, Files, ...) and FormalParameters are lists
    if not isinstance(values, list):
        values = [values]
    if not isinstance(valid_formal_params, list):
        valid_formal_params = [valid_formal_params]

    # Provenance Run Crate says matching of values with FormalParameters with exampleOfWork / workExample is not mandatory
    # It also recommends that the FormalParameter and the value have the same name
    for index, v in enumerate(values):
        param_section = section.add(f"Parameter {index + 1}")
        # Simple literal value
        if isinstance(v, str):
            param_section.add(f"Value: [dark_goldenrod]{v}[/dark_goldenrod]")
            continue

        # Expected types for COMPSs workflows are ["PropertyValue", "File", "Dataset"]
        # Matching can happen, since we fully use exampleOfWork / workExample
        # Try to get the corresponding FormalParameter for this value. This may fail since it is not
        # mandatory in the spec to add the correspondence
        eow = v.get("exampleOfWork", [])
        if not isinstance(eow, list):
            eow = [eow]
        fp_v = None
        for _fp in eow:
            if _fp in valid_formal_params:
                fp_v = _fp
                # First valid FormalParameter matching is enough
                break
        # if not fp_v, get whatever we can from v, since The PropertyValue / data entity has not been matched with any FormalParameter

        # In case of multi-file objects (Collection) we only print the main file name:
        if v.get("@type") == "Collection":
            v = v.get("mainEntity", {})

        # NAME
        name_str = None
        if fp_v:
            # Get variable name in the code, not the actual file name. Important for data entities. E.g. get 'fa' not 'A.0.0'
            name_str = fp_v.get('name')
        elif not name_str:
            # If the FormalParameter had no 'name' defined, try to get the one from the value
            name_str = v.get('name')
        if name_str:
            param_section.add(f"Name: [cyan]{name_str}[/cyan]")

        # TYPE
        type_str = None
        if fp_v:
            # SHOULD include: File, Dataset or Collection if it maps to a file, directory or multi-file dataset, respectively; 
            # PropertyValue if it maps to a dictionary-like structured value (e.g. a CWL record); 
            # DataType or one of its subtypes (e.g. Integer) if it maps to a non-structured value.
            additional_type = fp_v.get("additionalType") or fp_v.get("@type", "")
            type_str = (
                ", ".join(additional_type)
                if isinstance(additional_type, list)
                else additional_type
            )
            if multiv := fp_v.get("multipleValues"):
                # In the RO-Crate, the multipleValues obtained is a string, thus we compare to a string here
                if multiv == "True":
                    type_str = "Array, " + type_str
            if isinstance(additional_type, list) or multiv == "True":
                type_str = "[" + type_str + "]"
        else:
            type_str = v.get("@type")
        if type_str:
            param_section.add(f"Type: [grey50]{type_str}[/grey50]")

        # DESCRIPTION
        if desc_str := v.get("description"):
            # In COMPSs, we provide Rich information for Arrays and Dicts, worth to be printed
            param_section.add(f"Description: [grey50]{desc_str}[/grey50]")

        # VALUE
        # Data entities involved in an application’s input and output SHOULD have an @id that reflects the original file or directory name 
        # as processed by the application, but MAY be renamed to avoid clashes with other entities in the crate. In this case, 
        # they SHOULD refer to the original name via alternateName.
        # In COMPSs, for File and Datasets, we have the path to the file in the @id, better to print that than the 'name' or the 'alternateName'
        # even if they exist
        value_str = None
        if is_compss_wf:
            value_str = v.get("@id") if v.get("@type") in ["File", "Dataset"] else v.get("value")
        else:
            value_str = v.get("value") or v.get("alternateName") or v.get("@id")
        if value_str:
            param_section.add(f"Value: [dark_goldenrod]{value_str}[/dark_goldenrod]")


def _get_task_id(e, is_compss_wf):
    return e.id.split("_")[1] if is_compss_wf else e.id


def _get_method_name(e, is_compss_wf):
    method = e.get("instrument", {})
    if is_compss_wf:
        return method.get("@id", "").removeprefix("#"), method
    return method.get("name", ""), method

def _get_method_desc(e, is_compss_wf):
    # In CreateActions, 'name' usually includes a brief description of the task to be performed
    # Not useful to print this in COMPSs workflows, it is redundant
    method_desc = e.get("name", None)
    return method_desc if not is_compss_wf else None


def _get_status(e):
    action_status = e.get("actionStatus", "")

    if "CompletedActionStatus" in action_status:
        return "[green]COMPLETED[/green]", "COMPLETED"
    if "FailedActionStatus" in action_status:
        return "[red]FAILED[/red]", "FAILED"
    if "PotentialActionStatus" in action_status:
        return "[yellow]CANCELED[/yellow]", "CANCELED"

    return "", ""


def local_inspect_tasks(
    ro_crate_list,
    failing_tasks_only: bool,
    tasks_to_inspect: list[str],
    methods_to_inspect: list[str],
):
    from datetime import datetime
    from rich.tree import Tree
    from rich.console import Console

    console = Console()

    # Transform list to set to have faster "in" comparisons
    tasks_to_inspect = set(tasks_to_inspect) if tasks_to_inspect else None
    # Join and pre-compile the regular expression to achieve faster comparisons
    if methods_to_inspect:
        try:
            # compiled_methods = [re.compile(m) for m in methods_to_inspect]
            combined_methods = re.compile("|".join(methods_to_inspect))
        except re.error as e:
            raise ValueError(
            f"Invalid regex in methods: {methods_to_inspect}\n{e}"
        )

    for ro_crate_zip_or_dir in ro_crate_list:
        console.rule("RO-Crate Task Inspection")

        crate = _load_crate(ro_crate_zip_or_dir, console)
        if not crate:
            continue

        tree = Tree(f"[bold cyan]CRATE {ro_crate_zip_or_dir}")

        if crate.mainEntity and not crate.mainEntity.get("step"):
            console.print(
                " [yellow]Note: Workflow Step details are missing in this RO-Crate.\n For COMPSs, enable 'provenance_run: True' in the 'ro-crate-info.yaml' on your next run"
            )

        if crate.mainEntity.get("programmingLanguage").id == "#compss":
            is_compss_wf = True
        else:
            is_compss_wf = False

        log_tree = {}
        failing_tasks = set()
        canceled_tasks = set()
        print_candidates = []
        task_counter = 0

        # import time
        # part_time = time.time()

        # OrganizeAction -> object: all ControlActions of the tasks; result: main CreateAction
        # ControlAction  -> object: CreateAction of the task
        # Even when with COMPSs Task CreateActions could be found navigating from OrganizeAction -> object (ControlAction) -> object (CreateAction)
        # sequential search of CreateActions works much faster (6s vs 10s with 512K tasks)    
        # In the specification, OrganizeAction is MAY, and ControlAction is SHOULD. Therefore sequential search of Task CreateActions is always needed
        main_entity = crate.root_dataset.get("mainEntity")
        for e in crate.get_entities():
            if "CreateAction" in e.type:
                instr = e.get("instrument", None)  # CreateAction MUST have instrument to be considered an orchestrated Tool execution
                if instr and instr != main_entity:
                    # A Task / Tool execution CreateAction. Print candidate must match: task id, method_name, or status FAILED
                    task_counter += 1
                    task_id = _get_task_id(e, is_compss_wf)
                    method_name, _ = _get_method_name(e, is_compss_wf)
                    status, status_plain = _get_status(e)
                    if status_plain == "FAILED":
                        failing_tasks.add(task_id)
                    elif status_plain == "CANCELED":
                        canceled_tasks.add(task_id)
                        
                    # Eval candidate task. From less to most expensive evaluation. Once a part is false, the rest does not get evaluated
                    should_print = (
                        (not failing_tasks_only or status_plain == "FAILED")
                        and (not tasks_to_inspect or task_id in tasks_to_inspect)
                        and (not methods_to_inspect or combined_methods.search(method_name))
                    )
                    if should_print:
                        print_candidates.append(e)
            
            # —— LOGS ——
            # There is no way around this, all Files need to be examined, since the log will reference the corresponding task CreateAction with 'mentions'
            # There is no reference from the task CreateAction to the corresponding logs
            # Buidling all log_trees is useless for non print_candidates (if they will never be printed)
            elif is_compss_wf and "File" in e.type and e.get("about") and "logs" in e.get("@id"):
                task_id = (
                    e.get("about").get("@id").split("_")[1] if is_compss_wf else e.id
                )
                if (
                    (not tasks_to_inspect)
                    or (task_id in tasks_to_inspect)
                    or (failing_tasks_only and task_id in failing_tasks)
                ):
                    log_tree.setdefault(task_id, [])
                    log_tree[task_id].append(e.id)


        # print(f"PROVENANCE | Get CreateActions and logs TIME: {time.time() - part_time} s")
        # print(f"TO BE PRINTED: {len(print_candidates)}")

        task_tree = {}

        for e in print_candidates:
            # task_id could have been obtained from the Task ControlAction -> instrument -> position but it is only meaningful for COMPSs
            # Is it useful to print the 'position' for other WMSs???

            task_id = _get_task_id(e, is_compss_wf)
            status, status_plain = _get_status(e)
            if status_plain == "FAILED":
                failing_tasks.add(task_id)
            elif status_plain == "CANCELED":
                canceled_tasks.add(task_id)
            method_name, method = _get_method_name(e, is_compss_wf)
            method_desc = _get_method_desc(e, is_compss_wf)
            method_input_params = method.get("input", [])
            method_output_params = method.get("output", [])

            # All tasks will be printed here
            task_label = f"[bold yellow]Task {task_id}[/bold yellow]"
            task_tree[task_id] = tree.add(task_label)

            # —— STATUS ——
            if status:
                task_tree[task_id].add(f"Status: {status}")

            # —— METHOD ——
            if method_name:
                task_tree[task_id].add(f"Method: [cyan]{method_name}[/cyan]")

            # —— DESCRIPTION ——
            if method_desc:
                task_tree[task_id].add(f"Description: [grey50]{method_desc}[/grey50]")

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
            host = None
            if location := e.get("location"):
                
                host = location
            elif is_compss_wf and e.get("name"):
                name_before, _, name_host = e.get("name").rpartition(" ")
                host = name_host if name_before.endswith("host") else None
            if host:
                task_tree[task_id].add(f"Host: [blue]{host}[/blue]")

            # —— INPUTS ——
            _render_parameters(
                parent_node=task_tree[task_id],
                title="Inputs",
                values=e.get("object", []),
                valid_formal_params=method_input_params,
                is_compss_wf=is_compss_wf,
            )

            # —— OUTPUTS ——
            if "COMPLETED" in status or not status:
                _render_parameters(
                    parent_node=task_tree[task_id],
                    title="Outputs",
                    values=e.get("result", []),
                    valid_formal_params=method_output_params,
                    is_compss_wf=is_compss_wf,
                )

        # This can consume quite some time if all log_tree's are generated for extremely large workflows
        for task_id, logs in log_tree.items():
            if task_id in task_tree:
                log_section = task_tree[task_id].add("[bold green]Logs:[/bold green]")
                for log in logs:
                    log_section.add(f"[dim]{log}[/dim]")

        total_t = tree.add(f"[bold cyan]Total Tasks —— {task_counter}")
        total_t.add(f"[bold red]Failing Tasks —— {len(failing_tasks)}[/bold red]")
        total_t.add(f"[yellow]Canceled Tasks —— {len(canceled_tasks)}[/yellow]")

        console.print(tree)
        console.rule()

        # print(f"PROVENANCE | NEW TOTAL TIME: {time.time() - part_time} s")

