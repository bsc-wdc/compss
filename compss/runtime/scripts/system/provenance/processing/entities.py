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
import typing
import requests

from pathlib import Path

from rocrate.rocrate import ROCrate
from rocrate.model.person import Person
from rocrate.model.contextentity import ContextEntity

from utils.common_paths import find_subpath_in_cwd
from utils.common_paths import is_canonical


def add_person_definition(
    compss_crate: ROCrate, contact_type: str, yaml_author: dict, info_yaml: str
) -> tuple[bool, dict]:
    """
    Check if a specified person has enough defined terms to be added in the RO-Crate.

    :param compss_crate: The COMPSs RO-Crate being generated
    :param contact_type: contactType definition for the ContactPoint. "Author" or "Agent"
    :param yaml_author: Content of the YAML file describing the user
    :param info_yaml: Name of the YAML file specified by the user

    :returns: If the person is valid and added. Also returns updated details for the person
    """

    # Expected Person fields
    # orcid - Mandatory in RO-Crate 1.1
    # name - Mandatory in WorkflowHub
    # e-mail - Optional
    #
    # ror - Optional
    # organisation_name - Optional even if ror is defined. But won't show anything at WorkflowHub

    person_dict = {}
    mail_dict = {}
    org_dict = {}
    remote_orcid = None
    remote_org = None
    remote_org_name = None
    remote_mail = None
    remote_url = None
    remote_all_names = None
    searched_author = False

    if "name" in yaml_author:
        person_dict["name"] = yaml_author["name"]
        if not "orcid" in yaml_author:
            # If we have a name, but not an ORCID, search by name
            remote_orcid, remote_org, remote_mail, remote_all_names = search_orcid(
                yaml_author["name"]
            )
            searched_author = True
            if remote_orcid:
                yaml_author["orcid"] = remote_orcid
            # if not "organisation_name" in yaml_author and remote_org:
            # If the ror specified != remote_org, we have a problem
            # yaml_author["organisation_name"] = remote_org
    else:
        if "orcid" in yaml_author:
            # If we have an ORCID but not a name, we can try to complete the name info, searching by ORCID,
            # since the user has specified it
            remote_name, remote_org, remote_mail, remote_all_names = search_by_orcid(
                yaml_author["orcid"]
            )
            searched_author = True
            if remote_name:
                yaml_author["name"] = remote_name
                person_dict["name"] = remote_name

    if not "orcid" in yaml_author:
        print(
            f"PROVENANCE | ERROR in your {info_yaml} file. A 'Person' is ignored, since it has no 'orcid' defined"
        )
        return False, yaml_author

    if not "e-mail" in yaml_author and remote_mail:
        yaml_author["e-mail"] = remote_mail

    if "e-mail" in yaml_author:
        person_dict["contactPoint"] = {"@id": "mailto:" + yaml_author["e-mail"]}
        mail_dict["@type"] = "ContactPoint"
        mail_dict["contactType"] = contact_type
        mail_dict["email"] = yaml_author["e-mail"]
        mail_dict["identifier"] = yaml_author["e-mail"]
        mail_dict["url"] = yaml_author["orcid"]
        compss_crate.add(
            ContextEntity(compss_crate, "mailto:" + yaml_author["e-mail"], mail_dict)
        )

    # if "ror" in yaml_author and not "organisation_name" in yaml_author:
    # We can try to complete the organisation_name info, searching by ROR, since the user has specified it

    if not "ror" in yaml_author:
        # We can try to complete Organisation data
        if "organisation_name" in yaml_author:
            # Either specified by the user, or updated earlier by Author name
            remote_ror, remote_org_name, remote_url = search_ror(
                yaml_author["organisation_name"]
            )
        else:
            if not searched_author:
                # No previous author search has been done, search now
                if "orcid" in yaml_author:
                    remote_name, remote_org, remote_mail, remote_all_names = (
                        search_by_orcid(yaml_author["orcid"])
                    )
                else:
                    # Should not enter here, all authors should have orcid at this point
                    if "name" in yaml_author:
                        remote_orcid, remote_org, remote_mail, remote_all_names = (
                            search_orcid(yaml_author["name"])
                        )
            remote_ror, remote_org_name, remote_url = search_ror(remote_org)
        if remote_ror:
            yaml_author["ror"] = remote_ror
            yaml_author["organisation_name"] = remote_org_name
    else:
        if not "organisation_name" in yaml_author:
            # Search by ROR, if organisation_name has not been defined by the user
            remote_org_name, remote_url = search_by_ror(yaml_author["ror"])
            if remote_org_name:
                yaml_author["organisation_name"] = remote_org_name
        # else: we have ror and organisation_name, do nothing

    if remote_url:
        org_dict["url"] = remote_url

    if "ror" in yaml_author:
        # Not an else from previous case
        person_dict["affiliation"] = {"@id": yaml_author["ror"]}
        # If ror defined, organisation_name becomes mandatory, if it is to be shown in WorkflowHub
        org_dict["@type"] = "Organization"
        if "organisation_name" in yaml_author:
            org_dict["name"] = yaml_author["organisation_name"]
            compss_crate.add(ContextEntity(compss_crate, yaml_author["ror"], org_dict))
        else:
            print(
                f"PROVENANCE | WARNING in your {info_yaml} file. 'organisation_name' not defined for an 'Organisation'"
            )

    if remote_all_names:
        person_dict["givenName"] = remote_all_names["given-names"]
        person_dict["familyName"] = remote_all_names["family-names"]
        yaml_author["givenName"] = remote_all_names["given-names"]
        yaml_author["familyName"] = remote_all_names["family-names"]

    # givenName and familyName are optional for the user, but, if they set them, we get them
    if "givenName" in yaml_author:
        person_dict["givenName"] = yaml_author["givenName"]
    if "familyName" in yaml_author:
        person_dict["familyName"] = yaml_author["familyName"]

    if searched_author or remote_org_name:
        yaml_author["Updated"] = True

    compss_crate.add(Person(compss_crate, yaml_author["orcid"], person_dict))

    return True, yaml_author


def root_entity(
    compss_crate: ROCrate, yaml_content: dict, info_yaml: str
) -> typing.Tuple[dict, list]:
    """
    Generate the Root Entity in the RO-Crate generated for the COMPSs application

    :param compss_crate: The COMPSs RO-Crate being generated
    :param yaml_content: Content of the YAML file specified by the user
    :param info_yaml: Name of the YAML file specified by the user

    :returns: The updated yaml_content, and a list of author @id's
    """

    updated_authors = False
    # Get Sections
    compss_wf_info = yaml_content["COMPSs Workflow Information"]
    authors_info = []
    if "Authors" in yaml_content:
        authors_info_yaml = yaml_content["Authors"]  # Now a list of authors
        if isinstance(authors_info_yaml, list):
            authors_info = authors_info_yaml
        else:
            authors_info.append(authors_info_yaml)

    # COMPSs Workflow RO Crate generation
    # Root Entity

    # SHOULD in RO-Crate 1.1. MUST in WorkflowHub
    compss_crate.name = compss_wf_info["name"]

    if "description" in compss_wf_info:
        # SHOULD in Workflow Profile and WorkflowHub
        compss_crate.description = compss_wf_info["description"]

    if "license" in compss_wf_info:
        # License details could be also added as a Contextual Entity. MUST in Workflow RO-Crate Profile, but WorkflowHub does not consider it a mandatory field
        compss_crate.license = compss_wf_info["license"]

    author_list = []
    org_list = []

    for author in authors_info:
        if "orcid" in author and author["orcid"] in author_list:
            break
        added_person, author = add_person_definition(
            compss_crate, "Author", author, info_yaml
        )
        if "Updated" in author:
            # Updated with online search
            updated_authors = True
        if added_person:
            author_list.append(author["orcid"])
            if "ror" in author and author["ror"] not in org_list:
                org_list.append(author["ror"])

    # Generate 'author', 'creator' and 'publisher' terms
    crate_author_list = []
    crate_org_list = []
    for author_orcid in author_list:
        crate_author_list.append({"@id": author_orcid})
    if crate_author_list:
        compss_crate.root_dataset["author"] = (
            crate_author_list  # As specified in RO-Crate 1.1
        )
        compss_crate.creator = (
            crate_author_list  # Also needed, either for WFHub or rocrate-inveniordm
        )
    for org_ror in org_list:
        crate_org_list.append({"@id": org_ror})

    # publisher is SHOULD in RO-Crate 1.1. Preferably an Organisation, but could be a Person
    if not crate_org_list:
        # Empty list of organisations, add authors as publishers
        if crate_author_list and not len(crate_author_list) == 0:
            compss_crate.publisher = crate_author_list[0]
    else:
        if crate_org_list and not len(crate_org_list) == 0:
            compss_crate.publisher = crate_org_list[0]

    if len(crate_author_list) == 0:
        print(f"PROVENANCE | WARNING: No valid 'Authors' specified in {info_yaml}")

    if updated_authors:
        yaml_content["Authors"] = authors_info
        yaml_content["Updated"] = True

    return yaml_content, crate_author_list


def get_main_entities(
    wf_info: dict, info_yaml: str, dp_log: Path
) -> typing.Tuple[str, str, str, dict]:
    """
    Get COMPSs version and mainEntity from dataprovenance.log first lines
    3 First lines expected format: compss_version_number\n main_entity\n output_profile_file\n
    Next lines are for "accessed files" and "direction"
    mainEntity can be directly obtained for Python, or defined by the user in the YAML (sources_main_file)

    :param wf_info: YAML dict to extract info form the application, as specified by the user
    :param info_yaml: Name of the YAML file specified by the user
    :param dp_log: Path object to the dataprovenance.log file

    :returns: COMPSs version, main COMPSs file name, COMPSs profile file name, updated wf_info
    """

    # Build the whole source files list in list_of_sources, and get a backup main entity, in case we can't find one
    # automatically. The mainEntity must be an existing file, otherwise the RO-Crate won't have a ComputationalWorkflow
    yaml_sources_list = []  # YAML sources list
    list_of_sources = []  # Full list of source files, once directories are traversed
    # Should contain absolute paths, for correct comparison (two files in different directories
    # could be named the same)

    main_entity = None
    backup_main_entity = None

    if "sources" in wf_info:
        if isinstance(wf_info["sources"], list):
            yaml_sources_list.extend(wf_info["sources"])
        else:
            yaml_sources_list.append(wf_info["sources"])
    if "files" in wf_info:
        # Backward compatibility: if old "sources_dir" and "files" have been used, merge in yaml_sources_list.
        if isinstance(wf_info["files"], list):
            yaml_sources_list.extend(wf_info["files"])
        else:
            yaml_sources_list.append(wf_info["files"])
    if "sources_dir" in wf_info:
        #  Backward compatibility: if old "sources_dir" and "files" have been used, merge in yaml_sources_list.
        # sources_list = list(tuple(wf_info["files"])) + list(tuple(wf_info["sources"]))
        if isinstance(wf_info["sources_dir"], list):
            yaml_sources_list.extend(wf_info["sources_dir"])
        else:
            yaml_sources_list.append(wf_info["sources_dir"])

    # If no sources are defined, define automatically the main_entity or return warning
    keys = ["sources", "files", "sources_dir"]
    if not any(key in wf_info for key in keys):
        print(
            f"PROVENANCE | WARNING: No 'sources' defined at {info_yaml}. Only the mainEntity will be added as source file"
        )

    with open(dp_log, "r", encoding="UTF-8") as dp_file:
        compss_v = next(dp_file).rstrip()  # First line, COMPSs version number
        second_line = next(dp_file).rstrip()
        # Second, main_entity. Use better rstrip, just in case there is no '\n'
        if second_line.endswith(".py"):
            # Python. Line contains only the file name, need to locate it
            fn_detected_app = second_line
            detected_app = fn_detected_app  # No sub-paths in Python
        else:  # Java app. Need to fix filename first
            # Translate identified main entity matmul.files.Matmul to a comparable path
            me_file_name = second_line.split(".")[-1]
            fn_detected_app = me_file_name + ".java"
            # detected_app is also used much later in the code
            me_sub_path = second_line.replace(".", "/")
            detected_app = me_sub_path + ".java"  # Was detected_app
        if __debug__:
            print(f"PROVENANCE DEBUG | Detected app is: {detected_app}")
        third_line = next(dp_file).rstrip()
        out_profile_fn = Path(third_line)

    # Find a backup_main_entity while building the full list of source files
    for source in yaml_sources_list:
        path_source = Path(source).expanduser()
        resolved_source = str(path_source.resolve())
        if path_source.exists():
            if os.path.isfile(resolved_source):
                list_of_sources.append(resolved_source)
                if backup_main_entity is None and path_source.suffix in {
                    ".py",
                    ".java",
                    ".jar",
                    ".class",
                }:
                    backup_main_entity = resolved_source
                    if __debug__:
                        print(
                            f"PROVENANCE DEBUG | FOUND SOURCE FILE AS BACKUP MAIN: {backup_main_entity}"
                        )
            elif os.path.isdir(resolved_source):
                for root, _, files in os.walk(
                    resolved_source, topdown=True, followlinks=True
                ):
                    if "__pycache__" in root:
                        continue  # We skip __pycache__ subdirectories
                    for f_name in files:
                        if __debug__:
                            print(
                                f"PROVENANCE DEBUG | ADDING FILE to list_of_sources: {f_name}. root is: {root}"
                            )
                        if f_name.startswith("*"):
                            # Avoid dealing with symlinks with wildcards
                            continue
                        full_name = os.path.join(root, f_name)
                        list_of_sources.append(full_name)
                        if backup_main_entity is None and Path(f_name).suffix in {
                            ".py",
                            ".java",
                            ".jar",
                            ".class",
                        }:
                            backup_main_entity = full_name
                            if __debug__:
                                print(
                                    f"PROVENANCE DEBUG | FOUND SOURCE FILE IN A DIRECTORY AS BACKUP MAIN: {backup_main_entity}"
                                )
            else:
                print(
                    f"PROVENANCE | WARNING: A defined source is neither a directory, nor a file ({resolved_source})"
                )
        else:
            print(
                f"PROVENANCE | WARNING: Specified file or directory in {info_yaml} 'sources' does not exist ({path_source})"
            )

    # Can't get backup_main_entity from sources_main_file, because we do not know if it really exists
    if len(list_of_sources) == 0:
        print(
            "PROVENANCE | WARNING: Unable to find application source files. Please, review your "
            "ro_crate_info.yaml definition ('sources' term)"
        )
        # raise FileNotFoundError
    elif backup_main_entity is None:
        # No source files found in list_of_sources, set any file as backup
        backup_main_entity = list_of_sources[0]

    if __debug__:
        print(f"PROVENANCE DEBUG | backup_main_entity is: {backup_main_entity}")

    for file in list_of_sources:  # Try to find the identified mainEntity
        if file.endswith(detected_app):
            if __debug__:
                print(
                    f"PROVENANCE DEBUG | IDENTIFIED MAIN ENTITY FOUND IN LIST OF FILES: {file}"
                )
            main_entity = file
            break
    # main_entity has a value if mainEntity has been automatically detected

    if "sources_main_file" in wf_info:
        # Check what the user has defined
        # If it directly exists, we are done, no need to search in 'sources'
        found = False
        path_smf = Path(wf_info["sources_main_file"]).expanduser()
        resolved_sources_main_file = str(path_smf.resolve())
        if os.path.isfile(path_smf):
            # Checks if exists
            if main_entity is None:
                # the detected_app was not found previously in the list of files
                found = True
                print(
                    f"PROVENANCE | WARNING: The file defined at sources_main_file is assigned as 'mainEntity': {resolved_sources_main_file}"
                )
            else:
                if main_entity == resolved_sources_main_file:
                    print(
                        f"PROVENANCE | The file automatically identified as 'mainEntity' matches the one specified by the user with 'sources_main_file': {main_entity}"
                    )
                else:
                    print(
                        f"PROVENANCE | WARNING: The file defined at 'sources_main_file' "
                        f"({resolved_sources_main_file}) in {info_yaml} does not match with the "
                        f"automatically identified 'mainEntity' ({main_entity})"
                    )
            main_entity = resolved_sources_main_file
            found = True
            # Update wf_info so add_application_source_files works fine later
            if "sources" in wf_info:
                if isinstance(wf_info["sources"], list):
                    wf_info["sources"].append(resolved_sources_main_file)
                else:
                    tmp_list = []
                    tmp_list.append(wf_info["sources"])  # Single element
                    tmp_list.append(resolved_sources_main_file)
                    wf_info["sources"] = tmp_list
            else:
                wf_info["sources"] = resolved_sources_main_file
        else:
            # If the file defined in sources_main_file is not directly found, try to find it in 'sources'
            # if sources_main_file is an absolute path, the join has no effect
            for source in yaml_sources_list:  # Created at the beginning
                path_sources = Path(source).expanduser()
                if not path_sources.exists() or os.path.isfile(source):
                    continue
                resolved_sources = str(path_sources.resolve())
                resolved_sources_main_file = os.path.join(
                    resolved_sources, wf_info["sources_main_file"]
                )
                for file in list_of_sources:
                    if file == resolved_sources_main_file:
                        # The file exists
                        if __debug__:
                            print(
                                f"PROVENANCE DEBUG | The file defined at sources_main_file exists: {resolved_sources_main_file}"
                            )
                        if resolved_sources_main_file != main_entity:
                            print(
                                f"PROVENANCE | WARNING: The file defined at sources_main_file "
                                f"({resolved_sources_main_file}) in {info_yaml} does not match with the "
                                f"automatically identified 'mainEntity' ({main_entity})"
                            )
                        # else: the user has defined exactly the file we found
                        # In both cases: set file defined by user
                        main_entity = resolved_sources_main_file
                        # Can't use Path, file may not be in cwd
                        found = True
                        break
                    if file.endswith(wf_info["sources_main_file"]):
                        # The file exists
                        if __debug__:
                            print(
                                f"PROVENANCE DEBUG | The file defined at sources_main_file exists: {resolved_sources_main_file}"
                            )
                        if file != main_entity:
                            print(
                                f"PROVENANCE | WARNING: The file defined at sources_main_file "
                                f"({file}) in {info_yaml} does not match with the "
                                f"automatically identified 'mainEntity' ({main_entity})"
                            )
                        # else: the user has defined exactly the file we found
                        # In both cases: set file defined by user
                        main_entity = file
                        # Can't use Path, file may not be in cwd
                        found = True
                        break
            if not found:
                print(
                    f"PROVENANCE | WARNING: the defined 'sources_main_file' ({wf_info['sources_main_file']}) does "
                    f"not exist in the defined 'sources'. Check your {info_yaml}."
                )
                # If we identified the mainEntity automatically, we select it when the one defined
                # by the user is not found

    if main_entity is None:
        print(
            f"PROVENANCE | WARNING: The detected 'mainEntity' has not been found in the list of 'sources' provided in {info_yaml}. "
            f"Current Working Directory will be searched to find the 'mainEntity'"
        )
        # Last chance. If mainEntity still not found, try to find it in CWD
        # We try directly to add the mainEntity identified in dataprovenance.log, if exists in the CWD tree
        found_file = find_subpath_in_cwd(detected_app)
        if found_file:
            main_entity = found_file
            # list_of_sources.append(found_file)
            # Update wf_info so add_application_source_files works fine later
            if "sources" in wf_info:
                if isinstance(wf_info["sources"], list):
                    wf_info["sources"].append(found_file)
                else:
                    tmp_list = []
                    tmp_list.append(wf_info["sources"])  # Single element
                    tmp_list.append(found_file)
                    wf_info["sources"] = tmp_list
            else:
                wf_info["sources"] = found_file
        else:
            print(
                f"PROVENANCE | WARNING: The detected 'mainEntity' has not been found in Current Working Directory. "
                f"A backup 'mainEntity' will be added if possible"
            )

    if main_entity is None:
        # When neither identified, nor defined by user: get backup if exists
        if backup_main_entity is None:
            # We have a fatal problem
            print(
                f"PROVENANCE | ERROR: no 'mainEntity' has been found. Check the definition of 'sources' and "
                f"'sources_main_file' in {info_yaml}"
            )
            raise FileNotFoundError
        main_entity = backup_main_entity
        print(
            f"PROVENANCE | WARNING: the detected 'mainEntity' {detected_app} does not exist in the list "
            f"of application files provided in {info_yaml}. Setting {main_entity} as mainEntity"
        )

    print(
        f"PROVENANCE | COMPSs version: '{compss_v}', out_profile: '{out_profile_fn.name}', main_entity: '{main_entity}'"
    )

    return compss_v, main_entity, out_profile_fn.name, wf_info


def get_manually_defined_software_requirements(
    compss_crate: ROCrate, wf_info: dict, info_yaml: str
) -> list:
    """
    Extract all application software dependencies manually specified by the user in the YAML file. At least "name"
    and "version" must be specified in the YAML

    :param compss_crate: The COMPSs RO-Crate being generated
    :param wf_info: YAML dict to extract info form the application, as specified by the user
    :param info_yaml: Name of the YAML file specified by the user

    :returns: list of id's to be added to the ComputationalWorkflow as softwareRequirements
    """

    software_info = []
    software_requirements_list = []

    if not "software" in wf_info:
        return None

    if isinstance(wf_info["software"], list):
        software_info = wf_info["software"]
    else:
        software_info.append(wf_info["software"])

    for soft_details in software_info:
        if not "name" in soft_details or not "version" in soft_details:
            print(
                f"PROVENANCE | WARNING in your {info_yaml} file. A 'software' does not have a 'name' or 'version' "
                f"defined. The 'software' dependency definition will be ignored"
            )
            continue
        software_dict = {"@type": "SoftwareApplication"}
        if "url" in soft_details:
            software_id = soft_details["url"]
            software_dict["url"] = soft_details["url"]
        else:
            software_id = "#" + soft_details["name"].lower()
        software_dict["name"] = soft_details["name"]
        if is_canonical(str(soft_details["version"])):
            software_dict["softwareVersion"] = soft_details["version"]
        else:
            software_dict["version"] = soft_details["version"]
        software_requirements_list.append({"@id": software_id})
        compss_crate.add(ContextEntity(compss_crate, software_id, software_dict))
        version_str = (
            software_dict["softwareVersion"]
            if "softwareVersion" in software_dict
            else ""
        )
        print(
            f"PROVENANCE | 'softwareRequirements' dependency correctly added: {soft_details['name']} ({version_str})"
        )

    return software_requirements_list


def search_orcid(person_name: str) -> tuple[str, str, str, dict]:
    """
    Search at orcid.org the first ORCID matching the person's name

    :param person_name: Name of the person to search

    :returns: Name, organisation and e-mail of the first record found
    """

    # Base URL from ORCID for public search
    url_base = "https://pub.orcid.org/v3.0/expanded-search"
    # Request headers
    headers = {"Accept": "application/json"}
    # Search parameters
    params = {"q": person_name, "rows": 1}

    if not person_name:
        return None, None
    res_institution = None
    orcid = None
    e_mail = None
    all_names = {}
    # Submit the GET request

    try:
        print(
            f"PROVENANCE | PERSON '{person_name}': Searching ORCID, Organisation and e-Mail"
        )
        response = requests.get(url_base, headers=headers, params=params, timeout=5)
        if response.status_code == 200:
            list_of_results = response.json()
            # if 'num-found' in list_of_results:
            #    print(f"PROVENANCE | Records found: {list_of_results['num-found']}")
            if "expanded-result" in list_of_results:
                for result in list_of_results["expanded-result"]:
                    orcid = "https://orcid.org/" + result.get("orcid-id")
                    list_institutions = result.get("institution-name")
                    res_institution = (
                        list_institutions[0] if len(list_institutions) > 0 else None
                    )
                    all_names["given-names"] = str(result.get("given-names"))
                    all_names["family-names"] = str(result.get("family-names"))
                    obtained_full_name = (
                        str(result.get("given-names"))
                        + " "
                        + str(result.get("family-names"))
                    )
                    list_emails = result.get("email")
                    e_mail = list_emails[0] if list_emails else None
                    if obtained_full_name.lower() == person_name.lower():
                        print(
                            f"PROVENANCE | Fetched data. Given name(s): {all_names['given-names']}, Family name(s): {all_names['family-names']}, ORCID: {orcid}, Organisation: {res_institution}, e-Mail: {e_mail}"
                        )
                    else:
                        print(
                            f"PROVENANCE | Fetched name '{obtained_full_name}' does not match specified name '{person_name}'"
                        )
                        orcid = None
                        res_institution = None
                    break
            else:
                print(
                    f"PROVENANCE | Searching ORCID for person '{person_name}'. No records where found"
                )
        else:
            print(
                f"PROVENANCE | Searching ORCID for person '{person_name}'. Request error {response.status_code}"
            )
        return orcid, res_institution, e_mail, all_names
    except requests.exceptions.Timeout:
        print(
            f"PROVENANCE | Searching ORCID for person '{person_name}'. Request timeout"
        )
    except requests.exceptions.RequestException as e:
        print(
            f"PROVENANCE | Searching ORCID for person '{person_name}'. Request exception: {e}"
        )
    return orcid, res_institution, e_mail, all_names


def search_by_orcid(orcid_str: str) -> tuple[str, str, str, dict]:
    """
    Search at orcid.org the first ORCID matching the ORCID reference provided

    :param orcid_str: ORCID of the person to search

    :returns: Name, organisation and e-mail of the first record found
    """

    import json

    if not orcid_str:
        return None, None
    # Get info from a specific ORCID
    query_str = '"' + orcid_str.split("/")[-1] + '"'
    url_base = "https://pub.orcid.org/v3.0/expanded-search"
    # Request headers
    headers = {"Accept": "application/json"}
    # Search parameters
    params = {"q": query_str, "rows": 1}

    res_institution = None
    obtained_full_name = None
    e_mail = None
    all_names = {}
    # Submit the GET request
    try:
        print(
            f"PROVENANCE | PERSON '{orcid_str}': Searching Name, Organisation and e-Mail"
        )
        response = requests.get(url_base, headers=headers, params=params, timeout=5)
        if response.status_code == 200:
            list_of_results = response.json()
            # import json
            # print(json.dumps(list_of_results, indent=4, sort_keys=True))
            if "expanded-result" in list_of_results:
                for result in list_of_results["expanded-result"]:
                    list_institutions = result.get("institution-name")
                    res_institution = (
                        list_institutions[0] if len(list_institutions) > 0 else None
                    )
                    all_names["given-names"] = str(result.get("given-names"))
                    all_names["family-names"] = str(result.get("family-names"))
                    obtained_full_name = (
                        str(result.get("given-names"))
                        + " "
                        + str(result.get("family-names"))
                    )
                    list_emails = result.get("email")
                    e_mail = list_emails[0] if list_emails else None
                    print(
                        f"PROVENANCE | Fetched data. Given name(s): {all_names['given-names']}, Family name(s): {all_names['family-names']}, Organisation: {res_institution}, e-Mail: {e_mail}"
                    )
                    break
            else:
                print(
                    f"PROVENANCE | Searching Name for ORCID '{orcid_str}'. No records where found"
                )
        else:
            print(
                f"PROVENANCE | Searching Name for ORCID '{orcid_str}'. Request error {response.status_code}"
            )
        return obtained_full_name, res_institution, e_mail, all_names
    except requests.exceptions.Timeout:
        print(f"PROVENANCE | Searching Name for ORCID '{orcid_str}'. Request timeout")
    except requests.exceptions.RequestException as e:
        print(
            f"PROVENANCE | Searching Name for ORCID '{orcid_str}'. Request exception: {e}"
        )
    return obtained_full_name, res_institution, e_mail, all_names


def search_ror(org_name: str) -> tuple[str, str, str]:
    """
    Search at ror.org the first ROR matching the institution's name

    :param org_name: Name of the organisation to search

    :returns: ROR, name and URL for the first record found
    """

    # ROR base URL API for searching
    url_base = "https://api.ror.org/organizations"
    # Search parameters
    params = {"query": org_name}

    if not org_name:
        return None, None, None
    obtained_ror = None
    obtained_org_name = None
    obtained_url = None
    # Submit the GET request
    try:
        print(f"PROVENANCE | ORGANISATION '{org_name}': Searching ROR and URL")
        response = requests.get(url_base, params=params, timeout=5)
        if response.status_code == 200:
            list_of_results = response.json()
            # import json
            # print(json.dumps(list_of_results, indent=4, sort_keys=True))
            if "items" in list_of_results and list_of_results["items"]:
                for result in list_of_results["items"]:
                    obtained_ror = result.get("id")
                    obtained_org_name = result.get("name")
                    links = result.get("links")
                    obtained_url = links[0] if links else None
                    if obtained_org_name.lower() == org_name.lower():
                        print(
                            f"PROVENANCE | Fetched data. Organisation: {obtained_org_name}, ROR: {obtained_ror}, URL: {obtained_url}"
                        )
                    else:
                        print(
                            f"PROVENANCE | Fetched name '{obtained_org_name}' does not match specified name '{org_name}'"
                        )
                        obtained_ror = None
                        obtained_org_name = None
                        obtained_url = None
                    break
            else:
                print(
                    f"PROVENANCE | Searching ROR for organisation '{org_name}'. No records where found"
                )
        else:
            print(
                f"PROVENANCE | Searching ROR for organisation '{org_name}'. Request error {response.status_code}"
            )
        return obtained_ror, obtained_org_name, obtained_url
    except requests.exceptions.Timeout:
        print(
            f"PROVENANCE | Searching ROR for organisation '{org_name}'. Request timeout"
        )
    except requests.exceptions.RequestException as e:
        print(
            f"PROVENANCE | Searching ROR for organisation '{org_name}'. Request exception: {e}"
        )
    return obtained_ror, obtained_org_name, obtained_url


def search_by_ror(org_ror: str) -> tuple[str, str]:
    """
    Search at ror.org the first institution matching the ROR passed

    :param org_ror: ROR of the organisation to search

    :returns: Name and URL for the first record found
    """

    # ROR base URL API for searching
    url_base = "https://api.ror.org/organizations"
    # Search parameters
    params = {"query": org_ror}

    if not org_ror:
        return None, None

    obtained_org_name = None
    obtained_url = None
    # Submit the GET request
    try:
        print(f"PROVENANCE | ORGANISATION '{org_ror}': Searching for Name and URL")
        response = requests.get(url_base, params=params, timeout=5)
        if response.status_code == 200:
            list_of_results = response.json()
            # import json
            # print(json.dumps(list_of_results, indent=4, sort_keys=True))
            if "items" in list_of_results and list_of_results["items"]:
                for result in list_of_results["items"]:
                    obtained_ror = result.get("id")
                    obtained_org_name = result.get("name")
                    links = result.get("links")
                    obtained_url = links[0] if links else None
                    if obtained_ror == org_ror:
                        print(
                            f"PROVENANCE | Fetched data. Organisation: {obtained_org_name}, ROR: {obtained_ror}, URL: {obtained_url}"
                        )
                    else:
                        print(
                            f"PROVENANCE | Fetched ROR '{obtained_ror}' does not match specified name '{org_ror}'"
                        )
                        obtained_ror = None
                        obtained_org_name = None
                        obtained_url = None
                    break
            else:
                print(
                    f"PROVENANCE | Searching Name for organisation '{org_ror}'. No records where found"
                )
        else:
            print(
                f"PROVENANCE | Searching Name for organisation '{org_ror}'. Request error {response.status_code}"
            )
        return obtained_org_name, obtained_url
    except requests.exceptions.Timeout:
        print(
            f"PROVENANCE | Searching Name for organisation '{org_ror}'. Request timeout"
        )
    except requests.exceptions.RequestException as e:
        print(
            f"PROVENANCE | Searching Name for organisation '{org_ror}'. Request exception: {e}"
        )
    return obtained_org_name, obtained_url
