#!/bin/bash

tmpfile=$(mktemp)
hostfile=$(mktemp)
qstat -f "$PBS_JOBID" > "${tmpfile}"


startIndex=$(grep -n -m 1 "exec_vnode" "${tmpfile}" | cut -f1 -d: )
endIndex=$(grep -n -m 1 "Hold_Types" "${tmpfile}" | cut -f1 -d: )

nodes_info=$(echo -n $(sed -n ${startIndex},$((endIndex - 1))p < ${tmpfile}) | tr -d " ")
nodes_info=$(echo "${nodes_info#exec_vnode=}" | tr "+" "\\t" )

for node in ${nodes_info}; do 
  echo "${node}" | tr ":" "\\t" | tr "(" "\\t" | awk '{ print $1 }' | sed 's/archer_//' >> "${hostfile}"
done

m_node=$(head -n 1 "$hostfile")
sed -i 1d "$hostfile"
w_nodes=$(cat "$hostfile")
while [ "${#m_node}" -lt "5" ]; do
    echo "${m_node}"
    m_node="0${m_node}"
done
master_node="nid${m_node}"

for node in ${w_nodes}; do
    while [ "${#node}" -lt "5" ]; do
        node="0${node}"
    done
    if [ -z "${worker_nodes}" ]; then
        worker_nodes="nid${node}"
    else
        worker_nodes="${worker_nodes} nid${node}"
    fi
done

rm "$tmpfile" "$hostfile"

PBS_O_WORKDIR=$(readlink -f "$PBS_O_WORKDIR")
export PBS_O_WORKDIR
cd "$PBS_O_WORKDIR" || exit 1

export ${master_node}
export ${worker_nodes}

