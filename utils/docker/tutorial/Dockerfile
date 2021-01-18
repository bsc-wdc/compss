FROM compss/compss:2.8

EXPOSE 43000-44000

RUN pip2 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip2 install --no-cache-dir matplotlib && \
    pip3 install --no-cache-dir matplotlib && \
    pip2 install --no-cache-dir graphviz && \
    pip3 install --no-cache-dir graphviz && \
    python3 -m pip uninstall -y enum34 && \
    python3 -m pip install dislib && \
    # Patch dislib for latest scipy versions (next dislib releases should include it since it is already in master)
    file_to_patch=$(python3 -c "from __future__ import print_function; import dislib.cluster.dbscan.classes as cls; print(cls.__file__[:-1]) if cls.__file__.endswith('c') else print(cls.__file__)") && \
    sed -i -E 's/adj_matrix.rows\[core_idx\] = neighbours/adj_matrix.rows\[core_idx\] = neighbours.tolist()/g' $file_to_patch
