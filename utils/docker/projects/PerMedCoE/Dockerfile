FROM compss/compss:permedcoe

EXPOSE 43000-44000

ENV PATH /root/.local/bin:$PATH
ENV CONTAINER True 
# The CONTAINER variable is used to decide where the assets will be used (/root/assets)

RUN pip2 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip2 install --no-cache-dir graphviz && \
    pip3 install --no-cache-dir graphviz && \
# Remove enum34 since causes issues with dislib required libraries
    python3 -m pip uninstall -y enum34 && \
# Dislib requirements:
    python3 -m pip install scikit-learn==0.22.1 && \
    python3 -m pip install scipy>=1.3.0 && \
    python3 -m pip install numpy==1.19.5 && \
    python3 -m pip install numpydoc>=0.8.0 && \
    python3 -m pip install cvxpy>=1.1.5 && \
# Install dislib
    python3 -m pip install dislib && \
# This is already patched in dislib since version 0.7.0
   file_to_patch=$(python3 -c "from __future__ import print_function; import dislib.cluster.dbscan.classes as cls; print(cls.__file__[:-1]) if cls.__file__.endswith('c') else print(cls.__file__)") && \
   sed -i -E 's/adj_matrix.rows\[core_idx\] = neighbours/adj_matrix.rows\[core_idx\] = neighbours.tolist()/g' $file_to_patch
