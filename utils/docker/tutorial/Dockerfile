FROM compss/compss:2.8

RUN pip2 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir --upgrade pip && \
    pip2 install --no-cache-dir matplotlib && \
    pip3 install --no-cache-dir matplotlib && \
    pip2 install --no-cache-dir graphviz && \
    pip3 install --no-cache-dir graphviz && \
    python3 -m pip uninstall -y enum34 && \
    python3 -m pip install dislib
