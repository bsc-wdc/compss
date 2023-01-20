FROM compss/compss-tutorial:3.1

EXPOSE 43000-44000

ENV PATH /root/.local/bin:$PATH

RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir graphviz && \
# Dislib requirements:
    python3 -m pip install scikit-learn>=1.0.2 && \
    python3 -m pip install scipy>=1.3.0 && \
    # minimal version to use numba
    python3 -m pip install numpy==1.23.5 && \
    python3 -m pip install numpydoc>=0.8.0 && \
    python3 -m pip install cvxpy>=1.1.5 && \
    python3 -m pip install cbor2>=5.4.0 && \
    python3 -m pip install pandas>=0.24.2 && \
    python3 -m pip install matplotlib>=2.2.3 && \
# Install dislib
    python3 -m pip install dislib && \
# Upgrade numba
    python3 -m pip install numba==0.56.4 && \
# Other packackes
    python3 -m pip install tabulate==0.8.9
