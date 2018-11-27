FROM compss-ubuntu16:base

# =============================================================================
# Non public credentials
# =============================================================================

ARG repo=""

RUN wget "http://compss.bsc.es/releases/repofiles/repo_deb_ubuntu_x86-64${repo}.list" -O /etc/apt/sources.list.d/compss-framework_x86-64.list && \
    wget -qO - http://compss.bsc.es/repo/debs/deb-gpg-bsc-grid.pub.key | apt-key add - && \
    apt-get update && \
    apt-get install -y compss-framework

RUN echo "export PATH=$PATH:/opt/COMPSs/Runtime/scripts/user/:/opt/COMPSs/Runtime/scripts/utils" >> /root/.bashrc && \
    echo "export VISIBLE=now" >> /etc/profile && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    yes yes | ssh-keygen -f /root/.ssh/id_rsa -t rsa -N '' > /dev/null && \
    cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys
ENV NOTVISIBLE "in users profile"
