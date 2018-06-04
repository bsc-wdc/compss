FROM compss-ubuntu16:stable

# =============================================================================
# Non public credentials
# =============================================================================

RUN wget http://compss.bsc.es/releases/repofiles/repo_deb_ubuntu_x86-64((REPO)).list -O /etc/apt/sources.list.d/compss-framework_x86-64.list
RUN wget -qO - http://compss.bsc.es/repo/debs/deb-gpg-bsc-grid.pub.key | apt-key add -
RUN apt-get update
RUN apt-get install -y compss-framework
RUN echo "export PATH=$PATH:/opt/COMPSs/Runtime/scripts/user/" >> /root/.bashrc
#RUN mkdir /var/run/sshd
#RUN echo 'root:screencast' | chpasswd
#RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config
#RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
RUN echo "export VISIBLE=now" >> /etc/profile
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN yes yes | ssh-keygen -f /root/.ssh/id_rsa -t rsa -N '' > /dev/null
RUN cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys
ENV NOTVISIBLE "in users profile"
