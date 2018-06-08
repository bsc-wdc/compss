FROM compss-ubuntu16:testing

# =============================================================================
# Non public credentials
# =============================================================================

# Add public key for Jenkins login & add maven repository credentials
RUN mkdir /home/jenkins/.ssh && \
    mkdir /home/jenkins/.m2

COPY /files/settings.xml /home/jenkins/.m2/settings.xml

# Overwrite the ssh key for test infrastructure
COPY /files/authorized_keys /home/jenkins/.ssh/authorized_keys
COPY /files/id_rsa /home/jenkins/.ssh/id_rsa
COPY /files/id_rsa.pub /home/jenkins/.ssh/id_rsa.pub

# Git credentials
COPY /files/.gitconfig /home/jenkins/.gitconfig
RUN chown -R jenkins /home/jenkins && \
    chgrp -R jenkins /home/jenkins && \
    chmod 600 /home/jenkins/.ssh/authorized_keys && \
    chmod 700 /home/jenkins/.ssh && \
    chown -R jenkins /home/jenkins/.m2 && \
    chgrp -R jenkins /home/jenkins/.m2 && \
    cat /home/jenkins/.ssh/id_rsa.pub >> /home/jenkins/.ssh/authorized_keys && \
    chmod 600 /home/jenkins/.ssh/authorized_keys && \
    echo "GIT_USER=jenkins" >> /etc/environment && \
    echo "GIT_PASSWORD=jenkins2017" >> /etc/environment && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

