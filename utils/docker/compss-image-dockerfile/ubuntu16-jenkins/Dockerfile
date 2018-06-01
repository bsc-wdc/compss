FROM compss-ubuntu16-testing:stable

# =============================================================================
# Non public credentials
# =============================================================================

# Add public key for Jenkins login
RUN mkdir /home/jenkins/.ssh
COPY /files/authorized_keys /home/jenkins/.ssh/authorized_keys
RUN chown -R jenkins /home/jenkins
RUN chgrp -R jenkins /home/jenkins
RUN chmod 600 /home/jenkins/.ssh/authorized_keys
RUN chmod 700 /home/jenkins/.ssh

# Add maven repository credentials
RUN mkdir /home/jenkins/.m2
COPY /files/settings.xml /home/jenkins/.m2/settings.xml
RUN chown -R jenkins /home/jenkins/.m2
RUN chgrp -R jenkins /home/jenkins/.m2

# Overwrite the ssh key for test infrastructure
COPY /files/id_rsa /home/jenkins/.ssh/id_rsa
COPY /files/id_rsa.pub /home/jenkins/.ssh/id_rsa.pub
RUN cat /home/jenkins/.ssh/id_rsa.pub >> /home/jenkins/.ssh/authorized_keys
RUN chmod 600 /home/jenkins/.ssh/authorized_keys

# Git credentials
COPY /files/.gitconfig /home/jenkins/.gitconfig
RUN echo "GIT_USER=jenkins" >> /etc/environment
RUN echo "GIT_PASSWORD=jenkins2017" >> /etc/environment
