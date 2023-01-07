FROM ubuntu:20.04

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:$PATH

RUN apt-get update --fix-missing && \
    apt-get install -y zip make wget bzip2 ca-certificates curl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER root

RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    chmod +x Miniconda3-latest-Linux-x86_64.sh && \
    /bin/bash ./Miniconda3-latest-Linux-x86_64.sh -b && \
    ~/miniconda3/bin/conda -h && \
    rm -rf Miniconda3-latest-Linux-x86_64.sh

RUN ~/miniconda3/bin/conda create -n elyra python
RUN echo ". ~/miniconda3/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate elyra" >> ~/.bashrc

RUN . ~/miniconda3/etc/profile.d/conda.sh && \
    conda activate elyra && \
    conda install -y -c conda-forge/label/main nodejs && \
    node --version && \
    conda install -y -c conda-forge/label/main yarn && \
    yarn --version 

RUN cd ~ && \
    wget https://github.com/WangQing-WUT/elyra/archive/refs/heads/workflow.zip && \
    unzip workflow.zip

WORKDIR /root/elyra-workflow
RUN . ~/miniconda3/etc/profile.d/conda.sh && \
    conda activate elyra && \
    make install

# COPY requirements.yml requirements.yml

# RUN wget https://repo.anaconda.com/miniconda/Miniconda3-4.5.11-Linux-x86_64.sh -O ~/miniconda.sh
# RUN /bin/bash ~/miniconda.sh -b -p /opt/conda && \
#     rm ~/miniconda.sh && \
#     /opt/conda/bin/conda clean -tipsy && \
#     ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
#     echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
#     echo "conda activate elyra-env" >> ~/.bashrc && \
#     /opt/conda/bin/conda env create --file requirements.yml

# # update nodejs
# RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash - && \
#     apt-get install -y nodejs

# # install yarn
# RUN curl -sL https://dl.yarnpkg.com/debian/pubkey.gpg | gpg --dearmor | tee /usr/share/keyrings/yarnkey.gpg >/dev/null && \
#     echo "deb [signed-by=/usr/share/keyrings/yarnkey.gpg] https://dl.yarnpkg.com/debian stable main" | tee /etc/apt/sources.list.d/yarn.list && \
#     apt-get update && apt-get install yarn

#ENV TINI_VERSION v0.16.1
#ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
#RUN chmod +x /usr/bin/tini
#ENTRYPOINT [ "/usr/bin/tini", "--" ]
CMD [ "/bin/bash" ]

